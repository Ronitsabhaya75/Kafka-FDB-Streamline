"""Live FDB to Kafka ordering and daemon lifecycle verification."""

import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from fdb_kafka_bridge.bridge.pipeline import BridgePipeline
from fdb_kafka_bridge.cdc.checkpoint import CheckpointStore, SinkIdentity
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.listener import FDBMutationListener, strinc
from fdbkafka.cdc.v1 import mutations_pb2

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1"
        or os.environ.get("KAFKA_INTEGRATION") != "1",
        reason="requires live FDB and Kafka",
    ),
]

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _wait(predicate: Callable[[], bool], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("Timed out waiting for bridge progress")
        time.sleep(0.1)


@pytest.fixture
def live() -> Iterator[tuple[FDBClient, CheckpointStore, str, bytes, str, Consumer]]:
    client = FDBClient()
    client.open()
    name = f"e2e-{uuid.uuid4().hex}"
    prefix = name.encode() + b":"
    topic = f"fdb.cdc.{name}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[
        topic
    ].result(30)
    store = CheckpointStore(
        client,
        sink=SinkIdentity(admin.list_topics(timeout=10).cluster_id, topic),
        path=("bridge-tests", name),
    )
    store.open()
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([topic])
    try:
        yield client, store, name, prefix, topic, consumer
    finally:
        consumer.close()
        client.remove_cdc_stream(name)
        del client.db[prefix : strinc(prefix)]
        begin, end = store.key_range()
        del client.db[begin:end]
        client.close()
        admin.delete_topics([topic])[topic].result(30)


def _pipeline(
    listener: FDBMutationListener, producer: Any, store: CheckpointStore, topic: str
) -> BridgePipeline:
    store.claim(listener.raw_stream_name)
    return BridgePipeline(
        listener,
        producer,
        topic,
        lambda cursor: store.save(listener.raw_stream_name, cursor),
    )


def _read_until(consumer: Consumer, target_version: int) -> list[Any]:
    messages: list[Any] = []
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        message = consumer.poll(1)
        if message is None:
            continue
        assert not message.error(), message.error()
        messages.append(message)
        record = mutations_pb2.FDBMutationRecord.FromString(message.value())
        if (
            record.HasField("version_end")
            and record.version_end.fdb_version >= target_version
        ):
            return messages
    pytest.fail("Kafka did not receive the final VersionEnd")


@pytest.mark.parametrize("transactional", [False, True])
def test_split_batches_versions_and_partition_order(
    transactional: bool,
    live: tuple[FDBClient, CheckpointStore, str, bytes, str, Consumer],
) -> None:
    from fdb_kafka_bridge.kafka import MutationProducer

    client, store, name, prefix, topic, consumer = live
    listener = FDBMutationListener(client, name, subspace=prefix, max_record_bytes=300)
    expected: dict[int, set[bytes]] = {}
    with listener:
        for group in range(3):
            tr = client.db.create_transaction()
            keys = {prefix + f"{group}-{index}".encode() for index in range(5)}
            for key in keys:
                tr[key] = b"v" * 100
            tr.commit().wait()
            expected[tr.get_committed_version()] = keys
        producer = MutationProducer(
            BOOTSTRAP, transactional_id=f"tx-{name}" if transactional else None
        )
        if transactional:
            producer.init_transactions()
        pipeline = _pipeline(listener, producer, store, topic)
        for _ in range(15):
            pipeline.step()
            cursor = store.load(name.encode())
            if cursor is not None and cursor.last_consumed_version >= max(expected):
                break
        else:
            pytest.fail("Checkpoint never reached final mutation version")
    producer.close()
    messages = _read_until(consumer, max(expected))
    assert len({m.partition() for m in messages}) == 1
    assert all(m.key() == name.encode() for m in messages)
    assert [m.offset() for m in messages] == sorted(m.offset() for m in messages)
    records = [mutations_pb2.FDBMutationRecord.FromString(m.value()) for m in messages]
    seen: dict[int, list[bytes]] = {}
    ends: set[int] = set()
    batches: dict[int, int] = {}
    for record in records:
        assert len(record.SerializeToString()) <= 300
        if record.HasField("batch"):
            version = record.batch.mutations[0].version_index.fdb_version
            assert version not in ends
            batches[version] = batches.get(version, 0) + 1
            for mutation in record.batch.mutations:
                assert mutation.version_index.sequence_no == len(
                    seen.setdefault(version, [])
                )
                seen[version].append(mutation.single_key_mutation.key)
        elif record.HasField("version_end"):
            version = record.version_end.fdb_version
            assert record.version_end.total_mutations == len(seen.get(version, []))
            ends.add(version)
    assert {v: set(keys) for v, keys in seen.items()} == expected
    assert set(expected) <= ends
    assert all(batches[v] > 1 for v in expected)


def test_cli_shutdown_and_restart(
    live: tuple[FDBClient, CheckpointStore, str, bytes, str, Consumer],
) -> None:
    client, store, name, prefix, topic, consumer = live
    command = [
        sys.executable,
        "-m",
        "fdb_kafka_bridge.bridge",
        "--stream-name",
        name,
        "--subspace-prefix",
        prefix.decode(),
        "--checkpoint-path",
        "/".join(store.path),
        "--kafka-bootstrap-servers",
        BOOTSTRAP,
        "--shutdown-timeout-s",
        "2",
    ]
    env = {**os.environ, "PYTHONPATH": ".:src:protobuf/gen"}
    versions: list[int] = []
    for index in range(2):
        process = subprocess.Popen(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait(
                lambda process=process: process.poll() is not None
                or any(
                    stream.name == name.encode() for stream in client.list_cdc_streams()
                )
            )
            assert process.poll() is None, process.communicate()[0]
            tr = client.db.create_transaction()
            tr[prefix + str(index).encode()] = b"daemon"
            tr.commit().wait()
            version = tr.get_committed_version()
            versions.append(version)

            def checkpointed(
                process: subprocess.Popen[str] = process, version: int = version
            ) -> bool:
                if process.poll() is not None:
                    pytest.fail(process.communicate()[0])
                cursor = store.load(name.encode())
                return cursor is not None and cursor.last_consumed_version >= version

            _wait(checkpointed)
            process.terminate()
            output, _ = process.communicate(timeout=30)
            assert process.returncode == 0, output
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=10)
    before_rejection = store.load(name.encode())
    rejected = subprocess.run(
        [*command, "--topic", topic + "-wrong"],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert rejected.returncode == 3, rejected.stderr
    assert store.load(name.encode()) == before_rejection
    messages = _read_until(consumer, max(versions))
    keys = {
        m.single_key_mutation.key
        for message in messages
        for record in [mutations_pb2.FDBMutationRecord.FromString(message.value())]
        if record.HasField("batch")
        for m in record.batch.mutations
    }
    assert {prefix + b"0", prefix + b"1"} <= keys


def test_rejected_publication_replays_without_checkpoint_loss(
    live: tuple[FDBClient, CheckpointStore, str, bytes, str, Consumer],
) -> None:
    from confluent_kafka import KafkaException

    from fdb_kafka_bridge.kafka import MutationProducer

    client, store, name, prefix, topic, consumer = live
    target = prefix + b"oversized"
    listener = FDBMutationListener(client, name, subspace=prefix)
    listener.start()
    tr = client.db.create_transaction()
    tr[target] = b"v" * 2500
    tr.commit().wait()
    target_version = tr.get_committed_version()
    limited = MutationProducer(BOOTSTRAP, extra_config={"message.max.bytes": 1000})
    pipeline = _pipeline(listener, limited, store, topic)
    before = None
    try:
        for _ in range(15):
            before = store.load(name.encode())
            try:
                pipeline.step()
            except KafkaException:
                break
        else:
            pytest.fail("Producer did not reject the oversized message")
        assert store.load(name.encode()) == before
        assert limited.closed
    finally:
        listener.close()
    resumed = FDBMutationListener(client, name, cursor=before)
    with resumed:
        producer = MutationProducer(BOOTSTRAP)
        pipeline = _pipeline(resumed, producer, store, topic)
        for _ in range(15):
            pipeline.step()
            after = store.load(name.encode())
            if after is not None and after.last_consumed_version >= target_version:
                break
        else:
            pytest.fail("Replay did not advance the checkpoint")
    producer.close()
    assert after is not None
    messages = _read_until(consumer, after.last_consumed_version)
    values = {
        m.single_key_mutation.key: m.single_key_mutation.value
        for message in messages
        for record in [mutations_pb2.FDBMutationRecord.FromString(message.value())]
        if record.HasField("batch")
        for m in record.batch.mutations
        if m.HasField("single_key_mutation")
    }
    assert values[target] == b"v" * 2500
