"""Live Kafka check that one bridge step stays on a single partition.

Run with the broker up:

    KAFKA_INTEGRATION=1 pytest tests/bridge -m integration
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.pipeline import BridgePipeline
from src.kafka.producer import MutationProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PARTITIONS = 4
RECORD_COUNT = 8

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("KAFKA_INTEGRATION") != "1",
        reason="set KAFKA_INTEGRATION=1 to run live Kafka tests",
    ),
]


class _Reply:
    """One CDC reply and the cursor a caller would persist before ack."""

    def __init__(self, records: list[mutations_pb2.FDBMutationRecord]) -> None:
        self.raw_stream_name = b"orders"
        self.stream_name = "orders"
        self._records = records
        self._polls = 0
        self.cursor = {"last_consumed_version": 42}
        self.acked = False

    def poll_records(self) -> list[mutations_pb2.FDBMutationRecord]:
        self._polls += 1
        if self._polls > 1:
            raise AssertionError("pipeline polled a second reply")
        return self._records

    def get_position(self) -> dict[str, int]:
        return self.cursor

    def acknowledge(self) -> None:
        self.acked = True


def _no_error(error: object) -> bool:
    if error is None:
        return True
    code = getattr(error, "code", None)
    return callable(code) and code() == 0


def _create_topic(admin: AdminClient, name: str) -> None:
    future = admin.create_topics(
        [NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)]
    )[name]
    future.result(timeout=30)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(name, timeout=5)
        topic = metadata.topics.get(name)
        if (
            topic is not None
            and len(topic.partitions) >= PARTITIONS
            and _no_error(topic.error)
        ):
            return
        time.sleep(0.2)
    raise AssertionError(f"topic {name} did not reach {PARTITIONS} partitions")


def _consume_n(
    consumer: Consumer, count: int, timeout: float = 20.0
) -> list[tuple[int, bytes | None]]:
    found: list[tuple[int, bytes | None]] = []
    deadline = time.monotonic() + timeout
    while len(found) < count and time.monotonic() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        found.append((msg.partition(), msg.key()))
    if len(found) < count:
        pytest.fail(f"Only consumed {len(found)}/{count} messages within {timeout}s")
    return found


@pytest.fixture
def topic() -> Iterator[str]:
    name = f"fdb-bridge-part-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    _create_topic(admin, name)
    yield name
    admin.delete_topics([name])


def test_step_places_batch_on_one_partition(topic: str) -> None:
    records = [
        mutations_pb2.FDBMutationRecord(stream_name="orders")
        for _ in range(RECORD_COUNT)
    ]
    listener = _Reply(records)
    saved: list[dict[str, int]] = []
    producer = MutationProducer(BOOTSTRAP_SERVERS)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"bridge-part-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        pipeline = BridgePipeline(
            listener,  # type: ignore[arg-type]
            producer,
            topic,
            saved.append,
            ack_retry_delay_seconds=0,
            commit_retry_delay_seconds=0,
        )
        assert pipeline.step() == RECORD_COUNT
        assert saved == [listener.cursor]
        assert listener.acked is True

        consumer.assign(
            [TopicPartition(topic, partition) for partition in range(PARTITIONS)]
        )
        placed = _consume_n(consumer, RECORD_COUNT)
    finally:
        consumer.close()
        producer.close()

    partitions = {partition for partition, _key in placed}
    keys = {key for _partition, key in placed}
    assert partitions == {placed[0][0]}
    assert keys == {b"orders"}
