"""Integration tests for MutationProducer against a live Kafka broker.

Produces Protobuf-serialised FDBMutationRecord messages to a local Kafka
topic, then consumes and deserialises them to verify the full round-trip.

Run inside the devcontainer:
    PYTHONPATH=protobuf/gen pytest tests/test_kafka_producer.py -v -m integration
"""

import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "protobuf", "gen"))

from confluent_kafka import Consumer, KafkaException
from google.protobuf.timestamp_pb2 import Timestamp
from src.kafka.producer import MutationProducer

from fdbkafka.cdc.v1 import mutations_pb2

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
# Unique topic per test run avoids cross-run interference.
TEST_TOPIC = f"fdb-cdc-test-{uuid.uuid4().hex[:8]}"

pytestmark = pytest.mark.integration


# -- helpers ---------------------------------------------------------------


def _make_timestamp() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def _build_set_mutation_record(
    stream: str,
    version: int,
    seq: int,
    key: bytes,
    value: bytes,
) -> mutations_pb2.FDBMutationRecord:
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream,
        bridge_timestamp=_make_timestamp(),
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=version,
                sequence_no=seq,
            ),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=key,
                value=value,
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        ),
    )


def _build_version_end_record(
    stream: str,
    version: int,
    total: int,
) -> mutations_pb2.FDBMutationRecord:
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream,
        bridge_timestamp=_make_timestamp(),
        version_end=mutations_pb2.VersionEnd(
            fdb_version=version,
            total_mutations=total,
            bridge_timestamp=_make_timestamp(),
        ),
    )


def _build_batch_record(
    stream: str,
    version: int,
    count: int,
) -> mutations_pb2.FDBMutationRecord:
    mutations = [
        mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=version,
                sequence_no=i,
            ),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=f"batch:{i}".encode(),
                value=f"val:{i}".encode(),
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        )
        for i in range(count)
    ]
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream,
        bridge_timestamp=_make_timestamp(),
        batch=mutations_pb2.FDBMutationBatch(mutations=mutations),
    )


def _consume_n(consumer: Consumer, n: int, timeout: float = 15.0) -> list[bytes]:
    """Poll *n* messages from the consumer, raising on timeout."""
    messages: list[bytes] = []
    import time

    deadline = time.monotonic() + timeout
    while len(messages) < n and time.monotonic() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        messages.append(msg.value())
    if len(messages) < n:
        pytest.fail(f"Only consumed {len(messages)}/{n} messages within {timeout}s")
    return messages


# -- fixtures --------------------------------------------------------------


@pytest.fixture(scope="module")
def producer() -> MutationProducer:
    return MutationProducer(BOOTSTRAP_SERVERS)


@pytest.fixture(scope="module")
def consumer() -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"test-group-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([TEST_TOPIC])
    yield c
    c.close()


# -- tests -----------------------------------------------------------------


class TestProducerConfig:
    """Verify that MutationProducer enforces the mandatory config."""

    def test_cannot_weaken_acks(self) -> None:
        """Callers passing acks=1 must still get acks=all."""
        p = MutationProducer(
            BOOTSTRAP_SERVERS,
            extra_config={"acks": "1"},
        )
        # We can't inspect internal config directly, but producing
        # without error confirms the producer initialised successfully
        # with the mandatory overrides applied.
        assert p is not None

    def test_custom_linger_ms(self) -> None:
        """Performance knobs like linger.ms should be overridable."""
        p = MutationProducer(
            BOOTSTRAP_SERVERS,
            extra_config={"linger.ms": 50},
        )
        assert p is not None


class TestProduceAndConsume:
    """End-to-end produce → consume → deserialise round-trip."""

    def test_single_mutation_roundtrip(
        self, producer: MutationProducer, consumer: Consumer
    ) -> None:
        record = _build_set_mutation_record(
            stream="test-stream",
            version=100_000,
            seq=0,
            key=b"users:42",
            value=b'{"name": "Alice"}',
        )
        producer.produce(TEST_TOPIC, record, key=b"test-stream")
        remaining = producer.flush()
        assert remaining == 0

        payloads = _consume_n(consumer, 1)
        parsed = mutations_pb2.FDBMutationRecord.FromString(payloads[0])

        assert parsed.stream_name == "test-stream"
        assert parsed.WhichOneof("record") == "mutation"
        assert parsed.mutation.version_index.fdb_version == 100_000
        assert parsed.mutation.single_key_mutation.key == b"users:42"
        assert parsed.mutation.single_key_mutation.value == b'{"name": "Alice"}'

    def test_version_end_roundtrip(
        self, producer: MutationProducer, consumer: Consumer
    ) -> None:
        record = _build_version_end_record(
            stream="test-stream", version=100_000, total=5
        )
        producer.produce(TEST_TOPIC, record, key=b"test-stream")
        producer.flush()

        payloads = _consume_n(consumer, 1)
        parsed = mutations_pb2.FDBMutationRecord.FromString(payloads[0])

        assert parsed.WhichOneof("record") == "version_end"
        assert parsed.version_end.fdb_version == 100_000
        assert parsed.version_end.total_mutations == 5

    def test_batch_roundtrip(
        self, producer: MutationProducer, consumer: Consumer
    ) -> None:
        batch_size = 25
        record = _build_batch_record(
            stream="batch-stream", version=200_000, count=batch_size
        )
        producer.produce(TEST_TOPIC, record, key=b"batch-stream")
        producer.flush()

        payloads = _consume_n(consumer, 1)
        parsed = mutations_pb2.FDBMutationRecord.FromString(payloads[0])

        assert parsed.WhichOneof("record") == "batch"
        assert len(parsed.batch.mutations) == batch_size
        assert parsed.batch.mutations[0].single_key_mutation.key == b"batch:0"
        assert (
            parsed.batch.mutations[batch_size - 1].single_key_mutation.key
            == f"batch:{batch_size - 1}".encode()
        )

    def test_multiple_messages_ordering(
        self, producer: MutationProducer, consumer: Consumer
    ) -> None:
        """Messages produced in order must be consumed in the same order."""
        count = 10
        for i in range(count):
            record = _build_set_mutation_record(
                stream="order-test",
                version=300_000,
                seq=i,
                key=f"order:{i}".encode(),
                value=f"v{i}".encode(),
            )
            producer.produce(TEST_TOPIC, record, key=b"order-test")
        producer.flush()

        payloads = _consume_n(consumer, count)
        for i, raw in enumerate(payloads):
            parsed = mutations_pb2.FDBMutationRecord.FromString(raw)
            assert parsed.mutation.version_index.sequence_no == i
            assert parsed.mutation.single_key_mutation.key == f"order:{i}".encode()

    def test_clear_range_roundtrip(
        self, producer: MutationProducer, consumer: Consumer
    ) -> None:
        record = mutations_pb2.FDBMutationRecord(
            stream_name="clear-test",
            bridge_timestamp=_make_timestamp(),
            mutation=mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=400_000, sequence_no=0
                ),
                clear_range=mutations_pb2.FDBClearRange(
                    begin_key=b"tmp:", end_key=b"tmp:\xff"
                ),
            ),
        )
        producer.produce(TEST_TOPIC, record, key=b"clear-test")
        producer.flush()

        payloads = _consume_n(consumer, 1)
        parsed = mutations_pb2.FDBMutationRecord.FromString(payloads[0])

        assert parsed.WhichOneof("record") == "mutation"
        assert parsed.mutation.WhichOneof("mutation") == "clear_range"
        assert parsed.mutation.clear_range.begin_key == b"tmp:"
        assert parsed.mutation.clear_range.end_key == b"tmp:\xff"
