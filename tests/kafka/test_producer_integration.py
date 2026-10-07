"""Integration tests for MutationProducer against a live Kafka broker.

Produces Protobuf-serialised FDBMutationRecord messages to a local Kafka
topic, then consumes and deserialises them to verify the full round-trip.

Run explicitly inside the devcontainer:
    KAFKA_INTEGRATION=1 pytest tests/test_kafka_producer.py -v -m integration
"""

import os
import time
import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.kafka.producer import MutationProducer
from tests.kafka.utils import (
    build_batch_record,
    build_set_mutation_record,
    build_version_end_record,
    make_timestamp,
)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Unique topic per test run avoids cross-run interference.
TEST_TOPIC = f"fdb-cdc-test-{uuid.uuid4().hex[:8]}"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("KAFKA_INTEGRATION") != "1",
        reason="set KAFKA_INTEGRATION=1 to run live Kafka tests",
    ),
]


# -- helpers ---------------------------------------------------------------


def _consume_n(consumer: Consumer, n: int, timeout: float = 15.0) -> list[bytes]:
    """Poll *n* messages from the consumer, raising on timeout."""
    messages: list[bytes] = []
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
def consumer() -> Iterator[Consumer]:
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


class TestProduceAndConsume:
    """End-to-end produce → consume → deserialise round-trip."""

    def test_single_mutation_roundtrip(
        self, producer: MutationProducer, consumer: Consumer
    ) -> None:
        record = build_set_mutation_record(
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
        record = build_version_end_record(
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
        record = build_batch_record(
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
            record = build_set_mutation_record(
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
            bridge_timestamp=make_timestamp(),
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
