"""Unit tests for the Kafka mutation producer."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.kafka.producer import MutationProducer


@pytest.fixture
def producer_class() -> Iterator[MagicMock]:
    with patch("src.kafka.producer.Producer") as producer_class:
        yield producer_class


# ── Durability config ──────────────────────────────────────────────


def test_enforces_durability_config(producer_class: MagicMock) -> None:
    MutationProducer(
        "broker:9092",
        extra_config={
            "acks": "1",
            "enable.idempotence": False,
            "max.in.flight.requests.per.connection": 100,
            "linger.ms": 20,
        },
    )
    config = producer_class.call_args.args[0]
    assert config["bootstrap.servers"] == "broker:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["max.in.flight.requests.per.connection"] == 5
    assert config["linger.ms"] == 20


def test_default_performance_config(producer_class: MagicMock) -> None:
    MutationProducer("broker:9092")
    config = producer_class.call_args.args[0]
    assert config["linger.ms"] == 5
    assert config["compression.type"] == "lz4"


def test_transactional_id_passed_to_librdkafka(
    producer_class: MagicMock,
) -> None:
    p = MutationProducer("broker:9092", transactional_id="fdb-cdc-stream")
    config = producer_class.call_args.args[0]
    assert config["transactional.id"] == "fdb-cdc-stream"
    assert p.transactional is True


def test_non_transactional_by_default(producer_class: MagicMock) -> None:
    p = MutationProducer("broker:9092")
    assert p.transactional is False


# ── Produce and poll ───────────────────────────────────────────────


def test_produce_serializes_record_and_polls(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    record = mutations_pb2.FDBMutationRecord(stream_name="orders")

    producer.produce("mutations", record, key=b"orders")

    kafka_client.produce.assert_called_once()
    _, kwargs = kafka_client.produce.call_args
    assert kwargs["key"] == b"orders"
    assert kwargs["value"] == record.SerializeToString()
    assert callable(kwargs["on_delivery"])
    kafka_client.poll.assert_called_once_with(0)


def test_produce_without_key(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    record = mutations_pb2.FDBMutationRecord(stream_name="s")

    producer.produce("t", record)

    _, kwargs = kafka_client.produce.call_args
    assert "key" not in kwargs


def test_delivery_callback_forwarded(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    callback = MagicMock()
    producer = MutationProducer("broker:9092")
    producer.produce(
        "mutations",
        mutations_pb2.FDBMutationRecord(),
        on_delivery=callback,
    )
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    message = MagicMock()

    report(None, message)

    callback.assert_called_once_with(None, message)


# ── Flush ──────────────────────────────────────────────────────────


def test_flush_returns_undelivered_count(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 2
    producer = MutationProducer("broker:9092")

    assert producer.flush(timeout=3.5) == 2
    kafka_client.flush.assert_called_once_with(3.5)


def test_flush_raises_delivery_failure(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    producer.produce("mutations", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException) as raised:
        producer.flush()

    assert raised.value.args == (error,)


# ── Queue length ───────────────────────────────────────────────────


def test_len_delegates_to_client(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.__len__.return_value = 4
    producer = MutationProducer("broker:9092")

    assert len(producer) == 4


# ── Close ──────────────────────────────────────────────────────────


def test_close_flushes(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")

    producer.close()

    kafka_client.flush.assert_called_once_with(10.0)


# ── Transaction guards ────────────────────────────────────────────


def test_init_transactions_rejects_non_transactional(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")
    with pytest.raises(RuntimeError, match="transactional_id"):
        producer.init_transactions()


def test_begin_transaction_rejects_non_transactional(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")
    with pytest.raises(RuntimeError, match="transactional_id"):
        producer.begin_transaction()


def test_commit_transaction_rejects_non_transactional(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")
    with pytest.raises(RuntimeError, match="transactional_id"):
        producer.commit_transaction()


def test_abort_transaction_rejects_non_transactional(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")
    with pytest.raises(RuntimeError, match="transactional_id"):
        producer.abort_transaction()


def test_transactional_methods_delegate(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="fdb-cdc-test")

    producer.init_transactions(timeout=15.0)
    kafka_client.init_transactions.assert_called_once_with(15.0)

    producer.begin_transaction()
    kafka_client.begin_transaction.assert_called_once()

    producer.commit_transaction(timeout=20.0)
    kafka_client.commit_transaction.assert_called_once_with(20.0)

    producer.abort_transaction(timeout=5.0)
    kafka_client.abort_transaction.assert_called_once_with(5.0)
