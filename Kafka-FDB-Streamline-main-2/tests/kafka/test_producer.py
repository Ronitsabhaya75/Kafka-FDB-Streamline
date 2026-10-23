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


# ── Mandatory config ───────────────────────────────────────────────


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
    assert config["enable.gapless.guarantee"] is True


def test_transactional_config_omits_gapless(producer_class: MagicMock) -> None:
    MutationProducer("broker:9092", transactional_id="bridge-stream-0")

    config = producer_class.call_args.args[0]
    assert config["transactional.id"] == "bridge-stream-0"
    assert "enable.gapless.guarantee" not in config


def test_transactional_property(producer_class: MagicMock) -> None:
    plain = MutationProducer("broker:9092")
    txn = MutationProducer("broker:9092", transactional_id="t1")

    assert plain.transactional is False
    assert txn.transactional is True


# ── Produce / serialization ────────────────────────────────────────


def test_produce_serializes_record_and_polls(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    record = mutations_pb2.FDBMutationRecord(stream_name="orders")

    producer.produce("mutations", record, key=b"orders")

    kafka_client.produce.assert_called_once()
    args, kwargs = kafka_client.produce.call_args
    assert args == ("mutations",)
    assert kwargs["key"] == b"orders"
    assert kwargs["value"] == record.SerializeToString()
    assert callable(kwargs["on_delivery"])
    kafka_client.poll.assert_called_once_with(0)


def test_delivery_callback_is_forwarded(producer_class: MagicMock) -> None:
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


def test_flush_succeeds_when_all_delivered(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")

    producer.flush(timeout=3.5)
    kafka_client.flush.assert_called_once_with(3.5)


def test_flush_raises_on_timeout(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 3
    producer = MutationProducer("broker:9092")

    with pytest.raises(KafkaException):
        producer.flush()


def test_flush_raises_delivery_failure(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.produce("mutations", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException) as raised:
        producer.flush()

    assert raised.value.args == (error,)


# ── Sticky errors ──────────────────────────────────────────────────


def test_produce_raises_after_delivery_failure(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    producer.produce("mutations", mutations_pb2.FDBMutationRecord())

    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException) as raised:
        producer.produce("mutations", mutations_pb2.FDBMutationRecord())

    assert raised.value.args == (error,)
    assert kafka_client.produce.call_count == 1


def test_flush_raises_after_previous_delivery_failure(
    producer_class: MagicMock,
) -> None:
    """Once poisoned, flush raises immediately without calling librdkafka."""
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.produce("mutations", mutations_pb2.FDBMutationRecord())

    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    kafka_client.flush.return_value = 0
    with pytest.raises(KafkaException):
        producer.flush()

    # The sticky error means flush raises before calling librdkafka.flush again
    with pytest.raises(KafkaException):
        producer.flush()


def test_timeout_poisons_producer(producer_class: MagicMock) -> None:
    """A flush timeout must make the producer permanently unusable."""
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 2
    producer = MutationProducer("broker:9092")

    with pytest.raises(KafkaException):
        producer.flush()

    with pytest.raises(KafkaException):
        producer.produce("mutations", mutations_pb2.FDBMutationRecord())


# ── Transaction lifecycle ──────────────────────────────────────────


def test_init_transactions(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="t1")

    producer.init_transactions(timeout=15.0)

    kafka_client.init_transactions.assert_called_once_with(15.0)


def test_begin_commit_abort(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="t1")

    producer.begin_transaction()
    kafka_client.begin_transaction.assert_called_once()

    producer.commit_transaction(timeout=5.0)
    kafka_client.commit_transaction.assert_called_once_with(5.0)

    producer.abort_transaction(timeout=5.0)
    kafka_client.abort_transaction.assert_called_once_with(5.0)


def test_transaction_methods_require_transactional_id(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")

    with pytest.raises(RuntimeError):
        producer.init_transactions()
    with pytest.raises(RuntimeError):
        producer.begin_transaction()
    with pytest.raises(RuntimeError):
        producer.commit_transaction()
    with pytest.raises(RuntimeError):
        producer.abort_transaction()


# ── Close / len ────────────────────────────────────────────────────


def test_close_flushes(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")

    producer.close()

    kafka_client.flush.assert_called_once_with(10.0)


def test_close_does_not_clear_sticky_error(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    report(MagicMock(spec=KafkaError), MagicMock())

    producer.close()

    with pytest.raises(KafkaException):
        producer.produce("t", mutations_pb2.FDBMutationRecord())


def test_len_delegates_to_client(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.__len__.return_value = 4
    producer = MutationProducer("broker:9092")

    assert len(producer) == 4
