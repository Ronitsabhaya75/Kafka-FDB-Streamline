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


def test_len_delegates_to_client(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.__len__.return_value = 4
    producer = MutationProducer("broker:9092")

    assert len(producer) == 4


def test_produce_raises_after_delivery_failure(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    producer.produce("mutations", mutations_pb2.FDBMutationRecord())

    # Simulate a delivery error reported by a previous produce poll/callback
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    # The next produce must immediately raise without attempting delivery
    with pytest.raises(KafkaException) as raised:
        producer.produce("mutations", mutations_pb2.FDBMutationRecord())

    assert raised.value.args == (error,)
    # The inner producer's produce method should not have been called a second time
    assert kafka_client.produce.call_count == 1
