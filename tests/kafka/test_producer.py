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
    MutationProducer("broker:9092", extra_config={"linger.ms": 20})

    config = producer_class.call_args.args[0]
    assert config["bootstrap.servers"] == "broker:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["max.in.flight.requests.per.connection"] == 5
    assert config["enable.gapless.guarantee"] is True
    assert config["message.timeout.ms"] == 0
    assert config["linger.ms"] == 20


def test_mandatory_keys_rejected_in_extra_config(
    producer_class: MagicMock,
) -> None:
    with pytest.raises(ValueError, match="extra_config must not contain"):
        MutationProducer(
            "broker:9092",
            extra_config={"acks": "1"},
        )


def test_transactional_id_in_extra_config_rejected(
    producer_class: MagicMock,
) -> None:
    with pytest.raises(ValueError, match="transactional.id"):
        MutationProducer(
            "broker:9092",
            extra_config={"transactional.id": "sneaky"},
        )


def test_default_performance_config(producer_class: MagicMock) -> None:
    MutationProducer("broker:9092")
    config = producer_class.call_args.args[0]
    assert config["linger.ms"] == 5
    assert config["compression.type"] == "lz4"


# ── Transactional ID validation ──────────────────────────────────


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


def test_empty_transactional_id_rejected(producer_class: MagicMock) -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        MutationProducer("broker:9092", transactional_id="")


# ── Produce and poll ──────────────────────────────────────────────


def test_produce_serializes_record_and_polls(
    producer_class: MagicMock,
) -> None:
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


def test_produce_without_key(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    record = mutations_pb2.FDBMutationRecord(stream_name="s")

    producer.produce("t", record)

    _, kwargs = kafka_client.produce.call_args
    assert "key" not in kwargs


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


def test_user_on_delivery_exception_does_not_escape(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    bad_cb = MagicMock(side_effect=ValueError("callback crash"))
    producer = MutationProducer("broker:9092")
    producer.produce(
        "mutations",
        mutations_pb2.FDBMutationRecord(),
        on_delivery=bad_cb,
    )
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    message = MagicMock()

    # Must not raise despite the callback raising ValueError
    report(error, message)
    bad_cb.assert_called_once_with(error, message)
    assert producer._delivery_errors == [error]


# ── Sticky delivery errors (nontransactional) ────────────────────


def test_produce_raises_after_delivery_failure(
    producer_class: MagicMock,
) -> None:
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


def test_sticky_error_persists_across_flushes(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    producer.produce("mutations", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException):
        producer.flush()

    with pytest.raises(KafkaException) as raised:
        producer.flush()
    assert raised.value.args == (error,)


def test_sticky_error_blocks_produce(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092")
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException):
        producer.flush()

    with pytest.raises(KafkaException):
        producer.produce("t", mutations_pb2.FDBMutationRecord())

    assert kafka_client.produce.call_count == 1


# ── Flush ─────────────────────────────────────────────────────────


def test_flush_returns_undelivered_count(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 2
    producer = MutationProducer("broker:9092")

    assert producer.flush(timeout=3.5) == 2
    kafka_client.flush.assert_called_once_with(3.5)


def test_flush_or_raise_success(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")

    producer.flush_or_raise(timeout=5.0)
    kafka_client.flush.assert_called_once_with(5.0)


def test_flush_or_raise_raises_timeout_error(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 2
    producer = MutationProducer("broker:9092")

    with pytest.raises(TimeoutError, match="2 message"):
        producer.flush_or_raise(timeout=3.0)


# ── Queue length ──────────────────────────────────────────────────


def test_len_delegates_to_client(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.__len__.return_value = 4
    producer = MutationProducer("broker:9092")

    assert len(producer) == 4


def test_len_raises_when_fatal(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        len(producer)


def test_len_raises_when_closed(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.close()
    with pytest.raises(RuntimeError, match="closed"):
        len(producer)


# ── Transaction initialization tracking ──────────────────────────


def test_begin_before_init_fails(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.begin_transaction()


def test_commit_before_init_fails(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.commit_transaction()


def test_abort_before_init_fails(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.abort_transaction()


def test_failed_init_does_not_mark_initialized(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.init_transactions.side_effect = KafkaException(
        MagicMock(spec=KafkaError)
    )
    producer = MutationProducer("broker:9092", transactional_id="txn-1")

    with pytest.raises(KafkaException):
        producer.init_transactions()

    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.begin_transaction()


def test_successful_init_enables_transactions(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="txn-1")

    producer.init_transactions(timeout=15.0)
    kafka_client.init_transactions.assert_called_once_with(15.0)

    producer.begin_transaction()
    kafka_client.begin_transaction.assert_called_once()


def test_init_transactions_cannot_be_called_twice(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    with pytest.raises(RuntimeError, match="already been called"):
        producer.init_transactions()


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


def test_produce_in_transactional_mode_requires_open_transaction(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    with pytest.raises(RuntimeError, match="open transaction"):
        producer.produce("t", mutations_pb2.FDBMutationRecord())


def test_begin_transaction_when_already_open_raises(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.begin_transaction()
    with pytest.raises(RuntimeError, match="already open"):
        producer.begin_transaction()


def test_commit_without_open_transaction_raises(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    with pytest.raises(RuntimeError, match="without an open transaction"):
        producer.commit_transaction()


def test_abort_without_open_transaction_raises(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    with pytest.raises(RuntimeError, match="without an open transaction"):
        producer.abort_transaction()


# ── Transaction-scoped error handling ─────────────────────────────


def test_commit_clears_delivery_errors(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()

    producer.begin_transaction()
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    producer.commit_transaction()
    assert producer._delivery_errors == []


def test_abort_clears_delivery_errors(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()

    producer.begin_transaction()
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    producer.abort_transaction()
    assert producer._delivery_errors == []


def test_failed_abort_preserves_errors_and_marks_fatal(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.abort_transaction.side_effect = KafkaException(
        MagicMock(spec=KafkaError)
    )
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()

    producer.begin_transaction()
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException):
        producer.abort_transaction()
    assert len(producer._delivery_errors) == 1
    assert producer._fatal is True


def test_produce_abort_then_clean_flush(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.begin_transaction()
    producer.produce("t", mutations_pb2.FDBMutationRecord())

    # Simulate delivery error reported during transaction
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())
    assert len(producer._delivery_errors) == 1

    producer.abort_transaction()
    assert len(producer._delivery_errors) == 0

    # Next transaction: produce -> flush -> commit succeeds cleanly without raising
    producer.begin_transaction()
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    assert producer.flush() == 0
    producer.commit_transaction()


# ── Fatal and closed states ───────────────────────────────────────


def test_fatal_blocks_produce(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        producer.produce("t", mutations_pb2.FDBMutationRecord())


def test_fatal_blocks_flush(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        producer.flush()


def test_fatal_blocks_transaction_methods(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        producer.begin_transaction()


def test_error_cb_fatal_marks_producer_fatal(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True

    producer._error_cb(err)
    assert producer._fatal is True


def test_error_cb_non_fatal_does_not_mark_fatal(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = False

    producer._error_cb(err)
    assert producer._fatal is False


def test_user_error_cb_is_chained(producer_class: MagicMock) -> None:
    user_cb = MagicMock()
    producer = MutationProducer("broker:9092", extra_config={"error_cb": user_cb})
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True

    producer._error_cb(err)

    user_cb.assert_called_once_with(err)
    assert producer._fatal is True


def test_user_error_cb_exception_is_swallowed(
    producer_class: MagicMock,
) -> None:
    user_cb = MagicMock(side_effect=RuntimeError("user callback error"))
    producer = MutationProducer("broker:9092", extra_config={"error_cb": user_cb})
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = False

    producer._error_cb(err)
    user_cb.assert_called_once_with(err)


def test_commit_fatal_kafka_error_marks_producer_fatal(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True
    kafka_client.commit_transaction.side_effect = KafkaException(err)

    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.begin_transaction()

    with pytest.raises(KafkaException):
        producer.commit_transaction()

    assert producer._fatal is True


def test_use_after_close_raises_closed_error(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.close()

    with pytest.raises(RuntimeError, match="closed"):
        producer.produce("t", mutations_pb2.FDBMutationRecord())

    with pytest.raises(RuntimeError, match="closed"):
        producer.flush()

    with pytest.raises(RuntimeError, match="closed"):
        producer.flush_or_raise()

    with pytest.raises(RuntimeError, match="closed"):
        len(producer)


# ── Transactional methods delegate ───────────────────────────────


def test_transactional_methods_delegate(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="fdb-cdc-test")

    producer.init_transactions(timeout=15.0)
    kafka_client.init_transactions.assert_called_once_with(15.0)

    producer.begin_transaction()
    kafka_client.begin_transaction.assert_called_once()

    producer.commit_transaction(timeout=20.0)
    kafka_client.commit_transaction.assert_called_once_with(20.0)

    producer.begin_transaction()
    producer.abort_transaction(timeout=5.0)
    kafka_client.abort_transaction.assert_called_once_with(5.0)


# ── Close ─────────────────────────────────────────────────────────


def test_close_flushes(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")

    producer.close()

    kafka_client.flush.assert_called_once_with(10.0)


def test_close_marks_closed(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.close()
    assert producer._closed is True
    assert producer._fatal is False


def test_close_aborts_open_transaction(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.begin_transaction()

    producer.close()

    kafka_client.abort_transaction.assert_called_once_with(10.0)
    kafka_client.flush.assert_called_once_with(10.0)
    assert producer._txn_open is False
    assert producer._closed is True


def test_close_does_not_abort_when_no_transaction_open(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()

    producer.close()

    kafka_client.abort_transaction.assert_not_called()
    kafka_client.flush.assert_called_once_with(10.0)


def test_close_logs_warning_when_flush_raises(
    producer_class: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.side_effect = KafkaException(MagicMock(spec=KafkaError))
    kafka_client.__len__.return_value = 3
    producer = MutationProducer("broker:9092")

    producer.close()

    assert "3 message(s) could not be delivered during close" in caplog.text
    assert producer._closed is True
