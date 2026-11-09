"""Unit tests for the Kafka mutation producer."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from fdb_kafka_bridge.kafka.producer import FatalCause, MutationProducer
from fdbkafka.cdc.v1 import mutations_pb2


@pytest.fixture
def producer_class() -> Iterator[MagicMock]:
    with patch("fdb_kafka_bridge.kafka.producer.Producer") as producer_class:
        yield producer_class


# ── Durability config ──────────────────────────────────────────────


# Pins INV-KAFKA-1.
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


# Pins INV-KAFKA-1.
def test_mandatory_keys_rejected_in_extra_config(
    producer_class: MagicMock,
) -> None:
    with pytest.raises(ValueError, match="extra_config must not contain"):
        MutationProducer(
            "broker:9092",
            extra_config={"acks": "1"},
        )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("request.required.acks", 1),
        ("delivery.timeout.ms", 1000),
        ("max.in.flight", 100),
    ],
)
# Pins INV-KAFKA-1.
def test_librdkafka_alias_keys_rejected_in_extra_config(
    producer_class: MagicMock, key: str, value: int
) -> None:
    with pytest.raises(ValueError, match=key):
        MutationProducer("broker:9092", extra_config={key: value})


# Pins INV-KAFKA-1.
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


# Pins INV-KAFKA-13.
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


# Pins INV-KAFKA-2.
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


# Pins INV-KAFKA-2.
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
    kafka_client.flush.assert_called_once()


# Pins INV-KAFKA-3.
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
    assert producer.fatal is False


# Pins INV-KAFKA-2, INV-KAFKA-3.
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


# Pins INV-KAFKA-9.
def test_len_raises_when_fatal(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        len(producer)


# Pins INV-KAFKA-9.
def test_len_raises_when_closed(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")
    producer.close()
    with pytest.raises(RuntimeError, match="closed"):
        len(producer)


# ── Transaction initialization tracking ──────────────────────────


# Pins INV-KAFKA-10.
def test_begin_before_init_fails(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.begin_transaction()


# Pins INV-KAFKA-10.
def test_commit_before_init_fails(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.commit_transaction()


# Pins INV-KAFKA-10.
def test_abort_before_init_fails(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    with pytest.raises(RuntimeError, match="init_transactions"):
        producer.abort_transaction()


# Pins INV-KAFKA-10.
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


# Pins INV-KAFKA-10.
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


# Pins INV-KAFKA-4.
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


# Pins INV-KAFKA-5.
def test_commit_clears_delivery_errors(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()

    producer.begin_transaction()
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    # Real librdkafka fails the commit here; this pins the clear on success only.
    producer.commit_transaction()
    assert producer._delivery_errors == []


# Pins INV-KAFKA-5.
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


# Pins INV-KAFKA-5, INV-KAFKA-7.
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
    assert producer.fatal is True
    assert producer._txn_open is True


# Pins INV-KAFKA-5.
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


# Pins INV-KAFKA-9.
def test_fatal_blocks_produce(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        producer.produce("t", mutations_pb2.FDBMutationRecord())


# Pins INV-KAFKA-9.
def test_fatal_blocks_flush(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        producer.flush()


# Pins INV-KAFKA-9.
def test_fatal_blocks_transaction_methods(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.mark_fatal()
    with pytest.raises(RuntimeError, match="fatal"):
        producer.begin_transaction()


# Pins INV-KAFKA-8.
def test_error_cb_non_fatal_does_not_mark_fatal(
    producer_class: MagicMock,
) -> None:
    producer = MutationProducer("broker:9092")
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = False

    producer._error_cb(err)
    assert producer.fatal is False


# Pins INV-KAFKA-13.
def test_user_error_cb_is_chained(producer_class: MagicMock) -> None:
    user_cb = MagicMock()
    producer = MutationProducer("broker:9092", extra_config={"error_cb": user_cb})
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True

    producer._error_cb(err)

    user_cb.assert_called_once_with(err)
    assert producer.fatal is True


# Pins INV-KAFKA-13.
def test_user_error_cb_exception_is_swallowed(
    producer_class: MagicMock,
) -> None:
    user_cb = MagicMock(side_effect=RuntimeError("user callback error"))
    producer = MutationProducer("broker:9092", extra_config={"error_cb": user_cb})
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = False

    producer._error_cb(err)
    user_cb.assert_called_once_with(err)


def _fatal_kafka_exception() -> KafkaException:
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True
    return KafkaException(err)


def _non_fatal_kafka_exception() -> KafkaException:
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = False
    return KafkaException(err)


def _open_txn_producer() -> MutationProducer:
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.begin_transaction()
    return producer


def test_fatal_cause_none_initially(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    assert producer.fatal_cause is None
    assert producer.fatal is False


def test_fatal_cause_exported_from_package() -> None:
    import fdb_kafka_bridge.kafka as kafka_pkg

    assert kafka_pkg.FatalCause is FatalCause
    assert "FatalCause" in kafka_pkg.__all__


def test_fatal_cause_values() -> None:
    assert FatalCause.KAFKA == "kafka"
    assert FatalCause.ABORT_FAILED == "abort_failed"


# Pins INV-KAFKA-8.
def test_error_cb_fatal_sets_kafka_cause(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True

    producer._error_cb(err)

    assert producer.fatal_cause is FatalCause.KAFKA
    assert producer.fatal is True


# Pins INV-KAFKA-8.
def test_mark_fatal_sets_kafka_cause(producer_class: MagicMock) -> None:
    producer = MutationProducer("broker:9092")
    producer.mark_fatal()
    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-8.
def test_init_fatal_kafka_error_sets_kafka_cause(producer_class: MagicMock) -> None:
    producer_class.return_value.init_transactions.side_effect = _fatal_kafka_exception()
    producer = MutationProducer("broker:9092", transactional_id="txn-1")

    with pytest.raises(KafkaException):
        producer.init_transactions()

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-8.
def test_begin_fatal_kafka_error_sets_kafka_cause(producer_class: MagicMock) -> None:
    producer_class.return_value.begin_transaction.side_effect = _fatal_kafka_exception()
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()

    with pytest.raises(KafkaException):
        producer.begin_transaction()

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-8.
def test_commit_fatal_kafka_error_sets_kafka_cause(producer_class: MagicMock) -> None:
    producer_class.return_value.commit_transaction.side_effect = (
        _fatal_kafka_exception()
    )
    producer = _open_txn_producer()

    with pytest.raises(KafkaException):
        producer.commit_transaction()

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-7.
def test_abort_fatal_kafka_error_sets_kafka_cause(producer_class: MagicMock) -> None:
    producer_class.return_value.abort_transaction.side_effect = _fatal_kafka_exception()
    producer = _open_txn_producer()

    with pytest.raises(KafkaException):
        producer.abort_transaction()

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-7.
def test_abort_non_fatal_kafka_error_sets_abort_failed_cause(
    producer_class: MagicMock,
) -> None:
    producer_class.return_value.abort_transaction.side_effect = (
        _non_fatal_kafka_exception()
    )
    producer = _open_txn_producer()

    with pytest.raises(KafkaException):
        producer.abort_transaction()

    assert producer.fatal_cause is FatalCause.ABORT_FAILED
    assert producer.fatal is True


# Pins INV-KAFKA-7.
def test_abort_non_kafka_exception_sets_abort_failed_cause(
    producer_class: MagicMock,
) -> None:
    producer_class.return_value.abort_transaction.side_effect = OSError("boom")
    producer = _open_txn_producer()

    with pytest.raises(OSError, match="boom"):
        producer.abort_transaction()

    assert producer.fatal_cause is FatalCause.ABORT_FAILED
    assert producer._txn_open is True


# Pins INV-KAFKA-8.
def test_kafka_cause_overrides_abort_failed(producer_class: MagicMock) -> None:
    producer_class.return_value.abort_transaction.side_effect = (
        _non_fatal_kafka_exception()
    )
    producer = _open_txn_producer()
    with pytest.raises(KafkaException):
        producer.abort_transaction()
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True

    producer._error_cb(err)

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-8.
def test_mark_fatal_overrides_abort_failed(producer_class: MagicMock) -> None:
    producer_class.return_value.abort_transaction.side_effect = OSError("boom")
    producer = _open_txn_producer()
    with pytest.raises(OSError):
        producer.abort_transaction()

    producer.mark_fatal()

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-8.
def test_abort_failed_never_downgrades_kafka(producer_class: MagicMock) -> None:
    producer = _open_txn_producer()
    err = MagicMock(spec=KafkaError)
    err.fatal.return_value = True

    # librdkafka can report a fatal error_cb while abort is in progress.
    def abort(timeout: float) -> None:
        producer._error_cb(err)
        raise OSError("boom")

    producer_class.return_value.abort_transaction.side_effect = abort

    with pytest.raises(OSError):
        producer.abort_transaction()

    assert producer.fatal_cause is FatalCause.KAFKA


# Pins INV-KAFKA-9.
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
    assert producer.fatal is False


# Pins INV-KAFKA-11.
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


# Pins INV-KAFKA-11.
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


# Pins INV-KAFKA-11.
def test_close_twice_is_noop(producer_class: MagicMock) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    producer = MutationProducer("broker:9092")

    producer.close()
    producer.close()

    kafka_client.flush.assert_called_once_with(10.0)
    assert producer.closed is True


# Pins INV-KAFKA-11.
def test_close_swallows_failed_abort(
    producer_class: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.flush.return_value = 0
    kafka_client.abort_transaction.side_effect = _non_fatal_kafka_exception()
    producer = _open_txn_producer()

    with caplog.at_level("DEBUG", logger="fdb_kafka_bridge.kafka.producer"):
        producer.close()

    assert producer.closed is True
    assert "Abort during close" in caplog.text


# ── Commit failure and closed/fatal precedence ────────────────────


# Pins INV-KAFKA-5, INV-KAFKA-6.
def test_non_fatal_failed_commit_leaves_transaction_open(
    producer_class: MagicMock,
) -> None:
    kafka_client = producer_class.return_value
    kafka_client.commit_transaction.side_effect = _non_fatal_kafka_exception()
    producer = _open_txn_producer()
    producer.produce("t", mutations_pb2.FDBMutationRecord())
    report = kafka_client.produce.call_args.kwargs["on_delivery"]
    error = MagicMock(spec=KafkaError)
    report(error, MagicMock())

    with pytest.raises(KafkaException):
        producer.commit_transaction()

    assert producer.fatal is False
    assert producer._delivery_errors == [error]
    kafka_client.abort_transaction.side_effect = None
    producer.abort_transaction()
    kafka_client.abort_transaction.assert_called_once()


# Pins INV-KAFKA-9.
def test_closed_reported_before_fatal(producer_class: MagicMock) -> None:
    producer_class.return_value.flush.return_value = 0
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.init_transactions()
    producer.mark_fatal()
    producer.close()

    with pytest.raises(RuntimeError, match="closed"):
        producer.produce("t", mutations_pb2.FDBMutationRecord())
    with pytest.raises(RuntimeError, match="closed"):
        producer.flush()
    with pytest.raises(RuntimeError, match="closed"):
        producer.begin_transaction()


@pytest.mark.parametrize(
    "method",
    [
        "init_transactions",
        "begin_transaction",
        "commit_transaction",
        "abort_transaction",
    ],
)
# Pins INV-KAFKA-9.
def test_closed_blocks_transaction_methods(
    producer_class: MagicMock, method: str
) -> None:
    producer_class.return_value.flush.return_value = 0
    producer = MutationProducer("broker:9092", transactional_id="txn-1")
    producer.close()

    with pytest.raises(
        RuntimeError, match=f"{method} cannot proceed: producer is closed"
    ):
        getattr(producer, method)()
