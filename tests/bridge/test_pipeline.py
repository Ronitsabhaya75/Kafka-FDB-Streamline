"""Unit tests for the CDC → Kafka bridge pipeline."""

from unittest.mock import MagicMock, PropertyMock

import pytest
from confluent_kafka import KafkaError, KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.pipeline import BridgePipeline


def _make_record(stream: str = "test") -> mutations_pb2.FDBMutationRecord:
    return mutations_pb2.FDBMutationRecord(stream_name=stream)


def _make_pipeline(
    *,
    transactional: bool = False,
    records: list[mutations_pb2.FDBMutationRecord] | None = None,
) -> tuple[BridgePipeline, MagicMock, MagicMock]:
    listener = MagicMock()
    listener.poll_records.return_value = records or []

    producer = MagicMock()
    type(producer).transactional = PropertyMock(return_value=transactional)

    pipeline = BridgePipeline(
        listener,
        producer,
        "cdc-topic",
        partition_key=b"test-stream",
    )
    return pipeline, listener, producer


# ── Empty poll ─────────────────────────────────────────────────────


def test_step_empty_poll_returns_zero() -> None:
    pipeline, listener, producer = _make_pipeline(records=[])

    assert pipeline.step() == 0

    listener.acknowledge.assert_not_called()
    producer.produce.assert_not_called()


# ── Idempotent (non-transactional) path ────────────────────────────


def test_idempotent_step_produces_and_acknowledges() -> None:
    r1, r2 = _make_record("s1"), _make_record("s2")
    pipeline, listener, producer = _make_pipeline(records=[r1, r2])

    count = pipeline.step()

    assert count == 2
    assert producer.produce.call_count == 2
    producer.produce.assert_any_call("cdc-topic", r1, key=b"test-stream")
    producer.produce.assert_any_call("cdc-topic", r2, key=b"test-stream")
    producer.flush.assert_called_once()
    listener.acknowledge.assert_called_once()


def test_idempotent_acknowledges_after_flush() -> None:
    pipeline, listener, producer = _make_pipeline(records=[_make_record()])
    call_order: list[str] = []
    producer.flush.side_effect = lambda: call_order.append("flush")
    listener.acknowledge.side_effect = lambda: call_order.append("ack")

    pipeline.step()

    assert call_order == ["flush", "ack"]


# ── Transactional path ─────────────────────────────────────────────


def test_transactional_step_wraps_in_transaction() -> None:
    r1 = _make_record()
    pipeline, listener, producer = _make_pipeline(transactional=True, records=[r1])
    call_order: list[str] = []
    producer.begin_transaction.side_effect = lambda: call_order.append("begin")
    producer.produce.side_effect = lambda *a, **kw: call_order.append("produce")
    producer.commit_transaction.side_effect = lambda *a, **kw: call_order.append(
        "commit"
    )
    listener.acknowledge.side_effect = lambda: call_order.append("ack")

    count = pipeline.step()

    assert count == 1
    assert call_order == ["begin", "produce", "commit", "ack"]


def test_transactional_abortable_error_aborts_no_ack() -> None:
    pipeline, listener, producer = _make_pipeline(
        transactional=True, records=[_make_record()]
    )
    error = MagicMock(spec=KafkaError)
    error.txn_requires_abort.return_value = True
    producer.commit_transaction.side_effect = KafkaException(error)

    count = pipeline.step()

    assert count == 0
    producer.abort_transaction.assert_called_once()
    listener.acknowledge.assert_not_called()


def test_transactional_retriable_error_aborts_no_ack() -> None:
    pipeline, listener, producer = _make_pipeline(
        transactional=True, records=[_make_record()]
    )
    error = MagicMock(spec=KafkaError)
    error.txn_requires_abort.return_value = False
    error.retriable.return_value = True
    producer.commit_transaction.side_effect = KafkaException(error)

    count = pipeline.step()

    assert count == 0
    producer.abort_transaction.assert_called_once()
    listener.acknowledge.assert_not_called()


def test_transactional_fatal_error_propagates() -> None:
    pipeline, listener, producer = _make_pipeline(
        transactional=True, records=[_make_record()]
    )
    error = MagicMock(spec=KafkaError)
    error.txn_requires_abort.return_value = False
    error.retriable.return_value = False
    producer.commit_transaction.side_effect = KafkaException(error)

    with pytest.raises(KafkaException):
        pipeline.step()

    listener.acknowledge.assert_not_called()


def test_transactional_abort_failure_re_raises_original() -> None:
    pipeline, listener, producer = _make_pipeline(
        transactional=True, records=[_make_record()]
    )
    error = MagicMock(spec=KafkaError)
    error.txn_requires_abort.return_value = True
    original = KafkaException(error)
    producer.commit_transaction.side_effect = original
    producer.abort_transaction.side_effect = KafkaException(MagicMock(spec=KafkaError))

    with pytest.raises(KafkaException) as raised:
        pipeline.step()

    assert raised.value is original


# ── Partition key ──────────────────────────────────────────────────


def test_no_partition_key() -> None:
    listener = MagicMock()
    listener.poll_records.return_value = [_make_record()]
    producer = MagicMock()
    type(producer).transactional = PropertyMock(return_value=False)

    pipeline = BridgePipeline(listener, producer, "topic")

    pipeline.step()

    _, kwargs = producer.produce.call_args
    assert kwargs["key"] is None
