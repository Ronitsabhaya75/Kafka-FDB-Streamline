"""Unit tests for the CDC → Kafka bridge pipeline.

Uses ``create_autospec`` for listener and producer so that tests fail when
the pipeline calls methods that don't exist or with the wrong signature,
and so that return values (like ``flush()`` returning 0) match their real
types.
"""

from unittest.mock import PropertyMock, create_autospec

import pytest
from confluent_kafka import KafkaError, KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.pipeline import BridgePipeline, PublishFailedError
from src.cdc.errors import CDCRetryableError
from src.cdc.listener import FDBMutationListener
from src.kafka.producer import MutationProducer


def _make_record(stream: str = "test") -> mutations_pb2.FDBMutationRecord:
    return mutations_pb2.FDBMutationRecord(stream_name=stream)


def _make_pipeline(
    *,
    transactional: bool = False,
    records: list[mutations_pb2.FDBMutationRecord] | None = None,
    partition_key: bytes | None = b"test-stream",
    ack_retries: int = 3,
) -> tuple[BridgePipeline, MutationProducer, FDBMutationListener]:
    listener = create_autospec(FDBMutationListener, instance=True)
    listener.poll_records.return_value = records or []

    producer = create_autospec(MutationProducer, instance=True)
    type(producer).transactional = PropertyMock(return_value=transactional)
    producer.flush.return_value = 0

    pipeline = BridgePipeline(
        listener,
        producer,
        "cdc-topic",
        partition_key=partition_key,
        ack_retries=ack_retries,
    )
    return pipeline, producer, listener


def _kafka_error(
    *,
    fatal: bool = False,
    retriable: bool = False,
    txn_requires_abort: bool = False,
) -> KafkaError:
    error = create_autospec(KafkaError, instance=True)
    error.fatal.return_value = fatal
    error.retriable.return_value = retriable
    error.txn_requires_abort.return_value = txn_requires_abort
    return error


# ── Empty poll ─────────────────────────────────────────────────────


class TestEmptyPoll:
    def test_returns_zero(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[])

        assert pipeline.step() == 0

        listener.acknowledge.assert_not_called()
        producer.produce.assert_not_called()


# ── Idempotent (non-transactional) path ────────────────────────────


class TestIdempotentPath:
    def test_produces_and_acknowledges(self) -> None:
        r1, r2 = _make_record("s1"), _make_record("s2")
        pipeline, producer, listener = _make_pipeline(records=[r1, r2])

        count = pipeline.step()

        assert count == 2
        assert producer.produce.call_count == 2
        producer.produce.assert_any_call("cdc-topic", r1, key=b"test-stream")
        producer.produce.assert_any_call("cdc-topic", r2, key=b"test-stream")
        producer.flush.assert_called_once()
        listener.acknowledge.assert_called_once()

    def test_correct_ordering_produce_flush_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        call_order: list[str] = []
        producer.produce.side_effect = lambda *a, **kw: call_order.append("produce")
        producer.flush.side_effect = lambda *a, **kw: (call_order.append("flush") or 0)
        listener.acknowledge.side_effect = lambda: call_order.append("ack")

        pipeline.step()

        assert call_order == ["produce", "flush", "ack"]

    def test_flush_timeout_prevents_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        producer.flush.return_value = 3

        with pytest.raises(PublishFailedError, match="still in queue"):
            pipeline.step()

        listener.acknowledge.assert_not_called()

    def test_flush_exception_prevents_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        producer.flush.side_effect = KafkaException(_kafka_error())

        with pytest.raises(PublishFailedError, match="idempotent mode"):
            pipeline.step()

        listener.acknowledge.assert_not_called()

    def test_delivery_error_prevents_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        producer.produce.side_effect = KafkaException(_kafka_error())

        with pytest.raises(PublishFailedError):
            pipeline.step()

        listener.acknowledge.assert_not_called()

    def test_buffer_error_prevents_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        producer.produce.side_effect = BufferError("queue full")

        with pytest.raises(PublishFailedError, match="idempotent mode"):
            pipeline.step()

        listener.acknowledge.assert_not_called()

    def test_transaction_methods_never_called(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])

        pipeline.step()

        producer.begin_transaction.assert_not_called()
        producer.commit_transaction.assert_not_called()
        producer.abort_transaction.assert_not_called()


# ── Transactional path ─────────────────────────────────────────────


class TestTransactionalPath:
    def test_wraps_in_transaction(self) -> None:
        r1 = _make_record()
        pipeline, producer, listener = _make_pipeline(transactional=True, records=[r1])
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

    def test_flush_never_used(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )

        pipeline.step()

        producer.flush.assert_not_called()

    def test_abortable_error_aborts_no_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        error = _kafka_error(txn_requires_abort=True)
        producer.commit_transaction.side_effect = KafkaException(error)

        with pytest.raises(PublishFailedError):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_retriable_error_aborts_no_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        error = _kafka_error(retriable=True)
        producer.commit_transaction.side_effect = KafkaException(error)

        with pytest.raises(PublishFailedError):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_fatal_error_propagates_and_marks_fatal(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        error = _kafka_error(fatal=True)
        producer.commit_transaction.side_effect = KafkaException(error)

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.mark_fatal.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_abort_failure_chains_original_exception(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        error = _kafka_error(txn_requires_abort=True)
        original = KafkaException(error)
        producer.commit_transaction.side_effect = original
        producer.abort_transaction.side_effect = KafkaException(_kafka_error())

        with pytest.raises(KafkaException) as raised:
            pipeline.step()

        assert raised.value.__cause__ is original
        producer.mark_fatal.assert_called_once()

    def test_begin_failure_raises_publish_failed(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.begin_transaction.side_effect = KafkaException(_kafka_error())

        with pytest.raises(PublishFailedError, match="begin"):
            pipeline.step()

        listener.acknowledge.assert_not_called()

    def test_buffer_error_aborts_transaction(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.produce.side_effect = BufferError("queue full")

        with pytest.raises(PublishFailedError):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_produce_failure_aborts_transaction(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        error = _kafka_error(txn_requires_abort=True)
        producer.produce.side_effect = KafkaException(error)

        with pytest.raises(PublishFailedError):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_no_stale_errors_after_commit(self) -> None:
        r1 = _make_record()
        pipeline, producer, listener = _make_pipeline(transactional=True, records=[r1])

        pipeline.step()

        producer.commit_transaction.assert_called_once()


# ── CDC acknowledgement retry ──────────────────────────────────────


class TestAckRetry:
    def test_transient_failure_then_success(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("overloaded"),
            None,
        ]

        count = pipeline.step()

        assert count == 1
        assert listener.acknowledge.call_count == 2

    def test_multiple_transient_failures_then_success(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("1"),
            CDCRetryableError("2"),
            None,
        ]

        count = pipeline.step()

        assert count == 1
        assert listener.acknowledge.call_count == 3

    def test_retry_budget_exhausted(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record()], ack_retries=2
        )
        listener.acknowledge.side_effect = CDCRetryableError("overloaded")

        with pytest.raises(CDCRetryableError):
            pipeline.step()

        assert listener.acknowledge.call_count == 2

    def test_no_republish_during_ack_retry(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("overloaded"),
            None,
        ]

        pipeline.step()

        assert producer.produce.call_count == 1
        producer.flush.assert_called_once()

    def test_no_new_poll_during_ack_retry(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("overloaded"),
            None,
        ]

        pipeline.step()

        listener.poll_records.assert_called_once()

    def test_transactional_ack_retry(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("overloaded"),
            None,
        ]

        count = pipeline.step()

        assert count == 1
        assert listener.acknowledge.call_count == 2
        producer.commit_transaction.assert_called_once()


# ── Partition key ──────────────────────────────────────────────────


class TestPartitionKey:
    def test_stable_key_for_stream(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record(), _make_record()],
            partition_key=b"my-stream",
        )

        pipeline.step()

        for call in producer.produce.call_args_list:
            assert call.kwargs["key"] == b"my-stream"

    def test_none_key_passes_none(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            records=[_make_record()],
            partition_key=None,
        )

        pipeline.step()

        _, kwargs = producer.produce.call_args
        assert kwargs["key"] is None

    def test_version_marker_uses_same_key(self) -> None:
        r1 = _make_record("stream-a")
        r2 = mutations_pb2.FDBMutationRecord(stream_name="stream-a")
        pipeline, producer, listener = _make_pipeline(
            records=[r1, r2],
            partition_key=b"stream-a",
        )

        pipeline.step()

        for call in producer.produce.call_args_list:
            assert call.kwargs["key"] == b"stream-a"

    def test_different_streams_can_have_different_keys(self) -> None:
        r1 = _make_record("s1")
        r2 = _make_record("s2")

        p1, prod1, lis1 = _make_pipeline(records=[r1], partition_key=b"s1")
        p2, prod2, lis2 = _make_pipeline(records=[r2], partition_key=b"s2")

        p1.step()
        p2.step()

        assert prod1.produce.call_args.kwargs["key"] == b"s1"
        assert prod2.produce.call_args.kwargs["key"] == b"s2"


# ── Ordering invariants ───────────────────────────────────────────


class TestOrdering:
    def test_produce_before_flush(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        call_order: list[str] = []
        producer.produce.side_effect = lambda *a, **kw: call_order.append("produce")
        producer.flush.side_effect = lambda *a, **kw: (call_order.append("flush") or 0)

        pipeline.step()

        assert call_order.index("produce") < call_order.index("flush")

    def test_flush_before_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(records=[_make_record()])
        call_order: list[str] = []
        producer.flush.side_effect = lambda *a, **kw: (call_order.append("flush") or 0)
        listener.acknowledge.side_effect = lambda: call_order.append("ack")

        pipeline.step()

        assert call_order.index("flush") < call_order.index("ack")

    def test_produce_before_commit(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        call_order: list[str] = []
        producer.produce.side_effect = lambda *a, **kw: call_order.append("produce")
        producer.commit_transaction.side_effect = lambda *a, **kw: call_order.append(
            "commit"
        )

        pipeline.step()

        assert call_order.index("produce") < call_order.index("commit")

    def test_commit_before_ack(self) -> None:
        pipeline, producer, listener = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        call_order: list[str] = []
        producer.commit_transaction.side_effect = lambda *a, **kw: call_order.append(
            "commit"
        )
        listener.acknowledge.side_effect = lambda: call_order.append("ack")

        pipeline.step()

        assert call_order.index("commit") < call_order.index("ack")
