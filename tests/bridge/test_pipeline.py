"""Unit tests for the CDC → Kafka bridge pipeline."""

from collections.abc import Callable
from unittest.mock import MagicMock, PropertyMock, create_autospec

import pytest
from confluent_kafka import KafkaError, KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.pipeline import BridgePipeline
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
    raw_stream_name: bytes | None = None,
    stream_name: str | None = None,
    ack_retries: int = 3,
    ack_retry_delay_seconds: float = 0.0,
    commit_retries: int = 3,
    commit_retry_delay_seconds: float = 0.0,
    checkpoint: Callable[[object], None] | None = None,
) -> tuple[BridgePipeline, MagicMock, MagicMock, MagicMock, object]:
    listener = create_autospec(FDBMutationListener, instance=True)
    listener.poll_records.return_value = records or []
    cursor = object()
    listener.get_position.return_value = cursor
    if raw_stream_name is not None:
        listener.raw_stream_name = raw_stream_name
    if stream_name is not None:
        listener.stream_name = stream_name

    producer = create_autospec(MutationProducer, instance=True)
    type(producer).transactional = PropertyMock(return_value=transactional)

    if checkpoint is None:
        checkpoint_mock: MagicMock | Callable[[object], None] = MagicMock()
    else:
        checkpoint_mock = checkpoint

    pipeline = BridgePipeline(
        listener,
        producer,
        "cdc-topic",
        checkpoint_mock,
        partition_key=partition_key,
        ack_retries=ack_retries,
        ack_retry_delay_seconds=ack_retry_delay_seconds,
        commit_retries=commit_retries,
        commit_retry_delay_seconds=commit_retry_delay_seconds,
    )
    assert isinstance(checkpoint_mock, MagicMock)
    return pipeline, producer, listener, checkpoint_mock, cursor


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


class TestEmptyPoll:
    def test_returns_zero(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(records=[])

        assert pipeline.step() == 0

        listener.acknowledge.assert_not_called()
        listener.get_position.assert_not_called()
        producer.produce.assert_not_called()
        checkpoint.assert_not_called()


class TestIdempotentPath:
    def test_produces_checkpoints_and_acknowledges(self) -> None:
        r1, r2 = _make_record("s1"), _make_record("s2")
        pipeline, producer, listener, checkpoint, cursor = _make_pipeline(
            records=[r1, r2]
        )

        assert pipeline.step() == 2

        assert producer.produce.call_count == 2
        producer.produce.assert_any_call("cdc-topic", r1, key=b"test-stream")
        producer.produce.assert_any_call("cdc-topic", r2, key=b"test-stream")
        producer.flush_or_raise.assert_called_once()
        listener.get_position.assert_called_once()
        checkpoint.assert_called_once_with(cursor)
        listener.acknowledge.assert_called_once()

    def test_delivery_order(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(
            records=[_make_record()]
        )
        call_order: list[str] = []
        producer.produce.side_effect = lambda *a, **kw: call_order.append("produce")
        producer.flush_or_raise.side_effect = lambda *a, **kw: call_order.append(
            "flush"
        )
        listener.get_position.side_effect = lambda: call_order.append("position")
        checkpoint.side_effect = lambda cursor: call_order.append("checkpoint")
        listener.acknowledge.side_effect = lambda: call_order.append("ack")

        pipeline.step()

        assert call_order == ["produce", "flush", "position", "checkpoint", "ack"]

    def test_flush_timeout_closes_producer_and_skips_ack(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(
            records=[_make_record()]
        )
        producer.flush_or_raise.side_effect = TimeoutError("slow")

        with pytest.raises(TimeoutError, match="slow"):
            pipeline.step()

        producer.close.assert_called_once()
        checkpoint.assert_not_called()
        listener.acknowledge.assert_not_called()

    def test_flush_exception_closes_producer_and_skips_ack(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(
            records=[_make_record()]
        )
        producer.flush_or_raise.side_effect = KafkaException(_kafka_error())

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.close.assert_called_once()
        checkpoint.assert_not_called()
        listener.acknowledge.assert_not_called()

    def test_delivery_error_closes_producer_and_skips_ack(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(
            records=[_make_record()]
        )
        producer.produce.side_effect = KafkaException(_kafka_error())

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.close.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_buffer_error_closes_producer_and_skips_ack(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(
            records=[_make_record()]
        )
        producer.produce.side_effect = BufferError("queue full")

        with pytest.raises(BufferError, match="queue full"):
            pipeline.step()

        producer.close.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_transaction_methods_never_called(self) -> None:
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()]
        )

        pipeline.step()

        producer.begin_transaction.assert_not_called()
        producer.commit_transaction.assert_not_called()
        producer.abort_transaction.assert_not_called()


class TestTransactionalPath:
    def test_wraps_in_transaction_then_checkpoints(self) -> None:
        pipeline, producer, listener, checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        call_order: list[str] = []
        producer.begin_transaction.side_effect = lambda: call_order.append("begin")
        producer.produce.side_effect = lambda *a, **kw: call_order.append("produce")
        producer.commit_transaction.side_effect = lambda *a, **kw: call_order.append(
            "commit"
        )
        listener.get_position.side_effect = lambda: call_order.append("position")
        checkpoint.side_effect = lambda cursor: call_order.append("checkpoint")
        listener.acknowledge.side_effect = lambda: call_order.append("ack")

        assert pipeline.step() == 1
        assert call_order == [
            "begin",
            "produce",
            "commit",
            "position",
            "checkpoint",
            "ack",
        ]
        producer.flush_or_raise.assert_not_called()

    def test_abortable_error_aborts_no_ack(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.commit_transaction.side_effect = KafkaException(
            _kafka_error(txn_requires_abort=True)
        )

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        listener.acknowledge.assert_not_called()

    def test_retriable_commit_is_retried_then_succeeds(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.commit_transaction.side_effect = [
            KafkaException(_kafka_error(retriable=True)),
            None,
        ]

        assert pipeline.step() == 1

        assert producer.commit_transaction.call_count == 2
        producer.abort_transaction.assert_not_called()
        listener.acknowledge.assert_called_once()

    def test_retriable_and_abort_required_aborts(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.commit_transaction.side_effect = KafkaException(
            _kafka_error(retriable=True, txn_requires_abort=True)
        )

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        assert producer.commit_transaction.call_count == 1
        listener.acknowledge.assert_not_called()

    def test_exhausted_retriable_commit_retries_on_next_step(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True,
            records=[_make_record()],
            commit_retries=2,
        )
        producer.commit_transaction.side_effect = [
            KafkaException(_kafka_error(retriable=True)),
            KafkaException(_kafka_error(retriable=True)),
            None,
        ]

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.abort_transaction.assert_not_called()
        listener.acknowledge.assert_not_called()
        listener.poll_records.assert_called_once()
        assert producer.produce.call_count == 1

        assert pipeline.step() == 1

        listener.poll_records.assert_called_once()
        assert producer.produce.call_count == 1
        assert producer.commit_transaction.call_count == 3
        listener.acknowledge.assert_called_once()

    def test_fatal_error_propagates_and_marks_fatal(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.commit_transaction.side_effect = KafkaException(
            _kafka_error(fatal=True)
        )

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.mark_fatal.assert_called_once()
        producer.abort_transaction.assert_not_called()
        listener.acknowledge.assert_not_called()

    def test_fatal_produce_does_not_abort(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.produce.side_effect = KafkaException(_kafka_error(fatal=True))

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.mark_fatal.assert_called_once()
        producer.abort_transaction.assert_not_called()
        producer.commit_transaction.assert_not_called()
        listener.acknowledge.assert_not_called()

    def test_abort_failure_chains_original_exception(self) -> None:
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        original = KafkaException(_kafka_error(txn_requires_abort=True))
        producer.commit_transaction.side_effect = original
        producer.abort_transaction.side_effect = KafkaException(_kafka_error())

        with pytest.raises(KafkaException) as raised:
            pipeline.step()

        assert raised.value.__cause__ is original
        producer.mark_fatal.assert_not_called()

    def test_begin_failure_propagates(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.begin_transaction.side_effect = KafkaException(_kafka_error())

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.abort_transaction.assert_not_called()
        listener.acknowledge.assert_not_called()

    def test_buffer_error_aborts_transaction(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.produce.side_effect = BufferError("queue full")

        with pytest.raises(BufferError):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        producer.close.assert_not_called()
        listener.acknowledge.assert_not_called()

    def test_nonfatal_produce_failure_aborts_transaction(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()]
        )
        producer.produce.side_effect = KafkaException(
            _kafka_error(txn_requires_abort=True)
        )

        with pytest.raises(KafkaException):
            pipeline.step()

        producer.abort_transaction.assert_called_once()
        listener.acknowledge.assert_not_called()


class TestCheckpoint:
    def test_checkpoint_failure_skips_ack_and_retries_without_republish(self) -> None:
        pipeline, producer, listener, checkpoint, cursor = _make_pipeline(
            records=[_make_record()]
        )
        checkpoint.side_effect = [RuntimeError("disk full"), None]

        with pytest.raises(RuntimeError, match="disk full"):
            pipeline.step()

        listener.acknowledge.assert_not_called()
        assert producer.produce.call_count == 1

        assert pipeline.step() == 1

        listener.poll_records.assert_called_once()
        assert producer.produce.call_count == 1
        listener.acknowledge.assert_called_once()
        checkpoint.assert_called_with(cursor)


class TestAckRetry:
    def test_transient_failure_then_success(self) -> None:
        pipeline, _producer, listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [CDCRetryableError("overloaded"), None]

        assert pipeline.step() == 1
        assert listener.acknowledge.call_count == 2

    def test_retry_budget_exhausted_then_step_retries_ack_only(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()], ack_retries=2
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("1"),
            CDCRetryableError("2"),
            None,
        ]

        with pytest.raises(CDCRetryableError):
            pipeline.step()

        assert listener.acknowledge.call_count == 2
        assert producer.produce.call_count == 1

        assert pipeline.step() == 1

        listener.poll_records.assert_called_once()
        assert producer.produce.call_count == 1
        assert listener.acknowledge.call_count == 3

    def test_no_republish_during_ack_retry(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [CDCRetryableError("overloaded"), None]

        pipeline.step()

        assert producer.produce.call_count == 1
        producer.flush_or_raise.assert_called_once()
        listener.poll_records.assert_called_once()

    def test_transactional_ack_retry_does_not_recommit(self) -> None:
        pipeline, producer, listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True, records=[_make_record()], ack_retries=3
        )
        listener.acknowledge.side_effect = [CDCRetryableError("overloaded"), None]

        assert pipeline.step() == 1

        assert listener.acknowledge.call_count == 2
        producer.commit_transaction.assert_called_once()

    def test_ack_retry_backs_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []
        monkeypatch.setattr(
            "src.bridge.pipeline.time.sleep", lambda delay: slept.append(delay)
        )
        pipeline, _producer, listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()],
            ack_retries=3,
            ack_retry_delay_seconds=0.25,
        )
        listener.acknowledge.side_effect = [
            CDCRetryableError("1"),
            CDCRetryableError("2"),
            None,
        ]

        pipeline.step()

        assert slept == [0.25, 0.25]

    def test_commit_retry_backs_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []
        monkeypatch.setattr(
            "src.bridge.pipeline.time.sleep", lambda delay: slept.append(delay)
        )
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            transactional=True,
            records=[_make_record()],
            commit_retries=3,
            commit_retry_delay_seconds=0.4,
        )
        producer.commit_transaction.side_effect = [
            KafkaException(_kafka_error(retriable=True)),
            KafkaException(_kafka_error(retriable=True)),
            None,
        ]

        pipeline.step()

        assert slept == [0.4, 0.4]


class TestPartitionKey:
    def test_stable_key_for_stream(self) -> None:
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record(), _make_record()],
            partition_key=b"my-stream",
        )

        pipeline.step()

        for call in producer.produce.call_args_list:
            assert call.kwargs["key"] == b"my-stream"

    def test_derives_key_from_raw_stream_name(self) -> None:
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()],
            partition_key=None,
            raw_stream_name=b"orders",
        )

        pipeline.step()

        assert producer.produce.call_args.kwargs["key"] == b"orders"

    def test_derives_key_from_stream_name(self) -> None:
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            records=[_make_record()],
            partition_key=None,
            stream_name="orders",
        )

        pipeline.step()

        assert producer.produce.call_args.kwargs["key"] == b"orders"

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="partition_key"):
            _make_pipeline(partition_key=b"")

    def test_missing_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="partition_key"):
            _make_pipeline(partition_key=None)

    def test_version_marker_uses_same_key(self) -> None:
        records = [_make_record("stream-a"), _make_record("stream-a")]
        pipeline, producer, _listener, _checkpoint, _cursor = _make_pipeline(
            records=records,
            partition_key=b"stream-a",
        )

        pipeline.step()

        for call in producer.produce.call_args_list:
            assert call.kwargs["key"] == b"stream-a"


class TestConstruction:
    def test_ack_retries_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="ack_retries"):
            _make_pipeline(ack_retries=0)

    def test_commit_retries_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="commit_retries"):
            _make_pipeline(commit_retries=0)

    def test_negative_delay_rejected(self) -> None:
        with pytest.raises(ValueError, match="ack_retry_delay_seconds"):
            _make_pipeline(ack_retry_delay_seconds=-0.1)
