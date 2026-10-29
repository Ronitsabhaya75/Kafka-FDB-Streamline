"""Core bridge pipeline: CDC poll → Kafka produce → checkpoint → acknowledge.

Each :meth:`BridgePipeline.step` processes exactly one CDC reply. When the
producer is transactional the step wraps the batch in a Kafka transaction so
``read_committed`` consumers observe it atomically. When non-transactional,
records are flushed before the CDC cursor is checkpointed and acknowledged.

Neither mode is cross-system exactly-once. A crash after Kafka delivery but
before the checkpoint is durable makes FDB replay the batch. A crash after
the checkpoint and before ``acknowledge()`` is closed on resume, because the
listener re-acknowledges a restored cursor. Both modes are at-least-once.

Kafka failures do not acknowledge FDB. A failed idempotent step closes the
producer, which flushes and then purges queued messages. Because some messages
may have reached the broker, downstream consumers may see duplicate records
upon replay, but at-least-once is preserved because FDB is not acknowledged.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, NoReturn

from confluent_kafka import KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.errors import CDCRetryableError
from src.cdc.listener import FDBMutationListener
from src.kafka import MutationProducer, is_fatal_kafka_error

logger = logging.getLogger(__name__)

DEFAULT_ACK_RETRIES: int = 3
DEFAULT_ACK_RETRY_DELAY_SECONDS: float = 0.2
DEFAULT_COMMIT_RETRIES: int = 3
DEFAULT_COMMIT_RETRY_DELAY_SECONDS: float = 0.2

Checkpoint = Callable[[Any], None]

_KAFKA_ERROR_FLAGS: tuple[str, ...] = ("fatal", "retriable", "txn_requires_abort")


def _kafka_error(exc: KafkaException) -> Any | None:
    """Return the librdkafka error carried by ``exc``, if it has one."""
    if not exc.args:
        return None
    error = exc.args[0]
    if any(callable(getattr(error, name, None)) for name in _KAFKA_ERROR_FLAGS):
        return error
    return None


def _flag(error: Any, name: str) -> bool:
    """Return whether ``error.name()`` is true."""
    method = getattr(error, name, None)
    return callable(method) and method() is True


def _require_retries(name: str, value: int) -> None:
    """Reject a retry budget that would skip the operation."""
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be an integer >= 1")


def _require_delay(name: str, value: float) -> None:
    """Reject a negative or non-numeric backoff."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a non-negative number")
    if value < 0:
        raise ValueError(f"{name} must be a non-negative number")


class BridgePipeline:
    """Poll one CDC reply, publish it, checkpoint the cursor, then acknowledge.

    The pipeline does not own the listener or producer lifecycle. The caller
    creates, starts, and closes them. ``checkpoint`` runs only after Kafka
    delivery has succeeded. If it raises, FDB is not acknowledged, and the
    next :meth:`step` retries the checkpoint without polling or producing.

    A retriable ``commit_transaction()`` failure is retried. The transaction
    is aborted only when the error is fatal-free and either not retriable or
    ``txn_requires_abort()`` is true. If the retriable budget runs out, the
    transaction stays open and the next :meth:`step` retries the commit.
    Closing the producer in that state aborts a commit that may still be
    finishing on the broker.
    """

    def __init__(
        self,
        listener: FDBMutationListener,
        producer: MutationProducer,
        topic: str,
        checkpoint: Checkpoint,
        *,
        partition_key: bytes | None = None,
        ack_retries: int = DEFAULT_ACK_RETRIES,
        ack_retry_delay_seconds: float = DEFAULT_ACK_RETRY_DELAY_SECONDS,
        commit_retries: int = DEFAULT_COMMIT_RETRIES,
        commit_retry_delay_seconds: float = DEFAULT_COMMIT_RETRY_DELAY_SECONDS,
    ) -> None:
        """Bind a listener and producer to one topic.

        Args:
            listener: A started CDC mutation listener.
            producer: A Kafka mutation producer. Call ``init_transactions``
                before the first step when using a transactional producer.
            topic: Destination Kafka topic.
            checkpoint: Persists the cursor from ``get_position()`` after
                Kafka delivery and before acknowledgement.
            partition_key: Kafka record key for every record in a step. When
                omitted, the listener's ``raw_stream_name`` or ``stream_name``
                is used. Null keys are rejected because librdkafka's default
                partitioner spreads them across partitions.
            ack_retries: Attempts for a transient FDB acknowledgement failure.
            ack_retry_delay_seconds: Pause between acknowledgement attempts.
            commit_retries: Attempts for a retriable transactional commit.
            commit_retry_delay_seconds: Pause between commit attempts.

        Raises:
            ValueError: If the partition key is empty, or a retry count is
                less than 1, or a delay is negative.
            TypeError: If a delay is not a number, or partition_key is not
                bytes or None.
        """
        _require_retries("ack_retries", ack_retries)
        _require_retries("commit_retries", commit_retries)
        _require_delay("ack_retry_delay_seconds", ack_retry_delay_seconds)
        _require_delay("commit_retry_delay_seconds", commit_retry_delay_seconds)

        self._listener = listener
        self._producer = producer
        self._topic = topic
        self._checkpoint = checkpoint
        self._partition_key = _resolve_partition_key(listener, partition_key)
        self._ack_retries = ack_retries
        self._ack_retry_delay_seconds = float(ack_retry_delay_seconds)
        self._commit_retries = commit_retries
        self._commit_retry_delay_seconds = float(commit_retry_delay_seconds)
        self._pending_count = 0
        # Kafka has the batch. The cursor is not yet acknowledged.
        self._delivered_unacked = False
        # Whether the checkpoint callback has run successfully for the current batch.
        self._checkpointed = False
        # commit_transaction() was retriable. Do not abort; retry the commit.
        self._commit_outstanding = False

    @property
    def commit_outstanding(self) -> bool:
        """Whether a Kafka transaction commit is in progress / needs retry."""
        return self._commit_outstanding

    def step(self) -> int:
        """Process one CDC reply, or finish a delivery already in progress.

        Returns:
            The number of records published. Zero when the reply was empty.

        Raises:
            KafkaException: Delivery or transaction failure. FDB was not
                acknowledged. A fatal error means the producer must be
                replaced and the listener closed. A non-fatal transactional
                abort aborts the transaction; close the listener and resume
                from the last checkpoint (the transactional producer is
                reusable). A retriable commit whose budget is exhausted leaves
                the transaction open; call ``step`` again and do not close
                the producer.
            BufferError: The local produce queue was full. The idempotent
                producer has been closed. A transactional batch was aborted
                (close the listener and resume from the last checkpoint).
            TimeoutError: Idempotent flush timed out. The producer has been
                closed. FDB was not acknowledged.
            RuntimeError: The producer is closed or in a fatal error state.
                The producer must be replaced and the listener closed.
            CDCRetryableError: Transient FDB error. If raised during
                ``poll_records()``, nothing was delivered. If raised after
                Kafka delivery and checkpoint, all acknowledgement retries
                were exhausted (call ``step`` again to retry the ack).
            CDCTerminalError: Non-recoverable CDC error (e.g. stream removed).
            CDCConsumerClosedError: The listener was closed.
            CDCError: Underlying FDB CDC error.
            SerializationError: Mutation could not be serialized into a valid
                Protobuf record.
            Exception: ``checkpoint`` failed. FDB was not acknowledged. Call
                ``step`` again to retry the checkpoint without republishing.
        """
        if self._commit_outstanding or self._delivered_unacked:
            return self._finish_delivery()

        records = self._listener.poll_records()
        if not records:
            return 0

        if self._producer.transactional:
            self._begin_and_produce(records)
        else:
            self._produce_and_flush(records)
        self._pending_count = len(records)
        return self._finish_delivery()

    def _produce_all(self, records: list[mutations_pb2.FDBMutationRecord]) -> None:
        """Enqueue every record under the same partition key."""
        for record in records:
            self._producer.produce(self._topic, record, key=self._partition_key)

    def _produce_and_flush(
        self, records: list[mutations_pb2.FDBMutationRecord]
    ) -> None:
        """Publish a non-transactional batch, or close the producer and raise."""
        try:
            self._produce_all(records)
            self._producer.flush_or_raise()
        except (KafkaException, BufferError, TimeoutError, RuntimeError):
            self._producer.close()
            raise
        self._delivered_unacked = True

    def _begin_and_produce(
        self, records: list[mutations_pb2.FDBMutationRecord]
    ) -> None:
        """Open a transaction and enqueue the batch. Commit happens later."""
        self._producer.begin_transaction()
        try:
            self._produce_all(records)
        except (KafkaException, BufferError) as exc:
            self._abort_and_raise(exc)
        self._commit_outstanding = True

    def _finish_delivery(self) -> int:
        """Commit if needed, then checkpoint and acknowledge."""
        if self._commit_outstanding:
            self._commit_with_retry()
            self._commit_outstanding = False
            self._delivered_unacked = True
        self._checkpoint_and_acknowledge()
        self._delivered_unacked = False
        return self._pending_count

    def _commit_with_retry(self) -> None:
        """Retry a retriable commit. Abort only when the transaction is dead."""
        last_exc: KafkaException | None = None
        for attempt in range(1, self._commit_retries + 1):
            try:
                self._producer.commit_transaction()
                return
            except RuntimeError:
                self._commit_outstanding = False
                raise
            except KafkaException as exc:
                if is_fatal_kafka_error(exc):
                    self._commit_outstanding = False
                    self._producer.mark_fatal()
                    raise
                error = _kafka_error(exc)
                if (
                    error is None
                    or _flag(error, "txn_requires_abort")
                    or not _flag(error, "retriable")
                ):
                    self._commit_outstanding = False
                    self._abort_and_raise(exc)
                last_exc = exc
                logger.warning(
                    "Retriable Kafka commit failure (attempt %d/%d): %s",
                    attempt,
                    self._commit_retries,
                    exc,
                )
                if (
                    attempt < self._commit_retries
                    and self._commit_retry_delay_seconds > 0
                ):
                    time.sleep(self._commit_retry_delay_seconds)
        assert last_exc is not None
        raise last_exc

    def _abort_and_raise(self, exc: KafkaException | BufferError) -> NoReturn:
        """Abort the open transaction and raise ``exc``.

        A fatal Kafka error is re-raised without an abort. An abort failure
        is chained to ``exc``; the producer marks itself fatal.
        """
        if is_fatal_kafka_error(exc):
            self._producer.mark_fatal()
            raise exc
        try:
            self._producer.abort_transaction()
        except KafkaException as abort_exc:
            raise abort_exc from exc
        raise exc

    def _checkpoint_and_acknowledge(self) -> None:
        """Persist ``get_position()``, then acknowledge.

        A checkpoint failure skips the acknowledgement.
        """
        if not self._checkpointed:
            cursor = self._listener.get_position()
            self._checkpoint(cursor)
            self._checkpointed = True
        self._acknowledge_with_retry()
        self._checkpointed = False

    def _acknowledge_with_retry(self) -> None:
        """Acknowledge the delivered reply, backing off on ``CDCRetryableError``."""
        last_err: CDCRetryableError | None = None
        for attempt in range(1, self._ack_retries + 1):
            try:
                self._listener.acknowledge()
                return
            except CDCRetryableError as exc:
                last_err = exc
                logger.warning(
                    "Transient FDB ack failure (attempt %d/%d): %s",
                    attempt,
                    self._ack_retries,
                    exc,
                )
                if attempt < self._ack_retries and self._ack_retry_delay_seconds > 0:
                    time.sleep(self._ack_retry_delay_seconds)
        assert last_err is not None
        raise last_err


def _resolve_partition_key(
    listener: FDBMutationListener, partition_key: bytes | None
) -> bytes:
    """Return a non-empty key, deriving it from the listener when omitted.

    Raises:
        TypeError: If partition_key is not bytes or None.
        ValueError: If the derived or passed partition key is empty.
    """
    if partition_key is not None:
        if not isinstance(partition_key, (bytes, bytearray)):
            raise TypeError("partition_key must be bytes or None")
        resolved = bytes(partition_key)
    else:
        raw = getattr(listener, "raw_stream_name", None)
        if isinstance(raw, (bytes, bytearray)) and raw:
            resolved = bytes(raw)
        else:
            name = getattr(listener, "stream_name", None)
            resolved = name.encode("utf-8") if isinstance(name, str) else b""
    if not resolved:
        raise ValueError(
            "partition_key must be non-empty bytes, or the listener must "
            "expose a non-empty raw_stream_name or stream_name"
        )
    return resolved
