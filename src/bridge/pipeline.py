"""Core bridge pipeline: CDC poll → Kafka produce → FDB acknowledge.

Each :meth:`BridgePipeline.step` processes exactly one CDC reply.  When
the producer is transactional the step wraps every produce batch in a
Kafka transaction so downstream ``read_committed`` consumers observe the
batch atomically.  When non-transactional, records are flushed with
``acks=all`` and idempotent delivery before the CDC position is
acknowledged.

Neither mode provides cross-system exactly-once between FDB and Kafka.
A crash after Kafka commit/flush but before ``acknowledge()`` causes FDB
to replay the batch; downstream consumers may see duplicates (idempotent
mode) or see the batch once per transaction epoch (transactional mode,
provided they use ``read_committed``).

On any delivery failure, the pipeline raises :class:`PublishFailedError`
without acknowledging FDB.  The caller must close and resume the listener
from its last persisted cursor to replay the batch.
"""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.errors import CDCRetryableError
from src.cdc.listener import FDBMutationListener
from src.kafka.producer import MutationProducer

logger = logging.getLogger(__name__)

DEFAULT_ACK_RETRIES: int = 3


class PublishFailedError(Exception):
    """Raised when Kafka delivery or flush failed for the current CDC batch.

    The CDC batch was **not** acknowledged.  The caller must close the
    listener and resume from its last persisted cursor to replay the batch.
    """


class BridgePipeline:
    """Poll one CDC reply, publish its records to Kafka, then acknowledge FDB.

    The pipeline does **not** own the listener or producer lifecycle; the
    caller (e.g. a daemon or test harness) creates, starts, and closes them.

    Args:
        listener: A started :class:`FDBMutationListener`.
        producer: A :class:`MutationProducer`, already initialised for
            transactions when transactional mode is desired.
        topic: Destination Kafka topic.
        partition_key: Kafka record key for every produced message.  Should
            be the UTF-8-encoded stream name so all mutations for a stream
            land on the same partition.
        ack_retries: Maximum number of retries for transient FDB
            acknowledgement failures.
    """

    def __init__(
        self,
        listener: FDBMutationListener,
        producer: MutationProducer,
        topic: str,
        *,
        partition_key: bytes | None = None,
        ack_retries: int = DEFAULT_ACK_RETRIES,
    ) -> None:
        """Bind a listener and producer to the given topic.

        Args:
            listener: A started CDC mutation listener.
            producer: A Kafka mutation producer.
            topic: Destination Kafka topic.
            partition_key: Optional Kafka record key for partitioning.
            ack_retries: Maximum retries for transient ack failures.
        """
        self._listener = listener
        self._producer = producer
        self._topic = topic
        self._partition_key = partition_key
        self._ack_retries = ack_retries

    def step(self) -> int:
        """Process one CDC reply end-to-end.

        1. Polls one CDC reply from FDB.
        2. Produces all records to Kafka with the configured partition key.
        3. Confirms delivery (flush in idempotent mode, commit in
           transactional mode).
        4. Acknowledges the CDC batch back to FDB.

        Returns:
            The number of records published (0 when the reply was empty).

        Raises:
            PublishFailedError: Kafka delivery or transaction commit failed.
                The CDC batch was not acknowledged.  The caller must close
                the listener and resume from the last persisted cursor.
            KafkaException: A fatal producer error occurred; the producer
                must be replaced.
            CDCError: The listener raised a non-recoverable CDC error.
            CDCRetryableError: All acknowledgement retries were exhausted
                after successful Kafka delivery.
        """
        records = self._listener.poll_records()
        if not records:
            return 0

        if self._producer.transactional:
            return self._step_transactional(records)
        return self._step_idempotent(records)

    def _produce_all(
        self,
        records: list[mutations_pb2.FDBMutationRecord],
    ) -> None:
        """Produce every record; propagates ``BufferError`` and ``KafkaException``.

        Args:
            records: CDC mutation records to publish.
        """
        for record in records:
            self._producer.produce(self._topic, record, key=self._partition_key)

    def _acknowledge_with_retry(self) -> None:
        """Acknowledge the CDC batch, retrying transient FDB failures.

        Raises:
            CDCRetryableError: Retries exhausted on a transient failure.
            CDCError: A non-retryable acknowledgement error.
        """
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
        raise last_err  # type: ignore[misc]

    def _step_idempotent(
        self,
        records: list[mutations_pb2.FDBMutationRecord],
    ) -> int:
        """Produce, flush, then acknowledge.

        Raises:
            PublishFailedError: If produce raises ``BufferError``, flush raises
                ``KafkaException``, or flush returns a nonzero count.
        """
        try:
            self._produce_all(records)
            remaining = self._producer.flush()
        except (KafkaException, BufferError) as exc:
            raise PublishFailedError(
                "Kafka delivery failed in idempotent mode"
            ) from exc

        if remaining > 0:
            raise PublishFailedError(
                f"Flush timed out with {remaining} message(s) still in queue"
            )

        self._acknowledge_with_retry()
        return len(records)

    def _step_transactional(
        self,
        records: list[mutations_pb2.FDBMutationRecord],
    ) -> int:
        """Produce inside a Kafka transaction, then acknowledge.

        On an abortable or retriable error the transaction is aborted and
        :class:`PublishFailedError` is raised without acknowledging FDB.  The
        caller must close and resume the listener to replay the batch.

        Raises:
            PublishFailedError: The transaction was aborted.  The CDC batch was
                not acknowledged.
            KafkaException: Fatal producer error; the producer must be
                replaced.
        """
        try:
            self._producer.begin_transaction()
        except (KafkaException, RuntimeError) as exc:
            raise PublishFailedError("Failed to begin Kafka transaction") from exc

        try:
            self._produce_all(records)
            self._producer.commit_transaction()
        except KafkaException as exc:
            self._handle_transaction_error(exc)
            return 0
        except BufferError as exc:
            self._abort_or_escalate(exc)
            return 0

        self._acknowledge_with_retry()
        return len(records)

    def _abort_or_escalate(self, original: BaseException) -> None:
        """Attempt to abort the transaction; raise ``PublishFailedError`` or fatal.

        Args:
            original: The exception that caused the abort attempt.

        Raises:
            PublishFailedError: If the abort succeeds.
            KafkaException: If the abort fails with a fatal error.
        """
        try:
            self._producer.abort_transaction()
        except KafkaException as abort_exc:
            self._producer.mark_fatal()
            raise KafkaException(
                abort_exc.args[0] if abort_exc.args else None
            ) from original
        raise PublishFailedError(
            "Transaction aborted after production error"
        ) from original

    def _handle_transaction_error(self, exc: KafkaException) -> None:
        """Classify the error and abort if the transaction requires it.

        Raises:
            PublishFailedError: When the error is abortable or retriable and the
                abort succeeds.
            KafkaException: When the error is fatal, or when both the
                original operation and the abort fail.
        """
        error = exc.args[0] if exc.args else None

        if error is not None and getattr(error, "fatal", lambda: False)():
            self._producer.mark_fatal()
            raise

        needs_abort = (
            error is not None and getattr(error, "txn_requires_abort", lambda: False)()
        )
        is_retriable = (
            error is not None and getattr(error, "retriable", lambda: False)()
        )

        if needs_abort or is_retriable:
            logger.warning(
                "Aborting Kafka transaction (%s): %s",
                "abort-required" if needs_abort else "retriable",
                error,
            )
            try:
                self._producer.abort_transaction()
            except KafkaException as abort_exc:
                self._producer.mark_fatal()
                raise KafkaException(
                    abort_exc.args[0] if abort_exc.args else None
                ) from exc
            raise PublishFailedError(
                "Transaction aborted after commit failure"
            ) from exc

        self._producer.mark_fatal()
        raise
