"""Core bridge pipeline: CDC poll → Kafka produce → FDB acknowledge.

Each :meth:`BridgePipeline.step` processes exactly one CDC reply.  When
the producer is transactional the step wraps every produce batch in a
Kafka transaction so downstream ``read_committed`` consumers observe the
batch atomically.  When non-transactional, records are flushed with
``acks=all`` and idempotent delivery before the CDC position is
acknowledged.

Neither mode provides cross-system exactly-once between FDB and Kafka.
A crash after Kafka commit/flush but before ``acknowledge()`` causes FDB
to replay the batch; downstream consumers may see duplicates.  Both modes
are at-least-once.

On any Kafka failure the pipeline does **not** acknowledge FDB.  The
original exception propagates to the caller, who must close and resume
the listener from its last persisted cursor to replay the batch.
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
            KafkaException: Kafka delivery, flush, or transaction failure.
                The CDC batch was not acknowledged.  The caller must close
                the listener and resume from the last persisted cursor.
                If ``error.fatal()`` is true, the producer must also be
                replaced.
            BufferError: librdkafka's local queue was full during produce.
                The CDC batch was not acknowledged.
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
            KafkaException: Delivery or flush failure.
            BufferError: Producer queue full.
        """
        self._produce_all(records)
        remaining = self._producer.flush()

        if remaining > 0:
            raise KafkaException(
                f"Flush timed out with {remaining} message(s) still in queue"
            )

        self._acknowledge_with_retry()
        return len(records)

    def _step_transactional(
        self,
        records: list[mutations_pb2.FDBMutationRecord],
    ) -> int:
        """Produce inside a Kafka transaction, then acknowledge.

        On any failure the transaction is aborted (if possible) and the
        original exception re-raised without acknowledging FDB.  Fatal
        errors mark the producer and propagate directly.

        Raises:
            KafkaException: Transaction or delivery failure.
            BufferError: Producer queue full (transaction aborted).
        """
        self._producer.begin_transaction()
        try:
            self._produce_all(records)
            self._producer.commit_transaction()
        except (KafkaException, BufferError) as exc:
            self._abort_and_reraise(exc)

        self._acknowledge_with_retry()
        return len(records)

    def _abort_and_reraise(self, exc: KafkaException | BufferError) -> None:
        """Abort the transaction and re-raise the original exception.

        If the error is fatal, mark the producer and re-raise immediately
        without attempting an abort.  If the abort itself fails, mark the
        producer fatal and raise the abort failure chained to the original.

        Raises:
            KafkaException: Always re-raised (original or abort failure).
            BufferError: Re-raised when that was the original error.
        """
        if isinstance(exc, KafkaException):
            error = exc.args[0] if exc.args else None
            if error is not None and error.fatal():
                self._producer.mark_fatal()
                raise

        try:
            self._producer.abort_transaction()
        except KafkaException as abort_exc:
            self._producer.mark_fatal()
            raise abort_exc from exc

        raise exc
