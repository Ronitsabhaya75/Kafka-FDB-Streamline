"""Core bridge pipeline: CDC poll → Kafka produce → FDB acknowledge.

Each :meth:`step` processes exactly one CDC reply.  When the producer is
transactional the step wraps every produce in a Kafka transaction so
downstream ``read_committed`` consumers observe the batch atomically.
When non-transactional, records are flushed with ``acks=all`` and
idempotent delivery before the CDC position is acknowledged.

Neither mode provides cross-system exactly-once between FDB and Kafka.
A crash after Kafka commit/flush but before ``acknowledge()`` causes FDB
to replay; downstream consumers may see the batch twice (idempotent mode)
or exactly once per transaction epoch (transactional mode, provided they
use ``read_committed``).
"""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaException

from src.cdc.listener import FDBMutationListener
from src.kafka.producer import MutationProducer

logger = logging.getLogger(__name__)


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
            land on the same partition, preserving commit-version order.
    """

    def __init__(
        self,
        listener: FDBMutationListener,
        producer: MutationProducer,
        topic: str,
        *,
        partition_key: bytes | None = None,
    ) -> None:
        """Bind a listener and producer to the given topic.

        Args:
            listener: A started CDC mutation listener.
            producer: A Kafka mutation producer.
            topic: Destination Kafka topic.
            partition_key: Optional Kafka record key for partitioning.
        """
        self._listener = listener
        self._producer = producer
        self._topic = topic
        self._partition_key = partition_key

    def step(self) -> int:
        """Process one CDC reply end-to-end.

        The pipeline guarantees that ``acknowledge()`` is called only after
        every record in the reply has been durably committed by Kafka.  If
        any delivery or transaction error occurs, the CDC cursor is **not**
        advanced and the exception propagates to the caller, who should retry
        (the next ``poll_records`` replays the same version group).

        Returns:
            The number of records published (0 when the reply was empty).

        Raises:
            KafkaException: Flush timed out, a delivery permanently failed,
                or a transactional commit/abort failed.
            CDCError: The listener raised a non-recoverable CDC error.
        """
        records = self._listener.poll_records()
        if not records:
            return 0

        if self._producer.transactional:
            return self._step_transactional(records)
        return self._step_idempotent(records)

    def _step_idempotent(self, records: list[Any]) -> int:
        """Produce, flush, then acknowledge.

        ``flush()`` raises on delivery error or timeout, so ``acknowledge()``
        is only reached when every record has been durably confirmed.
        """
        for record in records:
            self._producer.produce(self._topic, record, key=self._partition_key)
        self._producer.flush()
        self._listener.acknowledge()
        return len(records)

    def _step_transactional(self, records: list[Any]) -> int:
        """Produce inside a Kafka transaction, then acknowledge.

        On an abortable or retriable error the transaction is aborted and the
        exception re-raised so the caller can apply backoff before retrying.
        The CDC cursor is **not** advanced, so the next ``poll_records``
        replays the same version group.
        """
        self._producer.begin_transaction()
        try:
            for record in records:
                self._producer.produce(self._topic, record, key=self._partition_key)
            self._producer.commit_transaction()
        except KafkaException as exc:
            self._abort_and_reraise(exc)
        self._listener.acknowledge()
        return len(records)

    def _abort_and_reraise(self, exc: KafkaException) -> None:
        """Abort the transaction if required, then always re-raise.

        Args:
            exc: The exception from ``commit_transaction`` or ``produce``.

        Raises:
            KafkaException: Always.  If the abort itself fails, the original
                exception is raised with the abort failure as context.
        """
        error = exc.args[0] if exc.args else None
        needs_abort = False

        if error is not None and callable(getattr(error, "txn_requires_abort", None)):
            if error.txn_requires_abort():
                needs_abort = True
        if not needs_abort and error is not None:
            if callable(getattr(error, "retriable", None)) and error.retriable():
                needs_abort = True

        if needs_abort:
            try:
                self._producer.abort_transaction()
            except KafkaException:
                logger.exception("Abort failed after transaction error")
                raise exc from None

        raise exc
