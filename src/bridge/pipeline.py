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
        partition_key: Kafka record key for every produced message, typically
            the UTF-8-encoded stream name.
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

        Returns:
            The number of records published (0 when the reply was empty).

        Raises:
            KafkaException: A transactional commit failed with a fatal or
                non-retriable error after the abort attempt.
            CDCError: The listener raised a non-recoverable CDC error.
        """
        records = self._listener.poll_records()
        if not records:
            return 0

        if self._producer.transactional:
            return self._step_transactional(records)
        return self._step_idempotent(records)

    def _step_idempotent(self, records: list[Any]) -> int:
        """Produce, flush, then acknowledge."""
        for record in records:
            self._producer.produce(self._topic, record, key=self._partition_key)
        self._producer.flush()
        self._listener.acknowledge()
        return len(records)

    def _step_transactional(self, records: list[Any]) -> int:
        """Produce inside a Kafka transaction, then acknowledge.

        On an abortable error the transaction is aborted and the CDC reply
        is **not** acknowledged, so FDB will replay it on the next poll.
        """
        self._producer.begin_transaction()
        try:
            for record in records:
                self._producer.produce(self._topic, record, key=self._partition_key)
            self._producer.commit_transaction()
        except KafkaException as exc:
            self._handle_transaction_error(exc)
            return 0
        self._listener.acknowledge()
        return len(records)

    def _handle_transaction_error(self, exc: KafkaException) -> None:
        """Classify the error and abort if the transaction requires it.

        Raises:
            KafkaException: Re-raised when the error is fatal or when
                the abort itself fails.
        """
        error = exc.args[0] if exc.args else None
        if error is not None and callable(getattr(error, "txn_requires_abort", None)):
            if error.txn_requires_abort():
                logger.warning("Aborting Kafka transaction: %s", error)
                try:
                    self._producer.abort_transaction()
                except KafkaException:
                    logger.exception("Abort after abortable error also failed")
                    raise exc from None
                return
        if error is not None and callable(getattr(error, "retriable", None)):
            if error.retriable():
                logger.warning(
                    "Retriable transaction error (will replay from CDC): %s",
                    error,
                )
                try:
                    self._producer.abort_transaction()
                except KafkaException:
                    logger.exception("Abort after retriable error also failed")
                    raise exc from None
                return
        raise exc
