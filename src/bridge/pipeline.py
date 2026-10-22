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
import threading
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import KafkaException

from src.cdc.client import FDBClient
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


@dataclass
class BridgeMetrics:
    """Runtime metrics for the autonomous bridge loop."""

    mutations_published: int = 0
    records_published: int = 0
    batches_acked: int = 0
    errors_retried: int = 0


class Bridge:
    """Autonomous Bridge daemon managing CDC listener, producer, and run loop."""

    def __init__(self, config: Any) -> None:
        """Initialize the Bridge with declarative configuration.

        Args:
            config: Validated BridgeConfig instance.
        """
        self.config = config
        self._shutdown_requested = threading.Event()
        self.metrics = BridgeMetrics()
        self._client: FDBClient | None = None
        self._listener: FDBMutationListener | None = None
        self._producer: MutationProducer | None = None
        self._pipeline: BridgePipeline | None = None

    @property
    def mutations_published(self) -> int:
        """Total count of individual mutations published to Kafka."""
        return self.metrics.mutations_published

    @property
    def batches_acked(self) -> int:
        """Total count of CDC batches acknowledged back to FoundationDB."""
        return self.metrics.batches_acked

    def start(self) -> None:
        """Initialize FDB client, register CDC stream, and start listener."""
        self._client = FDBClient(
            cluster_file=self.config.fdb.cluster_file,
            api_version=self.config.fdb.api_version,
        )
        self._client.open()

        key_range = (self.config.cdc.begin_key, self.config.cdc.end_key)
        self._listener = FDBMutationListener(
            self._client,
            stream_name=self.config.cdc.stream_name,
            key_range=key_range,
            max_record_bytes=self.config.cdc.max_record_bytes,
        )
        self._listener.start()

        self._producer = MutationProducer(
            self.config.kafka.bootstrap_servers,
        )

        partition_key = self.config.cdc.stream_name.encode("utf-8")
        self._pipeline = BridgePipeline(
            self._listener,
            self._producer,
            topic=self.config.kafka.topic,
            partition_key=partition_key,
        )

    def run(self) -> None:
        """Execute continuous streaming run loop until shutdown is requested."""
        if self._listener is None or self._producer is None:
            raise RuntimeError("Bridge must be started before calling run()")

        backoff = self.config.pipeline.backoff_base_seconds
        partition_key = self.config.cdc.stream_name.encode("utf-8")

        while not self._shutdown_requested.is_set():
            try:
                records = self._listener.poll_records()
                if not records:
                    time.sleep(backoff)
                    backoff = min(
                        backoff * 1.5, self.config.pipeline.backoff_max_seconds
                    )
                    continue

                backoff = self.config.pipeline.backoff_base_seconds
                for record in records:
                    self._producer.produce(
                        self.config.kafka.topic,
                        record,
                        key=partition_key,
                    )
                self._producer.flush(self.config.pipeline.flush_timeout_seconds)
                self._listener.acknowledge()

                batch_mutations = 0
                for record in records:
                    if record.HasField("batch"):
                        batch_mutations += len(record.batch.mutations)
                    elif record.HasField("mutation"):
                        batch_mutations += 1

                self.metrics.mutations_published += batch_mutations
                self.metrics.records_published += len(records)
                self.metrics.batches_acked += 1
            except Exception as exc:
                logger.warning("Error in bridge run loop: %s", exc)
                self.metrics.errors_retried += 1
                if self._shutdown_requested.is_set():
                    break
                time.sleep(backoff)

    def request_shutdown(self) -> None:
        """Signal the daemon run loop to stop cleanly."""
        self._shutdown_requested.set()

    def stop(self) -> None:
        """Request shutdown and release all handles."""
        self.request_shutdown()
        if self._producer is not None:
            self._producer.close()
            self._producer = None
        if self._listener is not None:
            self._listener.close()
            self._listener = None
        if self._client is not None:
            self._client.close()
            self._client = None
