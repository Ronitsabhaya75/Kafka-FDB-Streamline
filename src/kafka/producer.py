"""Kafka producer stub with instrumented ``produce()`` for observability.

Real broker I/O lands with the Kafka producer card. This module satisfies the
telemetry card by exposing ``MutationProducer.produce()`` with latency and
acknowledgment counters, and by marking the producer handle ready for
``/readyz``.
"""

from __future__ import annotations

import time
from collections.abc import Iterable

from fdbkafka.cdc.v1 import mutations_pb2
from src.observability import health, record_kafka_publish


class MutationProducer:
    """Publish ``FDBMutationRecord`` envelopes (stub: metrics only, no broker)."""

    def __init__(self, *, topic: str = "fdb-cdc", partition: int = 0) -> None:
        """Create a producer handle and mark Kafka ready for readiness probes.

        Args:
            topic: Destination topic name recorded in publish context.
            partition: Destination partition recorded in publish context.
        """
        self.topic = topic
        self.partition = partition
        self._closed = False
        health.set_kafka_ready(True)

    def produce(self, records: Iterable[mutations_pb2.FDBMutationRecord]) -> int:
        """Publish records with ``acks=all`` semantics (stubbed as immediate success).

        Each record increments ``kafka_records_published_total`` and observes
        ``kafka_publish_latency_seconds``. VersionEnd bodies also increment
        ``kafka_version_end_markers_total``.

        Args:
            records: Protobuf records from the CDC listener.

        Returns:
            The number of records published.

        Raises:
            RuntimeError: If the producer is closed.
        """
        if self._closed:
            raise RuntimeError("MutationProducer is closed")
        published = 0
        for record in records:
            started = time.perf_counter()
            # Stub: no broker round-trip. Latency is local handling time.
            is_version_end = record.WhichOneof("record") == "version_end"
            latency = time.perf_counter() - started
            record_kafka_publish(latency_seconds=latency, version_end=is_version_end)
            published += 1
        return published

    def close(self) -> None:
        """Close the producer handle and clear Kafka readiness."""
        self._closed = True
        health.set_kafka_ready(False)
