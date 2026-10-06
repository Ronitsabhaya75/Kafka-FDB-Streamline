"""Kafka producer stub until the real broker client lands.

Keeps the ``MutationProducer`` import path for the bridge. Does not touch Kafka
Prometheus metrics or ``/readyz`` — those wait on a producer that can fetch
metadata and observe broker acks.
"""

from __future__ import annotations

from collections.abc import Iterable

from fdbkafka.cdc.v1 import mutations_pb2


class MutationProducer:
    """Publish ``FDBMutationRecord`` envelopes (stub: no broker I/O)."""

    def __init__(self, *, topic: str = "fdb-cdc") -> None:
        """Create a stub producer handle.

        Args:
            topic: Destination topic name retained for the real producer.
        """
        self.topic = topic
        self._closed = False

    def produce(self, records: Iterable[mutations_pb2.FDBMutationRecord]) -> int:
        """Accept records without contacting a broker or updating metrics.

        Args:
            records: Protobuf records from the CDC listener.

        Returns:
            The number of records accepted.

        Raises:
            RuntimeError: If the producer is closed.
        """
        if self._closed:
            raise RuntimeError("MutationProducer is closed")
        return sum(1 for _ in records)

    def close(self) -> None:
        """Close the producer handle."""
        self._closed = True
