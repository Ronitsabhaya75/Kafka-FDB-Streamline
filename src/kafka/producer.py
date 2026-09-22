"""Kafka producer wrapper enforcing strict ordering and durable delivery.

Wraps ``confluent_kafka.Producer`` with hardcoded configuration for
exactly-once, ordered publishing of Protobuf-serialised CDC records.
The critical durability knobs (``acks``, idempotence, in-flight limit)
are set here and cannot be overridden by callers.
"""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

from fdbkafka.cdc.v1 import mutations_pb2

logger = logging.getLogger(__name__)

# Durability / ordering settings that must not be weakened by callers.
_MANDATORY_CONFIG: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    # Safe with idempotence — librdkafka reorders internally.
    "max.in.flight.requests.per.connection": 5,
}

_DEFAULT_PERFORMANCE_CONFIG: dict[str, Any] = {
    "linger.ms": 5,
    "compression.type": "lz4",
}


def _default_error_cb(err: KafkaError) -> None:
    """Log fatal broker-level errors; non-fatal ones are debug noise."""
    if err.fatal():
        raise KafkaException(err)
    logger.warning("Kafka producer error (non-fatal): %s", err)


class MutationProducer:
    """Produces Protobuf-serialised ``FDBMutationRecord`` messages to Kafka.

    Constructor merges caller-supplied config under mandatory ordering and
    durability settings so the critical guarantees cannot be accidentally
    weakened.  Performance knobs (``linger.ms``, ``compression.type``) can
    be overridden via *extra_config*.

    Args:
        bootstrap_servers: Kafka bootstrap server(s), e.g. ``"kafka:19092"``.
        extra_config: Optional librdkafka configuration overrides.  Keys that
            collide with the mandatory durability settings are silently
            ignored.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialise the producer with mandatory durability settings.

        Args:
            bootstrap_servers: Kafka bootstrap server(s).
            extra_config: Optional librdkafka overrides.  Durability
                keys are silently ignored.
        """
        merged: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            **_DEFAULT_PERFORMANCE_CONFIG,
        }
        if extra_config:
            merged.update(extra_config)

        # Mandatory settings win — applied last.
        merged.update(_MANDATORY_CONFIG)
        merged.setdefault("error_cb", _default_error_cb)

        self._producer = Producer(merged)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def produce(
        self,
        topic: str,
        record: mutations_pb2.FDBMutationRecord,
        *,
        key: bytes | None = None,
        on_delivery: Any = None,
    ) -> None:
        """Enqueue one ``FDBMutationRecord`` for async delivery.

        The record is serialised to Protobuf wire format before being
        handed to librdkafka's internal queue.  Call :meth:`flush` to
        block until all queued messages are broker-acknowledged.

        Args:
            topic: Destination Kafka topic.
            record: The CDC mutation record to publish.
            key: Optional partition key (bytes).  In production this will
                typically be the UTF-8-encoded ``stream_name`` so all
                mutations for a directory land on the same partition.
            on_delivery: Optional ``(err, msg)`` callback invoked once the
                broker acknowledges (or permanently fails) this message.
        """
        payload = record.SerializeToString()
        kwargs: dict[str, Any] = {"value": payload}
        if key is not None:
            kwargs["key"] = key
        if on_delivery is not None:
            kwargs["on_delivery"] = on_delivery

        self._producer.produce(topic, **kwargs)
        # Trigger delivery-report callbacks without blocking.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until all queued messages are broker-acknowledged.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            Number of messages still in the queue (0 means all delivered).
        """
        return self._producer.flush(timeout)

    def __len__(self) -> int:
        """Return the number of messages waiting in the producer queue."""
        return len(self._producer)
