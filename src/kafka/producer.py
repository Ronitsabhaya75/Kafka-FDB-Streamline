"""Kafka producer wrapper enforcing ordered, idempotent delivery.

The critical durability knobs (``acks``, idempotence, in-flight limit) are
set here and cannot be overridden by callers. This producer does not create
Kafka transactions, so it does not provide end-to-end exactly-once delivery.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from fdbkafka.cdc.v1 import mutations_pb2

logger = logging.getLogger(__name__)

DeliveryCallback = Callable[[KafkaError | None, Message], None]

# Durability / ordering settings that must not be weakened by callers.
_MANDATORY_CONFIG: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    # Safe with idempotence — librdkafka reorders internally.
    "max.in.flight.requests.per.connection": 5,
    "enable.gapless.guarantee": True,
}

_DEFAULT_PERFORMANCE_CONFIG: dict[str, Any] = {
    "linger.ms": 5,
    "compression.type": "lz4",
}


def _default_error_cb(err: KafkaError) -> None:
    """Log asynchronous client and broker errors.

    Per-message delivery failures are surfaced by :meth:`MutationProducer.flush`.

    Args:
        err: Error reported by librdkafka.
    """
    level = logging.ERROR if err.fatal() else logging.WARNING
    logger.log(level, "Kafka producer error: %s", err)


class MutationProducer:
    """Produces Protobuf-serialised ``FDBMutationRecord`` messages to Kafka.

    Constructor merges caller-supplied config under mandatory ordering and
    durability settings so the critical guarantees cannot be accidentally
    weakened. Performance knobs (``linger.ms``, ``compression.type``) can
    be overridden via *extra_config*.

    Args:
        bootstrap_servers: Kafka bootstrap server(s), e.g. ``"kafka:19092"``.
        extra_config: Optional librdkafka configuration overrides. Keys that
            collide with the mandatory durability settings are ignored.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialise the producer with mandatory durability settings."""
        merged: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            **_DEFAULT_PERFORMANCE_CONFIG,
        }
        if extra_config:
            merged.update(extra_config)

        merged.update(_MANDATORY_CONFIG)
        merged.setdefault("error_cb", _default_error_cb)

        self._producer = Producer(merged)
        self._delivery_errors: list[KafkaError] = []

    def produce(
        self,
        topic: str,
        record: mutations_pb2.FDBMutationRecord,
        *,
        key: bytes | None = None,
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        """Enqueue one ``FDBMutationRecord`` for async delivery.

        The record is serialised to Protobuf wire format before being
        handed to librdkafka's internal queue. Call :meth:`flush` to
        block until all queued messages are broker-acknowledged.

        Args:
            topic: Destination Kafka topic.
            record: The CDC mutation record to publish.
            key: Optional partition key (bytes). In production this will
                typically be the UTF-8-encoded ``stream_name`` so all
                mutations for a directory land on the same partition.
            on_delivery: Optional ``(err, msg)`` callback invoked when the
                broker acknowledges (or permanently fails) this message.
        """
        if self._delivery_errors:
            raise KafkaException(self._delivery_errors[0])

        payload = record.SerializeToString()
        kwargs: dict[str, Any] = {
            "value": payload,
            "on_delivery": self._delivery_callback(on_delivery),
        }
        if key is not None:
            kwargs["key"] = key

        self._producer.produce(topic, **kwargs)
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until all queued messages are broker-acknowledged.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            Number of messages still in the queue (0 means all delivered).

        Raises:
            KafkaException: At least one queued message permanently failed.
        """
        remaining = self._producer.flush(timeout)
        if self._delivery_errors:
            raise KafkaException(self._delivery_errors[0])
        return remaining

    def __len__(self) -> int:
        """Return the number of messages waiting in the producer queue."""
        return len(self._producer)

    def _delivery_callback(self, callback: DeliveryCallback | None) -> DeliveryCallback:
        def report(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                self._delivery_errors.append(error)
            if callback is not None:
                callback(error, message)

        return report
