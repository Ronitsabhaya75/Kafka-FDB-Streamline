"""Kafka producer wrapper enforcing ordered, idempotent delivery.

Mandatory durability knobs (``acks``, idempotence, in-flight limit) are set
here and cannot be overridden by callers.  When a ``transactional_id`` is
supplied the producer acquires a transactional context; the bridge pipeline
drives its ``begin`` / ``commit`` / ``abort`` lifecycle.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from fdbkafka.cdc.v1 import mutations_pb2

logger = logging.getLogger(__name__)

DeliveryCallback = Callable[[KafkaError | None, Message], None]

_MANDATORY_CONFIG: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
}

_DEFAULT_PERFORMANCE_CONFIG: dict[str, Any] = {
    "linger.ms": 5,
    "compression.type": "lz4",
}


def _default_error_cb(err: KafkaError) -> None:
    """Log asynchronous client and broker errors.

    Args:
        err: Error reported by librdkafka.
    """
    level = logging.ERROR if err.fatal() else logging.WARNING
    logger.log(level, "Kafka producer error: %s", err)


class MutationProducer:
    """Produce Protobuf-serialised ``FDBMutationRecord`` messages to Kafka.

    Constructor merges caller-supplied config under mandatory ordering and
    durability settings so the critical guarantees cannot be accidentally
    weakened.  Performance knobs (``linger.ms``, ``compression.type``) can
    be overridden via *extra_config*.

    When *transactional_id* is given the underlying librdkafka producer is
    configured for Kafka transactions.  The caller must invoke
    :meth:`init_transactions` once before the first transaction.

    Args:
        bootstrap_servers: Kafka bootstrap server(s), e.g. ``"kafka:19092"``.
        extra_config: Optional librdkafka configuration overrides.  Keys that
            collide with the mandatory durability settings are ignored.
        transactional_id: Optional stable transaction identifier.  Enables
            Kafka transactions and producer fencing.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        extra_config: dict[str, Any] | None = None,
        *,
        transactional_id: str | None = None,
    ) -> None:
        """Initialise the producer with mandatory durability settings.

        Args:
            bootstrap_servers: Kafka bootstrap server(s).
            extra_config: Optional librdkafka overrides.  Durability keys
                are ignored.
            transactional_id: Optional stable transaction identifier.
        """
        merged: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            **_DEFAULT_PERFORMANCE_CONFIG,
        }
        if extra_config:
            merged.update(extra_config)

        if transactional_id is not None:
            merged["transactional.id"] = transactional_id

        merged.update(_MANDATORY_CONFIG)
        merged.setdefault("error_cb", _default_error_cb)

        self._producer = Producer(merged)
        self._delivery_errors: list[KafkaError] = []
        self._transactional = transactional_id is not None

    @property
    def transactional(self) -> bool:
        """Whether this producer was created with a transactional id."""
        return self._transactional

    def init_transactions(self, timeout: float = 30.0) -> None:
        """Initialise the transactional state with the broker coordinator.

        Must be called exactly once before any ``begin_transaction``.

        Args:
            timeout: Maximum seconds to wait for coordinator handshake.

        Raises:
            KafkaException: On coordinator or fencing errors.
            RuntimeError: If the producer is not transactional.
        """
        if not self._transactional:
            raise RuntimeError("init_transactions requires a transactional_id")
        self._producer.init_transactions(timeout)

    def begin_transaction(self) -> None:
        """Begin a new Kafka transaction.

        Raises:
            KafkaException: If the previous transaction was not committed or
                aborted, or the producer is fenced.
            RuntimeError: If the producer is not transactional.
        """
        if not self._transactional:
            raise RuntimeError("begin_transaction requires a transactional_id")
        self._producer.begin_transaction()

    def commit_transaction(self, timeout: float = 30.0) -> None:
        """Commit the current Kafka transaction.

        All records produced since :meth:`begin_transaction` become atomically
        visible to ``read_committed`` consumers.

        Args:
            timeout: Maximum seconds to wait for commit.

        Raises:
            KafkaException: On commit failure (retriable, abortable, or fatal).
            RuntimeError: If the producer is not transactional.
        """
        if not self._transactional:
            raise RuntimeError("commit_transaction requires a transactional_id")
        self._producer.commit_transaction(timeout)

    def abort_transaction(self, timeout: float = 30.0) -> None:
        """Abort the current Kafka transaction.

        Args:
            timeout: Maximum seconds to wait for abort.

        Raises:
            KafkaException: On abort failure.
            RuntimeError: If the producer is not transactional.
        """
        if not self._transactional:
            raise RuntimeError("abort_transaction requires a transactional_id")
        self._producer.abort_transaction(timeout)

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
        handed to librdkafka's internal queue.  Call :meth:`flush` to
        block until all queued messages are broker-acknowledged.

        Args:
            topic: Destination Kafka topic.
            record: The CDC mutation record to publish.
            key: Optional partition key (bytes).  Typically the UTF-8-encoded
                ``stream_name`` so all mutations for a stream land on the same
                partition.
            on_delivery: Optional ``(err, msg)`` callback invoked when the
                broker acknowledges (or permanently fails) this message.
        """
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
            error = self._delivery_errors.pop(0)
            self._delivery_errors.clear()
            raise KafkaException(error)
        return remaining

    def close(self) -> None:
        """Flush remaining messages and release the underlying producer."""
        try:
            self._producer.flush(10.0)
        except Exception:
            logger.exception("Error flushing producer during close")
        self._delivery_errors.clear()

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
