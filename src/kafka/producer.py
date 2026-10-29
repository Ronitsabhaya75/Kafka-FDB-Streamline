"""Kafka producer wrapper enforcing ordered, idempotent delivery.

The critical durability knobs (``acks``, idempotence, in-flight limit,
gapless guarantee, infinite message timeout) are set here and cannot be overridden
by callers.  When a ``transactional_id`` is supplied the producer acquires a
transactional context; the caller drives its ``begin`` / ``commit`` / ``abort``
lifecycle.

Both modes provide at-least-once delivery within a single producer session.
Transactional mode adds Kafka-side atomicity: ``read_committed`` consumers
observe each batch as all-or-nothing.  Neither mode provides end-to-end
exactly-once between FDB and Kafka because the CDC cursor acknowledgement
is outside the Kafka transaction boundary.
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
    # Infinite timeout: prevents local timeouts from creating gaps during outages.
    "message.timeout.ms": 0,
}

_DEFAULT_PERFORMANCE_CONFIG: dict[str, Any] = {
    "linger.ms": 5,
    "compression.type": "lz4",
}

# Keys callers must not sneak in through extra_config.
_FORBIDDEN_EXTRA_KEYS = frozenset(_MANDATORY_CONFIG) | {"transactional.id"}


def _is_fatal_kafka_error(exc: Exception) -> bool:
    """Return True if exc represents a fatal Kafka error."""
    if isinstance(exc, KafkaException) and exc.args:
        err = exc.args[0]
        if hasattr(err, "fatal") and callable(err.fatal):
            res = err.fatal()
            return res is True or (isinstance(res, bool) and res)
    return False


def _default_error_cb(err: KafkaError) -> None:
    """Log asynchronous client and broker errors.

    Per-message delivery failures are not handled here: in nontransactional
    mode they are recorded by the delivery callback and raised by every later
    :meth:`MutationProducer.produce` and :meth:`MutationProducer.flush`.  In
    transactional mode, delivery failures do not block subsequent
    :meth:`MutationProducer.produce` calls within the transaction, but are
    raised by :meth:`MutationProducer.flush` and transaction completion.

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
            collide with the mandatory durability settings or with
            ``transactional.id`` are rejected.
        transactional_id: Optional stable transaction identifier.  Enables
            Kafka transactions and producer fencing.

    Raises:
        ValueError: If *transactional_id* is empty or *extra_config* contains
            a forbidden key (durability settings or ``transactional.id``).
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
            extra_config: Optional librdkafka overrides.  Durability keys and
                ``transactional.id`` are rejected.
            transactional_id: Optional stable transaction identifier.

        Raises:
            ValueError: If *transactional_id* is empty or *extra_config*
                contains a forbidden key.
        """
        if transactional_id is not None and not transactional_id:
            raise ValueError("transactional_id must be a non-empty string or None")

        if extra_config:
            bad = _FORBIDDEN_EXTRA_KEYS & extra_config.keys()
            if bad:
                raise ValueError(
                    f"extra_config must not contain mandatory or transactional "
                    f"keys: {', '.join(sorted(bad))}"
                )

        user_error_cb: Callable[[KafkaError], None] | None = None
        if extra_config and "error_cb" in extra_config:
            user_error_cb = extra_config["error_cb"]

        merged: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            **_DEFAULT_PERFORMANCE_CONFIG,
        }
        if extra_config:
            merged.update(extra_config)

        if transactional_id is not None:
            merged["transactional.id"] = transactional_id

        merged.update(_MANDATORY_CONFIG)
        self._user_error_cb = user_error_cb
        merged["error_cb"] = self._error_cb

        # None once closed; every other method checks _closed before use.
        self._producer: Producer | None = Producer(merged)
        self._delivery_errors: list[KafkaError] = []
        self._transactional: bool = transactional_id is not None
        self._txn_initialized: bool = False
        self._txn_open: bool = False
        self._closed: bool = False
        self._fatal: bool = False

    def _error_cb(self, err: KafkaError) -> None:
        """Handle librdkafka asynchronous client and broker errors.

        If librdkafka reports a fatal error, marks the producer fatal. Chains
        any user-supplied ``error_cb`` callback.

        Args:
            err: Error reported by librdkafka.
        """
        is_fatal = False
        if hasattr(err, "fatal") and callable(err.fatal):
            res = err.fatal()
            is_fatal = res is True or (isinstance(res, bool) and res)

        if is_fatal:
            self._fatal = True
            logger.error("Kafka producer fatal error: %s", err)
        else:
            logger.warning("Kafka producer error: %s", err)

        if self._user_error_cb is not None:
            try:
                self._user_error_cb(err)
            except Exception:
                logger.exception("User error_cb raised an unhandled exception")

    @property
    def transactional(self) -> bool:
        """Whether this producer was created with a transactional id."""
        return self._transactional

    def _require_transactional(self, method: str) -> None:
        """Guard for transaction methods.

        Raises:
            RuntimeError: If the producer is closed, fatal, not transactional,
                or has not been initialized.
        """
        if self._closed:
            raise RuntimeError(f"{method} cannot proceed: producer is closed")
        if self._fatal:
            raise RuntimeError(
                f"{method} cannot proceed: producer is in a fatal error state"
            )
        if not self._transactional:
            raise RuntimeError(f"{method} requires a transactional_id")
        if method != "init_transactions" and not self._txn_initialized:
            raise RuntimeError(
                f"{method} requires init_transactions() to be called first"
            )

    def init_transactions(self, timeout: float = 30.0) -> None:
        """Initialise the transactional state with the broker coordinator.

        Must be called exactly once before any ``begin_transaction``.

        Args:
            timeout: Maximum seconds to wait for coordinator handshake.

        Raises:
            KafkaException: On coordinator or fencing errors.
            RuntimeError: If the producer is closed, fatal, not transactional,
                or init_transactions has already been called.
        """
        self._require_transactional("init_transactions")
        if self._txn_initialized:
            raise RuntimeError("init_transactions() has already been called")
        try:
            self._producer.init_transactions(timeout)
        except KafkaException as exc:
            if _is_fatal_kafka_error(exc):
                self._fatal = True
            raise
        self._txn_initialized = True

    def begin_transaction(self) -> None:
        """Begin a new Kafka transaction.

        Raises:
            KafkaException: If the previous transaction was not committed or
                aborted, or the producer is fenced.
            RuntimeError: If the producer is closed, fatal, not transactional,
                not initialized, or a transaction is already open.
        """
        self._require_transactional("begin_transaction")
        if self._txn_open:
            raise RuntimeError("A transaction is already open")
        try:
            self._producer.begin_transaction()
        except KafkaException as exc:
            if _is_fatal_kafka_error(exc):
                self._fatal = True
            raise
        self._txn_open = True

    def commit_transaction(self, timeout: float = 30.0) -> None:
        """Commit the current Kafka transaction.

        All records produced since :meth:`begin_transaction` become atomically
        visible to ``read_committed`` consumers.  Clears transaction-scoped
        delivery errors on success.

        Args:
            timeout: Maximum seconds to wait for commit.

        Raises:
            KafkaException: On commit failure (retriable, abortable, or fatal).
            RuntimeError: If the producer is closed, fatal, not transactional,
                not initialized, or no transaction is open.
        """
        self._require_transactional("commit_transaction")
        if not self._txn_open:
            raise RuntimeError(
                "commit_transaction() called without an open transaction"
            )
        try:
            self._producer.commit_transaction(timeout)
        except KafkaException as exc:
            if _is_fatal_kafka_error(exc):
                self._fatal = True
            raise
        self._txn_open = False
        self._delivery_errors.clear()

    def abort_transaction(self, timeout: float = 30.0) -> None:
        """Abort the current Kafka transaction.

        Clears transaction-scoped delivery errors on success.  If the abort
        itself fails, the errors are preserved and the producer is marked fatal.

        Args:
            timeout: Maximum seconds to wait for abort.

        Raises:
            KafkaException: On abort failure.
            RuntimeError: If the producer is closed, fatal, not transactional,
                not initialized, or no transaction is open.
        """
        self._require_transactional("abort_transaction")
        if not self._txn_open:
            raise RuntimeError("abort_transaction() called without an open transaction")
        try:
            self._producer.abort_transaction(timeout)
        except KafkaException:
            self._fatal = True
            raise
        except Exception:
            self._fatal = True
            raise
        self._txn_open = False
        self._delivery_errors.clear()

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
        handed to librdkafka's internal queue.  Call :meth:`flush` or
        :meth:`flush_or_raise` to block until all queued messages are
        broker-acknowledged.

        Args:
            topic: Destination Kafka topic.
            record: The CDC mutation record to publish.
            key: Optional partition key (bytes).  Typically the UTF-8-encoded
                ``stream_name`` so all mutations for a stream land on the same
                partition.
            on_delivery: Optional ``(err, msg)`` callback invoked when the
                broker acknowledges (or permanently fails) this message.

        Raises:
            KafkaException: In nontransactional mode, an earlier message permanently
                failed (sticky error); the producer is unusable and must be replaced.
            BufferError: librdkafka's local queue is full.
            RuntimeError: The producer is closed, in a fatal error state, or in
                transactional mode without an open transaction.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")
        if self._fatal:
            raise RuntimeError("Producer is in a fatal error state")
        if self._transactional and not self._txn_open:
            raise RuntimeError(
                "produce() in transactional mode requires an open transaction "
                "(call begin_transaction() first)"
            )
        if not self._transactional and self._delivery_errors:
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

        In nontransactional mode, delivery errors are sticky: the first error
        is preserved and re-raised on every subsequent ``flush()`` and
        ``produce()`` call.  The producer must be replaced after a permanent
        delivery failure.  In transactional mode, ``flush()`` raises if any
        delivery errors occurred within the transaction, but ``produce()`` is
        not blocked until commit or abort.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            Number of messages still in the queue (0 means all delivered).

        Raises:
            KafkaException: At least one queued message permanently failed.
            RuntimeError: The producer is closed or in a fatal error state.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")
        if self._fatal:
            raise RuntimeError("Producer is in a fatal error state")
        remaining = self._producer.flush(timeout)
        if self._delivery_errors:
            raise KafkaException(self._delivery_errors[0])
        return remaining

    def flush_or_raise(self, timeout: float = 10.0) -> None:
        """Block until all queued messages are delivered, raising on timeout.

        Convenience wrapper around :meth:`flush`.  Unlike :meth:`flush`, which
        returns the remaining undelivered message count on timeout, this method
        raises ``TimeoutError`` if any messages remain undelivered after
        *timeout* seconds.

        Args:
            timeout: Maximum seconds to wait.

        Raises:
            KafkaException: At least one queued message permanently failed.
            TimeoutError: Queued messages remain undelivered after *timeout* seconds.
            RuntimeError: The producer is closed or in a fatal error state.
        """
        remaining = self.flush(timeout)
        if remaining > 0:
            raise TimeoutError(
                f"{remaining} message(s) remained undelivered after "
                f"{timeout}s flush timeout"
            )

    def mark_fatal(self) -> None:
        """Mark this producer as fatally failed.

        After this call, all operations except :meth:`close` will raise
        ``RuntimeError``.  The caller should discard this producer and create
        a new one.
        """
        self._fatal = True

    def close(self, timeout: float = 10.0) -> None:
        """Flush remaining messages and release the underlying producer.

        Aborts an open transaction in transactional mode, then flushes.  If
        messages remain undelivered after the flush (or the flush raises), they
        are purged: their ``on_delivery`` callbacks receive
        ``KafkaError._PURGE_QUEUE`` or ``KafkaError._PURGE_INFLIGHT`` and a
        warning is logged.  A purged in-flight message may still have reached
        the broker, so its outcome is unknown and the caller must replay it.

        The underlying librdkafka producer is then dropped and the wrapper
        marked closed.  Logs but does not propagate exceptions.  Idempotent.

        Args:
            timeout: Maximum seconds for the transaction abort and for each
                flush (before and after the purge).
        """
        if self._closed:
            return
        producer = self._producer

        if self._transactional and self._txn_open:
            try:
                producer.abort_transaction(timeout)
                self._txn_open = False
            except Exception:
                logger.debug("Abort during close (may be expected)", exc_info=True)

        flush_failed = False
        undelivered = 0
        try:
            undelivered = producer.flush(timeout)
        except Exception:
            flush_failed = True
            logger.exception("Error flushing producer during close")
            try:
                undelivered = len(producer)
            except Exception:
                undelivered = -1

        if undelivered > 0:
            logger.warning(
                "%d message(s) could not be delivered during close", undelivered
            )
        if undelivered > 0 or flush_failed:
            # Pending delivery callbacks hold references to self that librdkafka
            # owns, so without a purge the wrapper is never collected and, with
            # message.timeout.ms=0, keeps retrying for the life of the process.
            try:
                producer.purge()
                producer.flush(timeout)
            except Exception:
                logger.exception("Error purging producer during close")

        self._producer = None
        self._closed = True

    def __len__(self) -> int:
        """Return the number of messages waiting in the producer queue.

        Raises:
            RuntimeError: If the producer is closed or in a fatal error state.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")
        if self._fatal:
            raise RuntimeError("Producer is in a fatal error state")
        return len(self._producer)

    def _delivery_callback(self, callback: DeliveryCallback | None) -> DeliveryCallback:
        def report(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                self._delivery_errors.append(error)
            if callback is not None:
                try:
                    callback(error, message)
                except Exception:
                    logger.exception(
                        "User on_delivery callback raised an unhandled exception"
                    )

        return report
