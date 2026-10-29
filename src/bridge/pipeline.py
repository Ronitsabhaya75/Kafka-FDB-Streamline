"""Core bridge pipeline: CDC poll → Kafka produce → checkpoint → acknowledge.

Each :meth:`BridgePipeline.step` processes exactly one CDC reply. When the
producer is transactional the step wraps the batch in a Kafka transaction so
``read_committed`` consumers observe it atomically. When non-transactional,
records are flushed before the CDC cursor is checkpointed and acknowledged.

Neither mode is cross-system exactly-once. A crash after Kafka delivery but
before the checkpoint is durable makes FDB replay the batch. A crash after
the checkpoint and before ``acknowledge()`` is closed on resume, because the
listener re-acknowledges a restored cursor. Both modes are at-least-once.

Kafka failures do not acknowledge FDB. A failed idempotent step closes the
producer, which flushes and then purges queued messages. Because some messages
may have reached the broker, downstream consumers may see duplicate records
upon replay, but at-least-once is preserved because FDB is not acknowledged.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NoReturn

try:
    import fdb
except ImportError:
    fdb = None  # type: ignore[assignment]

from confluent_kafka import KafkaException

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.config import BridgeConfig
from src.cdc.client import FDBClient
from src.cdc.errors import CDCRetryableError, CDCTerminalError
from src.cdc.listener import FDBMutationListener
from src.kafka import MutationProducer, is_fatal_kafka_error
from src.serialization.errors import SerializationError

logger = logging.getLogger(__name__)

DEFAULT_ACK_RETRIES: int = 3
DEFAULT_ACK_RETRY_DELAY_SECONDS: float = 0.2
DEFAULT_COMMIT_RETRIES: int = 3
DEFAULT_COMMIT_RETRY_DELAY_SECONDS: float = 0.2
DEFAULT_FLUSH_TIMEOUT_SECONDS: float = 10.0

Checkpoint = Callable[[Any], None]

_KAFKA_ERROR_FLAGS: tuple[str, ...] = ("fatal", "retriable", "txn_requires_abort")


def _kafka_error(exc: KafkaException) -> Any | None:
    """Return the librdkafka error carried by ``exc``, if it has one."""
    if not exc.args:
        return None
    error = exc.args[0]
    if any(callable(getattr(error, name, None)) for name in _KAFKA_ERROR_FLAGS):
        return error
    return None


def _flag(error: Any, name: str) -> bool:
    """Return whether ``error.name()`` is true."""
    method = getattr(error, name, None)
    return callable(method) and method() is True


def _require_retries(name: str, value: int) -> None:
    """Reject a retry budget that would skip the operation."""
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be an integer >= 1")


def _require_delay(name: str, value: float) -> None:
    """Reject a negative or non-numeric backoff."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a non-negative number")
    if value < 0:
        raise ValueError(f"{name} must be a non-negative number")


class BridgePipeline:
    """Poll one CDC reply, publish it, checkpoint the cursor, then acknowledge.

    The pipeline does not own the listener or producer lifecycle. The caller
    creates, starts, and closes them. ``checkpoint`` runs only after Kafka
    delivery has succeeded. If it raises, FDB is not acknowledged, and the
    next :meth:`step` retries the checkpoint without polling or producing.

    A retriable ``commit_transaction()`` failure is retried. The transaction
    is aborted only when the error is fatal-free and either not retriable or
    ``txn_requires_abort()`` is true. If the retriable budget runs out, the
    transaction stays open and the next :meth:`step` retries the commit.
    Closing the producer in that state aborts a commit that may still be
    finishing on the broker.
    """

    def __init__(
        self,
        listener: FDBMutationListener,
        producer: MutationProducer,
        topic: str,
        checkpoint: Checkpoint,
        *,
        partition_key: bytes | None = None,
        ack_retries: int = DEFAULT_ACK_RETRIES,
        ack_retry_delay_seconds: float = DEFAULT_ACK_RETRY_DELAY_SECONDS,
        commit_retries: int = DEFAULT_COMMIT_RETRIES,
        commit_retry_delay_seconds: float = DEFAULT_COMMIT_RETRY_DELAY_SECONDS,
        flush_timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS,
    ) -> None:
        """Bind a listener and producer to one topic.

        Args:
            listener: A started CDC mutation listener.
            producer: A Kafka mutation producer. Call ``init_transactions``
                before the first step when using a transactional producer.
            topic: Destination Kafka topic.
            checkpoint: Persists the cursor from ``get_position()`` after
                Kafka delivery and before acknowledgement.
            partition_key: Kafka record key for every record in a step. When
                omitted, the listener's ``raw_stream_name`` or ``stream_name``
                is used. Null keys are rejected because librdkafka's default
                partitioner spreads them across partitions.
            ack_retries: Attempts for a transient FDB acknowledgement failure.
            ack_retry_delay_seconds: Pause between acknowledgement attempts.
            commit_retries: Attempts for a retriable transactional commit.
            commit_retry_delay_seconds: Pause between commit attempts.
            flush_timeout_seconds: Maximum wait for message delivery during flush.

        Raises:
            ValueError: If the partition key is empty, or a retry count is
                less than 1, or a delay/timeout is negative or zero.
            TypeError: If a delay or timeout is not a number, or partition_key
                is not bytes or None.
        """
        _require_retries("ack_retries", ack_retries)
        _require_retries("commit_retries", commit_retries)
        _require_delay("ack_retry_delay_seconds", ack_retry_delay_seconds)
        _require_delay("commit_retry_delay_seconds", commit_retry_delay_seconds)
        _require_delay("flush_timeout_seconds", flush_timeout_seconds)
        if flush_timeout_seconds <= 0:
            raise ValueError("flush_timeout_seconds must be positive")

        self._listener = listener
        self._producer = producer
        self._topic = topic
        self._checkpoint = checkpoint
        self._partition_key = _resolve_partition_key(listener, partition_key)
        self._ack_retries = ack_retries
        self._ack_retry_delay_seconds = float(ack_retry_delay_seconds)
        self._commit_retries = commit_retries
        self._commit_retry_delay_seconds = float(commit_retry_delay_seconds)
        self._flush_timeout_seconds = float(flush_timeout_seconds)
        self._pending_count = 0
        # Kafka has the batch. The cursor is not yet acknowledged.
        self._delivered_unacked = False
        # Whether the checkpoint callback has run successfully for the current batch.
        self._checkpointed = False
        # commit_transaction() was retriable. Do not abort; retry the commit.
        self._commit_outstanding = False

    @property
    def commit_outstanding(self) -> bool:
        """Whether a Kafka transaction commit is in progress / needs retry."""
        return self._commit_outstanding

    def step(self) -> int:
        """Process one CDC reply, or finish a delivery already in progress.

        Returns:
            The number of records published. Zero when the reply was empty.

        Raises:
            KafkaException: Delivery or transaction failure. FDB was not
                acknowledged. A fatal error means the producer must be
                replaced and the listener closed. A non-fatal transactional
                abort aborts the transaction; close the listener and resume
                from the last checkpoint (the transactional producer is
                reusable). A retriable commit whose budget is exhausted leaves
                the transaction open; call ``step`` again and do not close
                the producer.
            BufferError: The local produce queue was full. The idempotent
                producer has been closed. A transactional batch was aborted
                (close the listener and resume from the last checkpoint).
            TimeoutError: Idempotent flush timed out. The producer has been
                closed. FDB was not acknowledged.
            RuntimeError: The producer is closed or in a fatal error state.
                The producer must be replaced and the listener closed.
            CDCRetryableError: Transient FDB error. If raised during
                ``poll_records()``, nothing was delivered. If raised after
                Kafka delivery and checkpoint, all acknowledgement retries
                were exhausted (call ``step`` again to retry the ack).
            CDCTerminalError: Non-recoverable CDC error (e.g. stream removed).
            CDCConsumerClosedError: The listener was closed.
            CDCError: Underlying FDB CDC error.
            SerializationError: Mutation could not be serialized into a valid
                Protobuf record.
            Exception: ``checkpoint`` failed. FDB was not acknowledged. Call
                ``step`` again to retry the checkpoint without republishing.
        """
        if self._commit_outstanding or self._delivered_unacked:
            return self._finish_delivery()

        records = self._listener.poll_records()
        if not records:
            return 0

        if self._producer.transactional:
            self._begin_and_produce(records)
        else:
            self._produce_and_flush(records)
        self._pending_count = len(records)
        return self._finish_delivery()

    def _produce_all(self, records: list[mutations_pb2.FDBMutationRecord]) -> None:
        """Enqueue every record under the same partition key."""
        for record in records:
            self._producer.produce(self._topic, record, key=self._partition_key)

    def _produce_and_flush(
        self, records: list[mutations_pb2.FDBMutationRecord]
    ) -> None:
        """Publish a non-transactional batch, or close the producer and raise."""
        try:
            self._produce_all(records)
            self._producer.flush_or_raise(self._flush_timeout_seconds)
        except (KafkaException, BufferError, TimeoutError, RuntimeError):
            self._producer.close()
            raise
        self._delivered_unacked = True

    def _begin_and_produce(
        self, records: list[mutations_pb2.FDBMutationRecord]
    ) -> None:
        """Open a transaction and enqueue the batch. Commit happens later."""
        self._producer.begin_transaction()
        try:
            self._produce_all(records)
        except (KafkaException, BufferError) as exc:
            self._abort_and_raise(exc)
        self._commit_outstanding = True

    def _finish_delivery(self) -> int:
        """Commit if needed, then checkpoint and acknowledge."""
        if self._commit_outstanding:
            self._commit_with_retry()
            self._commit_outstanding = False
            self._delivered_unacked = True
        self._checkpoint_and_acknowledge()
        self._delivered_unacked = False
        return self._pending_count

    def _commit_with_retry(self) -> None:
        """Retry a retriable commit. Abort only when the transaction is dead."""
        last_exc: KafkaException | None = None
        for attempt in range(1, self._commit_retries + 1):
            try:
                self._producer.commit_transaction()
                return
            except RuntimeError:
                self._commit_outstanding = False
                raise
            except KafkaException as exc:
                if is_fatal_kafka_error(exc):
                    self._commit_outstanding = False
                    self._producer.mark_fatal()
                    raise
                error = _kafka_error(exc)
                if (
                    error is None
                    or _flag(error, "txn_requires_abort")
                    or not _flag(error, "retriable")
                ):
                    self._commit_outstanding = False
                    self._abort_and_raise(exc)
                last_exc = exc
                logger.warning(
                    "Retriable Kafka commit failure (attempt %d/%d): %s",
                    attempt,
                    self._commit_retries,
                    exc,
                )
                if (
                    attempt < self._commit_retries
                    and self._commit_retry_delay_seconds > 0
                ):
                    time.sleep(self._commit_retry_delay_seconds)
        assert last_exc is not None
        raise last_exc

    def _abort_and_raise(self, exc: KafkaException | BufferError) -> NoReturn:
        """Abort the open transaction and raise ``exc``.

        A fatal Kafka error is re-raised without an abort. An abort failure
        is chained to ``exc``; the producer marks itself fatal.
        """
        if is_fatal_kafka_error(exc):
            self._producer.mark_fatal()
            raise exc
        try:
            self._producer.abort_transaction()
        except KafkaException as abort_exc:
            raise abort_exc from exc
        raise exc

    def _checkpoint_and_acknowledge(self) -> None:
        """Persist ``get_position()``, then acknowledge.

        A checkpoint failure skips the acknowledgement.
        """
        if not self._checkpointed:
            cursor = self._listener.get_position()
            self._checkpoint(cursor)
            self._checkpointed = True
        self._acknowledge_with_retry()
        self._checkpointed = False

    def _acknowledge_with_retry(self) -> None:
        """Acknowledge the delivered reply, backing off on ``CDCRetryableError``."""
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
                if attempt < self._ack_retries and self._ack_retry_delay_seconds > 0:
                    time.sleep(self._ack_retry_delay_seconds)
        assert last_err is not None
        raise last_err


def _resolve_partition_key(
    listener: FDBMutationListener, partition_key: bytes | None
) -> bytes:
    """Return a non-empty key, deriving it from the listener when omitted.

    Raises:
        TypeError: If partition_key is not bytes or None.
        ValueError: If the derived or passed partition key is empty.
    """
    if partition_key is not None:
        if not isinstance(partition_key, (bytes, bytearray)):
            raise TypeError("partition_key must be bytes or None")
        resolved = bytes(partition_key)
    else:
        raw = getattr(listener, "raw_stream_name", None)
        if isinstance(raw, (bytes, bytearray)) and raw:
            resolved = bytes(raw)
        else:
            name = getattr(listener, "stream_name", None)
            resolved = name.encode("utf-8") if isinstance(name, str) else b""
    if not resolved:
        raise ValueError(
            "partition_key must be non-empty bytes, or the listener must "
            "expose a non-empty raw_stream_name or stream_name"
        )
    return resolved


@dataclass
class BridgeMetrics:
    """Runtime metrics for the autonomous bridge loop.

    Attributes:
        mutations_published: Total count of individual mutations published to Kafka.
        records_published: Total count of Kafka records published.
        batches_acked: Total count of CDC batches acknowledged back to FDB.
        errors_retried: Total count of transient errors caught and retried in the loop.
    """

    mutations_published: int = 0
    records_published: int = 0
    batches_acked: int = 0
    errors_retried: int = 0


@dataclass(frozen=True, slots=True)
class _PersistedCursor:
    """Minimal cursor structure exposing stream_id and last_consumed_version."""

    stream_id: int
    last_consumed_version: int


_DIR_FSYNC_UNSUPPORTED = frozenset(
    {errno.EINVAL, errno.ENOTSUP, getattr(errno, "EOPNOTSUPP", errno.ENOTSUP)}
)


def _save_cursor_to_file(
    path: str | None, position: Any, *, stream_name: str | None = None
) -> None:
    """Persist current consumer position coordinates to a file durably and atomically.

    Flushes and fsyncs the temporary file before atomic rename, and fsyncs the
    parent directory after rename to guarantee that the cursor is durable on disk
    before the caller acknowledges the batch in FoundationDB.

    Args:
        path: Destination file path, or None/empty to skip persistence.
        position: Native CDC cursor object exposing coordinates.
        stream_name: CDC stream name to record alongside the coordinates, so a
            later load can reject a file written for a different stream.

    Raises:
        ValueError: If position is None or lacks stream_id or
            last_consumed_version.
        OSError: If writing, fsyncing, or replacing the cursor file fails,
            including a parent-directory fsync failure other than the
            filesystem not supporting it.
    """
    if not path:
        return
    if position is None:
        raise ValueError("Cannot checkpoint cursor: position is None")

    stream_id = getattr(position, "stream_id", None)
    version = getattr(position, "last_consumed_version", None)
    if isinstance(position, dict):
        stream_id = position.get("stream_id", stream_id)
        version = position.get("last_consumed_version", version)

    if stream_id is None or version is None:
        raise ValueError(
            f"Cannot checkpoint cursor: position {position!r} missing "
            "stream_id or last_consumed_version"
        )
    if (
        not isinstance(stream_id, int)
        or isinstance(stream_id, bool)
        or not isinstance(version, int)
        or isinstance(version, bool)
    ):
        raise ValueError("stream_id and last_consumed_version must be integers")

    payload: dict[str, Any] = {"stream_id": stream_id, "last_consumed_version": version}
    if stream_name is not None:
        payload["stream_name"] = stream_name

    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)

    # Until the parent dir is fsynced the rename may not survive a crash, so any
    # failure here must block the FDB ack. Only filesystems that cannot fsync a
    # directory at all are tolerated.
    dir_path = os.path.dirname(os.path.abspath(path)) or "."
    try:
        dir_flags = os.O_DIRECTORY if hasattr(os, "O_DIRECTORY") else os.O_RDONLY
        dir_fd = os.open(dir_path, dir_flags)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError as e:
        if e.errno not in _DIR_FSYNC_UNSUPPORTED:
            raise


def _load_cursor_from_file(
    path: str | None, *, stream_name: str | None = None
) -> Any | None:
    """Load persisted cursor coordinates from a file if present.

    Args:
        path: Path to the cursor file, or None.
        stream_name: Expected CDC stream name. When given, the file must record
            the same ``stream_name``; when None, the check is skipped.

    Returns:
        A reconstructed cursor instance, or None if path is None or file does not exist.

    Raises:
        ValueError: If the file exists but contains invalid cursor coordinates,
            or ``stream_name`` is given and the file does not record that stream.
        OSError: If reading the cursor file fails.
    """
    if not path or not os.path.exists(path):
        return None

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(
            f"Corrupt cursor file at {path}: expected JSON object, "
            f"got {type(data).__name__}"
        )

    if "stream_id" not in data or "last_consumed_version" not in data:
        raise ValueError(
            f"Corrupt cursor file at {path}: missing stream_id or last_consumed_version"
        )

    raw_stream_id = data["stream_id"]
    raw_version = data["last_consumed_version"]
    if (
        not isinstance(raw_stream_id, int)
        or isinstance(raw_stream_id, bool)
        or not isinstance(raw_version, int)
        or isinstance(raw_version, bool)
    ):
        raise ValueError(f"Corrupt cursor file at {path}: coordinates must be integers")

    # stream_id is FDB's numeric id and cannot be mapped back to a name, so the
    # name written at save time is the only way to detect a foreign file.
    if stream_name is not None and data.get("stream_name") != stream_name:
        raise ValueError(
            f"Cursor file at {path} belongs to stream "
            f"{data.get('stream_name')!r}, expected {stream_name!r}"
        )

    if fdb is not None and hasattr(fdb, "CdcCursor"):
        return fdb.CdcCursor(raw_stream_id, raw_version)
    return _PersistedCursor(stream_id=raw_stream_id, last_consumed_version=raw_version)


class _RecordCountingListener:
    """Wrapper around FDBMutationListener to capture published records and counts."""

    def __init__(self, listener: FDBMutationListener) -> None:
        """Bind the underlying CDC listener.

        Args:
            listener: Underlying FDBMutationListener instance.
        """
        self._listener = listener
        self._last_records: list[mutations_pb2.FDBMutationRecord] = []

    def poll_records(
        self, bridge_timestamp_ns: int | None = None
    ) -> list[mutations_pb2.FDBMutationRecord]:
        """Poll the underlying listener and buffer records for metric tallying.

        Args:
            bridge_timestamp_ns: Optional timestamp for record headers.

        Returns:
            List of polled Protobuf mutation records.
        """
        records = self._listener.poll_records(bridge_timestamp_ns=bridge_timestamp_ns)
        self._last_records = records
        return records

    def pop_last_records(self) -> list[mutations_pb2.FDBMutationRecord]:
        """Return and clear the buffered records from the latest poll.

        Returns:
            List of Protobuf mutation records from the last successful poll.
        """
        records = self._last_records
        self._last_records = []
        return records

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying listener."""
        return getattr(self._listener, name)


class Bridge:
    """Autonomous Bridge daemon managing CDC listener, producer, and run loop.

    Orchestrates FoundationDB CDC polling, Protobuf serialization, durable Kafka
    publishing, and watermark advancement via :class:`BridgePipeline`.

    Args:
        config: Declarative configuration for FDB, CDC stream, Kafka, and pipeline.
        client: Optional pre-configured :class:`FDBClient` (useful for testing).
        listener: Optional pre-configured :class:`FDBMutationListener`.
        producer: Optional pre-configured :class:`MutationProducer`.
        producer_factory: Optional builder for replacement producers.
    """

    def __init__(
        self,
        config: BridgeConfig,
        *,
        client: FDBClient | None = None,
        listener: FDBMutationListener | None = None,
        producer: MutationProducer | None = None,
        producer_factory: Callable[[], MutationProducer] | None = None,
    ) -> None:
        """Initialize the Bridge with declarative configuration.

        Injected components are used by :meth:`start` and closed by
        :meth:`stop`. A rebuild replaces the listener from ``config`` and the
        producer from ``producer_factory`` (default: ``config.kafka``).

        Args:
            config: Validated BridgeConfig instance.
            client: Optional injected FDBClient.
            listener: Optional injected FDBMutationListener.
            producer: Optional injected MutationProducer. A transactional one
                must not have had ``init_transactions()`` called; ``start``
                calls it.
            producer_factory: Builds each replacement producer after a
                rebuild. Defaults to ``MutationProducer`` from
                ``config.kafka``, transactional when ``transactional_id`` is
                set.

        Raises:
            ValueError: If ``producer`` is transactional but neither
                ``producer_factory`` nor ``config.kafka.transactional_id`` is
                given, so a rebuild would downgrade it to idempotent.
        """
        if (
            producer is not None
            and producer.transactional
            and producer_factory is None
            and config.kafka.transactional_id is None
        ):
            raise ValueError(
                "An injected transactional producer needs producer_factory or "
                "kafka.transactional_id so rebuilds stay transactional"
            )
        self.config = config
        self._shutdown_requested = threading.Event()
        self._step_lock = threading.Lock()
        self.metrics = BridgeMetrics()
        self._client: FDBClient | None = client
        self._listener: FDBMutationListener | None = listener
        self._producer: MutationProducer | None = producer
        self._producer_factory: Callable[[], MutationProducer] = (
            producer_factory or self._producer_from_config
        )
        self._pipeline: BridgePipeline | None = None
        self._counting_listener: _RecordCountingListener | None = None

    @property
    def mutations_published(self) -> int:
        """Total count of individual mutations published to Kafka."""
        return self.metrics.mutations_published

    @property
    def batches_acked(self) -> int:
        """Total count of CDC batches acknowledged back to FoundationDB."""
        return self.metrics.batches_acked

    def start(self) -> None:
        """Initialize FDB client, register CDC stream, and start listener.

        Raises:
            ValueError: If stream_name is empty or cursor file is corrupt or
                belongs to another stream.
        """
        if self._pipeline is not None:
            return

        if not self.config.cdc.stream_name:
            raise ValueError("Bridge requires a non-empty cdc.stream_name")

        self._build_pipeline()

    def _producer_from_config(self) -> MutationProducer:
        """Build a producer from ``config.kafka``."""
        return MutationProducer(
            self.config.kafka.bootstrap_servers,
            transactional_id=self.config.kafka.transactional_id,
        )

    def _build_pipeline(self) -> None:
        """Create any missing client, listener and producer, then the pipeline.

        On failure the listener and producer are closed and unset (the client
        is kept), so calling this again starts from a clean slate.

        Raises:
            Exception: Whatever opening, starting or initialising a component
                raised.
        """
        try:
            if self._client is None:
                client = FDBClient(
                    cluster_file=self.config.fdb.cluster_file,
                    api_version=self.config.fdb.api_version,
                )
                client.open()
                self._client = client

            if self._listener is None:
                key_range = (
                    (self.config.cdc.begin_key, self.config.cdc.end_key)
                    if self.config.cdc.begin_key or self.config.cdc.end_key
                    else None
                )
                cursor = _load_cursor_from_file(
                    self.config.cdc.cursor_file,
                    stream_name=self.config.cdc.stream_name,
                )
                self._listener = FDBMutationListener(
                    self._client,
                    stream_name=self.config.cdc.stream_name,
                    key_range=key_range,
                    cursor=cursor,
                    max_record_bytes=self.config.cdc.max_record_bytes,
                )
                self._listener.start()

            if self._producer is None:
                self._producer = self._producer_factory()
            # Every producer reaching here is new or injected, so uninitialised.
            if self._producer.transactional:
                self._producer.init_transactions()
        except Exception:
            self._discard_listener_and_producer()
            raise

        self._counting_listener = _RecordCountingListener(self._listener)
        stream_bytes = (
            self.config.cdc.stream_name.encode("utf-8")
            if isinstance(self.config.cdc.stream_name, str)
            else self.config.cdc.stream_name
        )
        partition_key = stream_bytes if stream_bytes else None

        def _checkpoint_callback(position: Any) -> None:
            _save_cursor_to_file(
                self.config.cdc.cursor_file,
                position,
                stream_name=self.config.cdc.stream_name,
            )

        self._pipeline = BridgePipeline(
            self._counting_listener,  # type: ignore[arg-type]
            self._producer,
            topic=self.config.kafka.topic,
            checkpoint=_checkpoint_callback,
            partition_key=partition_key,
            flush_timeout_seconds=self.config.pipeline.flush_timeout_seconds,
        )

    def _discard_listener_and_producer(self) -> None:
        """Drop the pipeline, then close and unset the listener and producer.

        Close errors are logged, not raised.
        """
        self._pipeline = None
        self._counting_listener = None
        if self._listener is not None:
            try:
                self._listener.close()
            except Exception:
                logger.exception("Error closing listener")
            self._listener = None
        if self._producer is not None:
            try:
                self._producer.close(
                    timeout=self.config.pipeline.shutdown_timeout_seconds
                )
            except Exception:
                logger.exception("Error closing producer")
            self._producer = None

    def _rebuild_pipeline(self) -> None:
        """Rebuild listener and producer from the last durable checkpoint.

        A no-op while a transactional commit is outstanding: closing the
        producer would abort a commit that may still complete, so the
        existing pipeline is kept and the next ``step()`` retries the commit.
        Otherwise either a new pipeline is set or this raises with the
        listener, producer and pipeline unset; calling it again is safe.

        Raises:
            Exception: Whatever rebuilding a component raised.
        """
        if self._pipeline is not None and self._pipeline.commit_outstanding:
            logger.warning("Kafka commit outstanding; retrying it before rebuild")
            return
        logger.info("Rebuilding bridge pipeline from checkpoint...")
        self._discard_listener_and_producer()
        self._build_pipeline()
        logger.info("Bridge pipeline successfully rebuilt from checkpoint.")

    def run(self) -> None:
        """Execute continuous streaming run loop until shutdown is requested.

        Other errors are retried with backoff. When one leaves the producer
        closed or fatal, or the listener unable to poll, the pipeline is rebuilt
        from the last checkpoint; a failed rebuild is itself retried.

        Raises:
            RuntimeError: If called before :meth:`start`.
            CDCTerminalError: If FDB CDC encounters an unrecoverable stream error.
            KafkaException: If Kafka encounters a fatal unrecoverable error.
            ValueError: If a checkpoint or cursor file is permanently invalid
                (``SerializationError`` excepted, which triggers a rebuild).
        """
        if (
            self._listener is None
            or self._producer is None
            or self._pipeline is None
            or self._counting_listener is None
        ):
            raise RuntimeError("Bridge must be started before calling run()")

        backoff = self.config.pipeline.backoff_base_seconds
        rebuild_pending = False

        while not self._shutdown_requested.is_set():
            try:
                with self._step_lock:
                    if self._shutdown_requested.is_set():
                        break
                    # _pipeline is None only after a failed rebuild; retry it.
                    if rebuild_pending or self._pipeline is None:
                        self._rebuild_pipeline()
                        rebuild_pending = False
                    published = self._pipeline.step()
                    if published == 0:
                        records = []
                    else:
                        records = self._counting_listener.pop_last_records()

                if published == 0:
                    self._shutdown_requested.wait(timeout=backoff)
                    backoff = min(
                        backoff * 1.5, self.config.pipeline.backoff_max_seconds
                    )
                    continue

                backoff = self.config.pipeline.backoff_base_seconds
                batch_mutations = 0
                for record in records:
                    if record.HasField("batch"):
                        batch_mutations += len(record.batch.mutations)
                    elif record.HasField("mutation"):
                        batch_mutations += 1

                self.metrics.mutations_published += batch_mutations
                self.metrics.records_published += len(records) if records else published
                self.metrics.batches_acked += 1

            except CDCTerminalError as exc:
                logger.error(
                    "Terminal CDC error in bridge run loop: %s", exc, exc_info=True
                )
                self.request_shutdown()
                raise
            except Exception as exc:
                if is_fatal_kafka_error(exc):
                    logger.error(
                        "Fatal Kafka error in bridge run loop: %s", exc, exc_info=True
                    )
                    self.request_shutdown()
                    raise
                # Checkpoint coordinates and cursor files fail the same way on
                # every retry. SerializationError is per-reply and resumable.
                if isinstance(exc, ValueError) and not isinstance(
                    exc, SerializationError
                ):
                    logger.error(
                        "Permanent error in bridge run loop: %s", exc, exc_info=True
                    )
                    self.request_shutdown()
                    raise

                self.metrics.errors_retried += 1
                logger.warning(
                    "Recoverable error in bridge loop: %s", exc, exc_info=True
                )
                with self._step_lock:
                    rebuild_pending = rebuild_pending or self._needs_rebuild(exc)

                self._shutdown_requested.wait(timeout=backoff)
                backoff = min(backoff * 1.5, self.config.pipeline.backoff_max_seconds)

    def _needs_rebuild(self, exc: Exception) -> bool:
        """Whether ``exc`` left state that only a resume from checkpoint clears.

        Requires ``_step_lock``.
        """
        producer, listener, pipeline = self._producer, self._listener, self._pipeline
        if producer is None or listener is None or pipeline is None:
            return True
        if producer.closed or producer.fatal or listener.delivery_failed:
            return True
        # A non-fatal txn abort leaves the reply unacknowledged with nothing to
        # commit, so every later poll fails. Checkpoint and ack failures also
        # leave awaiting_ack set, but step() retries those in place.
        return (
            listener.awaiting_ack
            and not pipeline.commit_outstanding
            and isinstance(exc, (KafkaException, BufferError))
        )

    def request_shutdown(self) -> None:
        """Signal the daemon run loop to stop cleanly."""
        self._shutdown_requested.set()

    def stop(self) -> None:
        """Request shutdown, finish any outstanding commit, and close all handles.

        Waits up to ``shutdown_timeout_seconds`` for an in-flight step. If it
        is still running (typically parked in ``consume()`` on an idle
        stream), the listener is closed to unblock it and the wait repeats
        once; after that, handles are closed regardless. Every close is
        attempted.

        Raises:
            Exception: The first error raised by a close, after all closes
                were attempted.
        """
        self.request_shutdown()
        timeout = self.config.pipeline.shutdown_timeout_seconds
        locked = self._step_lock.acquire(timeout=timeout)
        if not locked:
            # A blocked consume().wait() raises once its consumer is closed.
            if self._listener is not None:
                try:
                    self._listener.close()
                except Exception:
                    logger.exception("Error closing listener to unblock step")
            locked = self._step_lock.acquire(timeout=timeout)
            if not locked:
                logger.warning("Bridge step still running; closing handles anyway")
        try:
            # Without the lock the run thread may still be inside step().
            if locked:
                self._finish_outstanding_commit(timeout)
            producer, listener, client = self._producer, self._listener, self._client
            self._producer = self._listener = self._client = None
            self._pipeline = None
            self._counting_listener = None

            closes: list[Callable[[], None]] = []
            if producer is not None:
                closes.append(lambda: producer.close(timeout=timeout))
            if listener is not None:
                closes.append(listener.close)
            if client is not None:
                closes.append(client.close)
            first_error: Exception | None = None
            for close in closes:
                try:
                    close()
                except Exception as exc:
                    logger.exception("Error closing bridge handle")
                    first_error = first_error or exc
            if first_error is not None:
                raise first_error
        finally:
            if locked:
                self._step_lock.release()

    def _finish_outstanding_commit(self, timeout: float) -> None:
        """Retry an outstanding transactional commit before the producer closes.

        Closing mid-commit aborts a transaction that may still commit. FDB is
        not acknowledged, so nothing is lost, but the batch replays as
        duplicates. Each ``step()`` already spends the full commit retry
        budget, so the first failure ends the attempt.

        Args:
            timeout: Maximum seconds to keep retrying.
        """
        pipeline = self._pipeline
        if pipeline is None:
            return
        deadline = time.monotonic() + timeout
        while pipeline.commit_outstanding and time.monotonic() < deadline:
            try:
                pipeline.step()
            except Exception:
                logger.exception("Outstanding Kafka commit failed during stop")
                break
        if pipeline.commit_outstanding:
            logger.error("Closing producer mid-commit; the batch will replay")


__all__ = [
    "Bridge",
    "BridgeMetrics",
    "BridgePipeline",
    "Checkpoint",
]
