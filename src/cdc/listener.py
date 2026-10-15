"""FoundationDB CDC listener with explicit delivery and acknowledgement."""

import time
from typing import Any, Final, Self

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.client import FDBClient, _raise_consume_failure, _to_bytes
from src.cdc.errors import CDCConsumerClosedError, CDCError, CDCInvalidCursorError
from src.observability import (
    audit_version_end,
    heartbeat,
    record_cdc_versions,
    record_mutation_polled,
    record_serializer_bytes,
    record_serializer_error,
)
from src.serialization import serialize_version_end, serialize_version_group
from src.serialization.errors import RecordTooLargeError, SerializationError

DEFAULT_MAX_RECORD_BYTES: Final[int] = 1_000_000
DEFAULT_RESUME_TIMEOUT_SECONDS: Final[float] = 10.0
DEFAULT_RESUME_POLL_INTERVAL_SECONDS: Final[float] = 0.05
DEFAULT_GRV_CACHE_TTL_SECONDS: Final[float] = 2.0


def strinc(key: bytes) -> bytes:
    """Return the next prefix after ``key`` in lexicographical byte order.

    Args:
        key: Input byte string.

    Returns:
        The next prefix byte string.

    Raises:
        ValueError: If key is empty or consists entirely of ``0xFF`` bytes.
    """
    stripped = key.rstrip(b"\xff")
    if not stripped:
        raise ValueError("Key must contain at least one byte not equal to 0xFF")
    return stripped[:-1] + bytes([stripped[-1] + 1])


def subspace_to_key_range(subspace: Any) -> tuple[bytes, bytes]:
    """Compute the strict ``[begin, end)`` range covering a subspace.

    Args:
        subspace: FoundationDB Subspace, DirectorySubspace, or bytes prefix.

    Returns:
        The inclusive begin key and exclusive end key.

    Raises:
        TypeError: If subspace is neither an FDB Subspace nor bytes.
    """
    if hasattr(subspace, "key") and callable(subspace.key):
        prefix = subspace.key()
    elif isinstance(subspace, bytes):
        prefix = subspace
    else:
        raise TypeError(
            f"Expected FDB Subspace or bytes prefix, got {type(subspace).__name__}"
        )
    return prefix, strinc(prefix)


class FDBMutationListener:
    """Consume native CDC replies as bounded Protobuf record envelopes.

    A listener permits only one unacknowledged native reply at a time. Callers must
    durably publish and checkpoint every record returned by :meth:`poll_records`
    before calling :meth:`acknowledge`.
    """

    def __init__(
        self,
        client: FDBClient,
        stream_name: str | bytes,
        *,
        key_range: tuple[bytes, bytes] | None = None,
        subspace: Any | None = None,
        cursor: Any | None = None,
        max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
        resume_timeout_seconds: float = DEFAULT_RESUME_TIMEOUT_SECONDS,
        resume_poll_interval_seconds: float = DEFAULT_RESUME_POLL_INTERVAL_SECONDS,
    ) -> None:
        """Initialize a CDC listener.

        Supplying a key range or subspace registers the stream during ``start``.
        Omitting both consumes an already registered stream.

        Args:
            client: FoundationDB client used for stream operations.
            stream_name: Registered CDC stream name.
            key_range: Optional explicit ``(begin_key, end_key)`` registration range.
            subspace: Optional FDB subspace or bytes prefix to register.
            cursor: Optional persisted cursor from which to resume.
            max_record_bytes: Maximum serialized size of every returned record.
            resume_timeout_seconds: Maximum wait for the cluster read version to
                reach a resumed cursor.
            resume_poll_interval_seconds: Delay between resume read-version checks.

        Raises:
            TypeError: If a numeric configuration value has an invalid type.
            ValueError: If both range forms are supplied or a value is not positive.
        """
        if key_range is not None and subspace is not None:
            raise ValueError("Specify either key_range or subspace, not both")
        if type(max_record_bytes) is not int:
            raise TypeError("max_record_bytes must be an integer")
        if max_record_bytes < 1:
            raise ValueError("max_record_bytes must be positive")
        if not isinstance(resume_timeout_seconds, (int, float)) or isinstance(
            resume_timeout_seconds, bool
        ):
            raise TypeError("resume_timeout_seconds must be numeric")
        if resume_timeout_seconds <= 0:
            raise ValueError("resume_timeout_seconds must be positive")
        if not isinstance(resume_poll_interval_seconds, (int, float)) or isinstance(
            resume_poll_interval_seconds, bool
        ):
            raise TypeError("resume_poll_interval_seconds must be numeric")
        if resume_poll_interval_seconds <= 0:
            raise ValueError("resume_poll_interval_seconds must be positive")

        self.client = client
        self.raw_stream_name = _to_bytes(stream_name)
        self.stream_name = self.raw_stream_name.decode("utf-8", errors="replace")
        self.key_range = (
            subspace_to_key_range(subspace) if subspace is not None else key_range
        )
        self.initial_cursor = cursor
        self.max_record_bytes = max_record_bytes
        self.resume_timeout_seconds = float(resume_timeout_seconds)
        self.resume_poll_interval_seconds = float(resume_poll_interval_seconds)
        self._consumer: Any | None = None
        self._is_closed = False
        self._awaiting_ack = False
        self._delivery_failed = False
        self._grv_cache: int | None = None
        self._grv_cache_monotonic: float = 0.0
        self._pending_audits: list[tuple[int, int, float]] = []

    def start(self) -> None:
        """Register when requested, then create or safely resume the consumer."""
        if self._consumer is not None:
            return
        if self._is_closed:
            raise CDCConsumerClosedError("Cannot start a closed FDBMutationListener.")

        self.client.open()
        if self.key_range is not None:
            begin_key, end_key = self.key_range
            self.client.register_cdc_stream(self.raw_stream_name, begin_key, end_key)

        if self.initial_cursor is None:
            self._consumer = self.client.create_cdc_consumer(self.raw_stream_name)
            return

        consumer = self.client.resume_cdc_consumer(self.initial_cursor)
        self._consumer = consumer
        try:
            self._reconcile_resumed_consumer()
        except BaseException:
            self._consumer = None
            consumer.close()
            raise

    def _reconcile_resumed_consumer(self) -> None:
        """Wait for the cursor version to be readable, then restore its ack."""
        target_version = self.initial_cursor.last_consumed_version
        deadline = time.monotonic() + self.resume_timeout_seconds
        while self.client.get_read_version() < target_version:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CDCInvalidCursorError(
                    "Timed out waiting for the cluster read version to reach "
                    f"resumed cursor version {target_version}"
                )
            time.sleep(min(self.resume_poll_interval_seconds, remaining))
        try:
            self.consumer.acknowledge().wait()
        except Exception as exc:
            _raise_consume_failure(exc, "restore resumed CDC position")

    @property
    def consumer(self) -> Any:
        """Return the active native consumer, starting it on first access."""
        if self._is_closed:
            raise CDCConsumerClosedError("CDC consumer is closed.")
        if self._consumer is None:
            self.start()
        return self._consumer

    def _consume_batch(self) -> Any:
        """Consume one native reply after enforcing delivery state."""
        if self._delivery_failed:
            raise CDCError(
                "The previous CDC reply could not be serialized; close and resume "
                "from the last persisted cursor."
            )
        if self._awaiting_ack:
            raise CDCError(
                "The previous CDC reply must be durably processed and acknowledged "
                "before polling again."
            )
        try:
            return self.consumer.consume().wait()
        except Exception as exc:
            if self._is_closed:
                raise CDCConsumerClosedError("CDC consumer is closed.") from exc
            _raise_consume_failure(exc, "consume CDC reply")

    def poll_records(
        self, bridge_timestamp_ns: int | None = None
    ) -> list[mutations_pb2.FDBMutationRecord]:
        """Poll one native reply and return bounded Protobuf records.

        Each version group produces one or more mutation batches followed by its
        ``VersionEnd``. If the reply watermark advances beyond its last group, an
        additional empty ``VersionEnd`` preserves that progress. Any non-empty
        result requires an explicit acknowledgement before another poll.

        Args:
            bridge_timestamp_ns: Optional record timestamp in Unix nanoseconds.

        Returns:
            Ordered mutation-batch and version-end records for one native reply.

        Raises:
            CDCError: If another reply is unacknowledged, or prior serialization
                failed and the listener must be resumed.
            CDCRetryableError: If the native consume failed with ``server_overloaded``.
            CDCTerminalError: If the native consume failed with a terminal code.
            SerializationError: If a native mutation or record cannot be serialized.
        """
        batch = self._consume_batch()
        # Duration is serialize work only — not the long-poll wait inside consume.
        poll_started = time.perf_counter()
        heartbeat()
        self._delivery_failed = True
        timestamp_ns = (
            bridge_timestamp_ns if bridge_timestamp_ns is not None else time.time_ns()
        )

        watermark = -1
        serialized: list[bytes] = []
        last_group_version = -1
        for version_group in getattr(batch, "mutations", ()):
            for mutation in version_group.mutations:
                record_mutation_polled(int(mutation.type))
            try:
                chunk = serialize_version_group(
                    version_group.mutations,
                    fdb_version=version_group.version,
                    max_record_bytes=self.max_record_bytes,
                    stream_name=self.stream_name,
                    bridge_timestamp_ns=timestamp_ns,
                )
            except SerializationError:
                record_serializer_error()
                raise
            for payload in chunk:
                record_serializer_bytes(len(payload))
            serialized.extend(chunk)
            last_group_version = version_group.version

        watermark = getattr(batch, "last_consumed_version", -1)
        if watermark >= 0 and watermark > last_group_version:
            try:
                watermark_record = serialize_version_end(
                    fdb_version=watermark,
                    total_mutations=0,
                    stream_name=self.stream_name,
                    bridge_timestamp_ns=timestamp_ns,
                )
            except SerializationError:
                record_serializer_error()
                raise
            if len(watermark_record) > self.max_record_bytes:
                record_bytes = len(watermark_record)
                record_serializer_error()
                raise RecordTooLargeError(
                    f"watermark version end of version {watermark} needs a "
                    f"{record_bytes}-byte record; max_record_bytes is "
                    f"{self.max_record_bytes}",
                    index=None,
                    record_bytes=record_bytes,
                    max_record_bytes=self.max_record_bytes,
                )
            record_serializer_bytes(len(watermark_record))
            serialized.append(watermark_record)

        records = [
            mutations_pb2.FDBMutationRecord.FromString(data) for data in serialized
        ]

        if watermark >= 0:
            record_cdc_versions(
                latest=watermark,
                cluster_read_version=self._cluster_read_version(),
            )

        duration = time.perf_counter() - poll_started
        self._pending_audits = [
            (
                record.version_end.fdb_version,
                record.version_end.total_mutations,
                duration,
            )
            for record in records
            if record.WhichOneof("record") == "version_end"
        ]

        self._delivery_failed = False
        self._awaiting_ack = bool(records)
        return records

    def _cluster_read_version(self) -> int | None:
        """Return a cached cluster read version, refreshing every few seconds."""
        now = time.monotonic()
        if (
            self._grv_cache is not None
            and now - self._grv_cache_monotonic < DEFAULT_GRV_CACHE_TTL_SECONDS
        ):
            return self._grv_cache
        try:
            self._grv_cache = int(self.client.get_read_version())
            self._grv_cache_monotonic = now
            return self._grv_cache
        except Exception:
            return None

    def acknowledge(self) -> None:
        """Acknowledge the last fully delivered reply after durable processing.

        Raises:
            CDCError: If no reply awaits acknowledgement or serialization failed.
            CDCRetryableError: If the native ack failed with ``server_overloaded``.
            CDCTerminalError: If the native ack failed with a terminal code.
            CDCConsumerClosedError: If the listener is closed.
        """
        if self._is_closed:
            raise CDCConsumerClosedError("CDC consumer is closed.")
        if self._delivery_failed:
            raise CDCError(
                "Cannot acknowledge a CDC reply that was not completely serialized."
            )
        if not self._awaiting_ack:
            raise CDCError("No delivered CDC reply is awaiting acknowledgement.")
        try:
            self.consumer.acknowledge().wait()
        except Exception as exc:
            if self._is_closed:
                raise CDCConsumerClosedError("CDC consumer is closed.") from exc
            _raise_consume_failure(exc, "acknowledge CDC position")
        self._awaiting_ack = False
        heartbeat()
        for fdb_version, mutation_count, duration_seconds in self._pending_audits:
            audit_version_end(
                fdb_version=fdb_version,
                mutation_count=mutation_count,
                duration_seconds=duration_seconds,
            )
        self._pending_audits = []

    def get_position(self) -> Any:
        """Return the native cursor for the current consumer position."""
        return self.consumer.get_position()

    def close(self) -> None:
        """Close the consumer without acknowledging or removing the stream."""
        self._is_closed = True
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            consumer.close()

    def __enter__(self) -> Self:
        """Start and return this listener."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Close the listener when leaving its context."""
        self.close()
