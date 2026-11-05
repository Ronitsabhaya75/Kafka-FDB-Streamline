"""Durable, identity-checked CDC cursors stored outside captured key ranges."""

import math
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Any, TypeVar

from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.listener import strinc

T = TypeVar("T")
KeyRange = tuple[bytes, bytes]


class CheckpointError(Exception):
    """Base class for fatal checkpoint failures."""


class CheckpointWriteError(CheckpointError):
    """A checkpoint operation exhausted its transaction retry budget."""


class CheckpointRegressionError(CheckpointError):
    """A cursor would move durable progress backwards."""


class CheckpointStreamMismatchError(CheckpointError):
    """A cursor belongs to a different registration of the stream."""


class CheckpointSinkMismatchError(CheckpointError):
    """The cursor belongs to another Kafka cluster or topic."""


class CheckpointFormatError(CheckpointError):
    """Stored cursor data is corrupt or uses an unsupported format."""


class CheckpointOverlapError(CheckpointError):
    """Checkpoint data or directory metadata overlaps a captured range."""


@dataclass(frozen=True)
class SinkIdentity:
    """Kafka destination associated with durable CDC progress.

    Attributes:
        kafka_cluster_id: Broker-reported immutable cluster identity.
        topic: Topic receiving the stream's records.
    """

    kafka_cluster_id: str
    topic: str

    def __post_init__(self) -> None:
        """Reject absent or malformed destination identities."""
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (self.kafka_cluster_id, self.topic)
        ):
            raise ValueError("Sink cluster ID and topic must be non-empty strings")


@dataclass(frozen=True)
class CheckpointCursor:
    """Portable native cursor coordinates, without process-local state.

    Attributes:
        stream_id: Identity assigned to this stream registration.
        last_consumed_version: Inclusive consumed watermark.
    """

    stream_id: int
    last_consumed_version: int

    def __post_init__(self) -> None:
        """Validate native cursor coordinates."""
        if type(self.stream_id) is not int or not 0 <= self.stream_id < 2**64:
            raise CheckpointFormatError("stream_id must be uint64")
        if (
            type(self.last_consumed_version) is not int
            or not -1 <= self.last_consumed_version < 2**63
        ):
            raise CheckpointFormatError("last_consumed_version must be -1 or int64")


def validate_progress(stored: CheckpointCursor | None, new: CheckpointCursor) -> bool:
    """Validate cursor identity and monotonicity; return whether a write is needed.

    Args:
        stored: Previously persisted cursor, if any.
        new: Proposed durable cursor.

    Returns:
        False for an equal-version save; True for new progress.

    Raises:
        CheckpointStreamMismatchError: If stream identities differ.
        CheckpointRegressionError: If the proposed version is older.
    """
    if stored is None:
        return True
    if stored.stream_id != new.stream_id:
        raise CheckpointStreamMismatchError(f"Stored {stored}; attempted {new}")
    if stored.last_consumed_version > new.last_consumed_version:
        raise CheckpointRegressionError(f"Stored {stored}; attempted {new}")
    return stored != new


def check_overlap(key_range: KeyRange, captured: list[KeyRange]) -> None:
    """Reject any intersection between a protected range and captured ranges.

    Args:
        key_range: Protected half-open key range.
        captured: Stream's captured half-open ranges.

    Raises:
        CheckpointOverlapError: If the ranges intersect.
    """
    begin, end = key_range
    if any(
        begin < other_end and other_begin < end for other_begin, other_end in captured
    ):
        raise CheckpointOverlapError("Checkpoint or directory metadata overlaps CDC")


class CheckpointStore:
    """Persist full cursors using bounded optimistic FDB transactions."""

    def __init__(
        self,
        client: FDBClient,
        *,
        sink: SinkIdentity,
        path: tuple[str, ...] = ("fdb-kafka-bridge", "checkpoints"),
        timeout_s: float = 5.0,
        retry_limit: int = 10,
    ) -> None:
        """Configure the store without touching FDB.

        Args:
            client: Active FDB client.
            sink: Kafka cluster and topic that received the checkpointed data.
            path: Directory-layer path for checkpoint keys.
            timeout_s: Transaction timeout, including retries.
            retry_limit: Maximum transaction retries.

        Raises:
            ValueError: If configuration is invalid.
        """
        if not path or any(not isinstance(part, str) or not part for part in path):
            raise ValueError("checkpoint path must contain non-empty strings")
        if (
            isinstance(timeout_s, bool)
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise ValueError("checkpoint timeout must be finite and positive")
        if type(retry_limit) is not int or retry_limit < 0:
            raise ValueError("checkpoint retry_limit must be nonnegative")
        self.sink = sink
        self.client = client
        self.path = path
        self.timeout_s = timeout_s
        self.retry_limit = retry_limit
        self._directory: Any = None

    def _run(self, operation: Callable[[Any], T], *, write: bool = False) -> T:
        fdb = import_module("fdb")
        tr = self.client.db.create_transaction()
        tr.options.set_timeout(max(1, int(self.timeout_s * 1000)))
        tr.options.set_retry_limit(self.retry_limit)
        try:
            attempt = 0
            while True:
                try:
                    result = operation(tr)
                    if write:
                        tr.commit().wait()
                    return result
                except fdb.FDBError as exc:
                    if attempt == self.retry_limit:
                        raise CheckpointWriteError(
                            "Checkpoint retries exhausted"
                        ) from exc
                    attempt += 1
                    tr.on_error(exc.code).wait()
        except fdb.FDBError as exc:
            raise CheckpointWriteError("Checkpoint transaction failed") from exc
        finally:
            tr.cancel()

    def open(
        self, *, create: bool = True, forbidden_ranges: list[KeyRange] | None = None
    ) -> None:
        """Resolve or allocate the directory, checking overlap before commit.

        Args:
            create: False performs a read-only lookup of an existing directory.
            forbidden_ranges: Captured ranges that must exclude store writes.

        Raises:
            CheckpointOverlapError: If store writes could enter CDC.
            CheckpointWriteError: If the transaction fails.
        """
        directory = import_module("fdb").directory
        captured = forbidden_ranges or []
        check_overlap((b"\xfe", b"\xff"), captured)

        def resolve(tr: Any) -> Any:
            if not create and not directory.exists(tr, self.path):
                return None
            subspace = (
                directory.create_or_open(tr, self.path)
                if create
                else directory.open(tr, self.path)
            )
            prefix = subspace.key()
            check_overlap((prefix, strinc(prefix)), captured)
            return subspace

        self._directory = self._run(resolve, write=create)

    def key_range(self) -> KeyRange:
        """Return the allocated checkpoint directory range.

        Raises:
            CheckpointError: If the directory has not been allocated.
        """
        if self._directory is None:
            raise CheckpointError("Checkpoint directory is not open")
        prefix = self._directory.key()
        return prefix, strinc(prefix)

    def _decode(self, value: bytes | None) -> CheckpointCursor | None:
        if value is None:
            return None
        try:
            fields = import_module("fdb.tuple").unpack(value)
            if len(fields) != 5 or type(fields[0]) is not int or fields[0] != 1:
                raise ValueError("unsupported checkpoint format")
            stored_sink = SinkIdentity(fields[3], fields[4])
            if stored_sink != self.sink:
                raise CheckpointSinkMismatchError(
                    f"Stored sink {stored_sink}; configured sink {self.sink}"
                )
            return CheckpointCursor(fields[1], fields[2])
        except (ValueError, TypeError, IndexError, OverflowError) as exc:
            raise CheckpointFormatError("Invalid checkpoint data") from exc

    def load(self, stream_name: bytes) -> CheckpointCursor | None:
        """Read and validate a stream's persisted cursor.

        Args:
            stream_name: Raw stream name.

        Returns:
            Stored cursor, or None when no checkpoint exists.
        """
        if self._directory is None:
            return None
        key = self._directory.pack((stream_name,))
        return self._run(lambda tr: self._decode(tr.get(key).value))

    def save(self, stream_name: bytes, cursor: Any) -> None:
        """Save a monotonic cursor after all its Kafka records are durable.

        Args:
            stream_name: Raw stream name.
            cursor: Native cursor exposing stream_id and last_consumed_version.

        Raises:
            CheckpointError: If identity, progress, format or transaction checks fail.
        """
        self.key_range()
        new = CheckpointCursor(cursor.stream_id, cursor.last_consumed_version)
        key = self._directory.pack((stream_name,))
        value = import_module("fdb.tuple").pack(
            (
                1,
                new.stream_id,
                new.last_consumed_version,
                self.sink.kafka_cluster_id,
                self.sink.topic,
            )
        )

        def save(tr: Any) -> None:
            stored = self._decode(tr.get(key).value)
            if validate_progress(stored, new):
                tr[key] = value

        self._run(save, write=True)

    def delete(self, stream_name: bytes) -> None:
        """Delete one checkpoint for explicit operator recovery.

        Args:
            stream_name: Raw stream name.
        """
        self.key_range()
        key = self._directory.pack((stream_name,))

        def remove(tr: Any) -> None:
            del tr[key]

        self._run(remove, write=True)
