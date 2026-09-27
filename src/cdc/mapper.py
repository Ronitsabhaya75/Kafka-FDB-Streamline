"""Mappers for converting raw FoundationDB CDC mutations to Protobuf messages."""

import time
from collections.abc import Iterable
from typing import Any, Protocol

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2

# Type code for CLEAR_RANGE in FoundationDB CDC.
_CLEAR_RANGE_TYPE_CODE = 1


class RawMutation(Protocol):
    """Protocol for raw CDC mutation objects conforming to FDB CdcMutation."""

    @property
    def type(self) -> int:
        """Raw operation type code."""
        ...

    @property
    def param1(self) -> bytes:
        """Key, or begin key of clear range."""
        ...

    @property
    def param2(self) -> bytes:
        """Value or operand, or end key of clear range."""
        ...


def to_timestamp(timestamp_ns: int | None = None) -> Timestamp:
    """Create a Protobuf Timestamp message from nanoseconds since Unix epoch.

    Args:
        timestamp_ns: Nanoseconds since Unix epoch. If None, uses current time.

    Returns:
        Populated google.protobuf.Timestamp message.
    """
    if timestamp_ns is None:
        timestamp_ns = time.time_ns()
    seconds, nanos = divmod(timestamp_ns, 10**9)
    return Timestamp(seconds=seconds, nanos=nanos)


def to_version_index(
    fdb_version: int, sequence_no: int
) -> mutations_pb2.FDBVersionIndex:
    """Create an FDBVersionIndex Protobuf message.

    Args:
        fdb_version: Commit version of the mutation.
        sequence_no: Zero-based sequence number within the commit version.

    Returns:
        Populated FDBVersionIndex message.
    """
    return mutations_pb2.FDBVersionIndex(
        fdb_version=fdb_version,
        sequence_no=sequence_no,
    )


def to_single_key_mutation(
    key: bytes, value: bytes, mutation_type: int
) -> mutations_pb2.FDBSingleKeyMutation:
    """Map key, value, and mutation type code to FDBSingleKeyMutation.

    Args:
        key: Target key in FoundationDB.
        value: Target value or operand.
        mutation_type: Operation type code (e.g. SET_VALUE=0, ADD=2).

    Returns:
        Populated FDBSingleKeyMutation Protobuf message.
    """
    return mutations_pb2.FDBSingleKeyMutation(
        key=key,
        value=value,
        mutation_type=mutation_type,  # type: ignore[arg-type]
    )


def to_clear_range(begin_key: bytes, end_key: bytes) -> mutations_pb2.FDBClearRange:
    """Map begin and end keys to FDBClearRange.

    Args:
        begin_key: Inclusive start key of the cleared range.
        end_key: Exclusive end key of the cleared range.

    Returns:
        Populated FDBClearRange Protobuf message.
    """
    return mutations_pb2.FDBClearRange(
        begin_key=begin_key,
        end_key=end_key,
    )


def to_fdb_mutation(
    raw_mutation: RawMutation | tuple[int, bytes, bytes] | Any,
    fdb_version: int,
    sequence_no: int,
) -> mutations_pb2.FDBMutation:
    """Map a raw FDB mutation to a tagged FDBMutation Protobuf message.

    Args:
        raw_mutation: Raw mutation object (with type, param1, param2) or 3-tuple.
        fdb_version: Commit version of the mutation.
        sequence_no: Sequence number within the commit version.

    Returns:
        Populated FDBMutation message with either single_key_mutation or clear_range.
    """
    if isinstance(raw_mutation, tuple):
        type_code, param1, param2 = raw_mutation
    else:
        type_code = raw_mutation.type
        param1 = raw_mutation.param1
        param2 = raw_mutation.param2

    version_index = to_version_index(fdb_version, sequence_no)

    if type_code == _CLEAR_RANGE_TYPE_CODE:
        return mutations_pb2.FDBMutation(
            version_index=version_index,
            clear_range=to_clear_range(param1, param2),
        )

    return mutations_pb2.FDBMutation(
        version_index=version_index,
        single_key_mutation=to_single_key_mutation(param1, param2, type_code),
    )


def to_fdb_mutation_batch(
    raw_mutations: Iterable[RawMutation | tuple[int, bytes, bytes] | Any],
    fdb_version: int,
    first_sequence_no: int = 0,
) -> mutations_pb2.FDBMutationBatch:
    """Map an iterable of raw mutations to an FDBMutationBatch message.

    Args:
        raw_mutations: Raw mutations from the same commit version.
        fdb_version: Commit version of the batch.
        first_sequence_no: Starting sequence number within the version.

    Returns:
        Populated FDBMutationBatch Protobuf message.
    """
    mutations = [
        to_fdb_mutation(m, fdb_version, first_sequence_no + idx)
        for idx, m in enumerate(raw_mutations)
    ]
    return mutations_pb2.FDBMutationBatch(mutations=mutations)


def to_version_end(
    fdb_version: int,
    total_mutations: int,
    bridge_timestamp_ns: int | None = None,
) -> mutations_pb2.VersionEnd:
    """Create a VersionEnd Protobuf message.

    Args:
        fdb_version: Concluded commit version.
        total_mutations: Total number of mutations in this version group.
        bridge_timestamp_ns: Nanoseconds since Unix epoch. If None, uses current time.

    Returns:
        Populated VersionEnd Protobuf message.
    """
    ts = to_timestamp(bridge_timestamp_ns)
    return mutations_pb2.VersionEnd(
        fdb_version=fdb_version,
        total_mutations=total_mutations,
        bridge_timestamp=ts,
    )


def to_mutation_record(
    stream_name: str,
    *,
    mutation: mutations_pb2.FDBMutation | None = None,
    batch: mutations_pb2.FDBMutationBatch | None = None,
    version_end: mutations_pb2.VersionEnd | None = None,
    bridge_timestamp_ns: int | None = None,
) -> mutations_pb2.FDBMutationRecord:
    """Wrap a mutation, batch, or version end into an FDBMutationRecord envelope.

    Args:
        stream_name: Identifier for the stream or directory.
        mutation: Optional single FDBMutation payload.
        batch: Optional FDBMutationBatch payload.
        version_end: Optional VersionEnd boundary payload.
        bridge_timestamp_ns: Nanoseconds since Unix epoch. If None, uses current time.

    Returns:
        Populated FDBMutationRecord Protobuf message.

    Raises:
        ValueError: If not exactly one of mutation, batch, or version_end is provided.
    """
    payloads = [p for p in (mutation, batch, version_end) if p is not None]
    if len(payloads) != 1:
        raise ValueError(
            "Exactly one of mutation, batch, or version_end must be provided"
        )

    kwargs: dict[str, Any] = {
        "stream_name": stream_name,
        "bridge_timestamp": to_timestamp(bridge_timestamp_ns),
    }
    if mutation is not None:
        kwargs["mutation"] = mutation
    elif batch is not None:
        kwargs["batch"] = batch
    else:
        kwargs["version_end"] = version_end

    return mutations_pb2.FDBMutationRecord(**kwargs)
