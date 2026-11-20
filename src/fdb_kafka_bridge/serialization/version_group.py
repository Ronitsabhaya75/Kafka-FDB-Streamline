"""A whole version group as its ordered record sequence."""

from collections.abc import Callable, Iterable

from google.protobuf.message import Message

from fdb_kafka_bridge.serialization import _checks, _wire
from fdb_kafka_bridge.serialization.errors import (
    InputTypeError,
    InputValueError,
    RecordTooLargeError,
)
from fdb_kafka_bridge.serialization.model import (
    FDBDirectoryContext,
    MutationType,
    NativeMutation,
)
from fdb_kafka_bridge.serialization.serializer import serialize_version_end
from fdbkafka.cdc.v1 import mutations_pb2


def _varint_bytes(value: int) -> int:
    return max(1, (value.bit_length() + 6) // 7)


def _tag_bytes(message: type[Message], field_name: str) -> int:
    # A tag is the varint of `field_number << 3 | wire_type`.
    return _varint_bytes(message.DESCRIPTOR.fields_by_name[field_name].number << 3)


_BATCH_TAG_BYTES = _tag_bytes(mutations_pb2.FDBMutationRecord, "batch")
_MUTATIONS_TAG_BYTES = _tag_bytes(mutations_pb2.FDBMutationBatch, "mutations")


def _framed_bytes(tag_bytes: int, payload_bytes: int) -> int:
    """Size of a length-delimited field: tag, length varint, payload."""
    return tag_bytes + _varint_bytes(payload_bytes) + payload_bytes


_is_range_within_subspace = _checks.is_range_within_subspace


def serialize_version_group(
    mutations: Iterable[NativeMutation],
    *,
    fdb_version: int,
    max_record_bytes: int,
    stream_name: str,
    bridge_timestamp_ns: int,
    stream_id: int | None = None,
    source_id: str | None = None,
    directory_resolver: (
        Callable[[NativeMutation], FDBDirectoryContext | None] | None
    ) = None,
    strict_directory_clears: bool = False,
) -> list[bytes]:
    """Serialize a whole version group as batch records plus its version end.

    Slicing is greedy in native order. Batches are split when the resolved
    directory context changes, or when the next mutation would push the record
    over `max_record_bytes`. Native mutation sequence numbers are assigned
    before splitting, and native mutation order is strictly preserved.
    VersionEnd remains stream-wide and carries no directory prefix.

    Args:
        mutations: Every native mutation of the group. Consumed once, in order.
        fdb_version: Commit version of the version group.
        max_record_bytes: Upper bound on `len()` of each returned record.
        stream_name: The stream's registered name.
        bridge_timestamp_ns: Build time, integer ns since the Unix epoch.
        stream_id: Optional native CDC stream identifier.
        source_id: Optional stable operator-configured source identity.
        directory_resolver: Optional callable mapping a native mutation to its
            resolved `FDBDirectoryContext`.
        strict_directory_clears: When True, requires `directory_resolver` to be
            provided and raises `InputValueError` if a range clear is unresolved
            or crosses directory boundaries. When False, ambiguous or unresolved
            range clears preserve the native clear unchanged and omit directory
            context on that batch.

    Returns:
        Homogeneous batch records covering positions `0 .. n-1` in order, then one
        stream-wide version-end record with `total_mutations == n`.

    Raises:
        InputTypeError: A keyword or attribute has the wrong Python type.
        InputValueError: A keyword or a type code is out of range, or an ambiguous
            clear range is encountered in strict mode.
        RecordTooLargeError: One mutation alone, or the version end, does not fit
            `max_record_bytes`.
    """
    if type(strict_directory_clears) is not bool:
        got_strict = type(strict_directory_clears).__name__
        raise InputTypeError(
            f"strict_directory_clears must be bool, got {got_strict}",
            field="strict_directory_clears",
        )
    if directory_resolver is not None and not callable(directory_resolver):
        got_resolver = type(directory_resolver).__name__
        raise InputTypeError(
            f"directory_resolver must be callable, got {got_resolver}",
            field="directory_resolver",
        )
    if strict_directory_clears and directory_resolver is None:
        raise InputValueError(
            "strict_directory_clears=True requires a directory_resolver",
            field="strict_directory_clears",
        )
    _checks.fdb_version(fdb_version)
    _checks.max_record_bytes(max_record_bytes)
    _checks.envelope(stream_name, bridge_timestamp_ns)
    _checks.stream_id(stream_id)
    _checks.source_id(source_id)
    _checks.identity_pair(stream_id, source_id)

    raw_mutations = list(_checks.mutations(mutations))
    checked_natives: list[tuple[int, bytes, bytes]] = []
    resolved_directories: list[FDBDirectoryContext | None] = []

    for index, mut in enumerate(raw_mutations):
        _checks.assigned_sequence_no(index, index=index)
        code, p1, p2 = _checks.native(mut, index=index)
        checked_natives.append((code, p1, p2))

        dir_ctx: FDBDirectoryContext | None = None
        if directory_resolver is not None:
            dir_ctx = directory_resolver(mut)
            if dir_ctx is not None:
                _checks.directory_context(dir_ctx, index=index)

        if code == MutationType.CLEAR_RANGE:
            if dir_ctx is not None:
                if not _is_range_within_subspace(p1, p2, dir_ctx.subspace_prefix):
                    if strict_directory_clears:
                        raise InputValueError(
                            f"Clear range crosses directory boundaries at "
                            f"version {fdb_version}, sequence {index}",
                            field="mutations",
                            index=index,
                        )
                    dir_ctx = None
            else:
                if strict_directory_clears:
                    raise InputValueError(
                        f"Unresolved clear range under strict_directory_clears at "
                        f"version {fdb_version}, sequence {index}",
                        field="mutations",
                        index=index,
                    )
        elif dir_ctx is not None:
            _checks.check_mutation_within_directory(
                code,
                p1,
                p2,
                dir_ctx,
                field="mutations",
                index=index,
                fdb_version=fdb_version,
            )

        resolved_directories.append(dir_ctx)

    messages = _wire.mutation_messages(checked_natives, fdb_version=fdb_version)

    def _envelope_size(directory: FDBDirectoryContext | None) -> int:
        return len(
            _wire.record(
                stream_name,
                bridge_timestamp_ns,
                directory=directory,
                stream_id=stream_id,
                source_id=source_id,
            )
        )

    slices: list[tuple[int, int, FDBDirectoryContext | None]] = []
    start = 0
    batch_bytes = 0
    current_dir: FDBDirectoryContext | None = (
        resolved_directories[0] if resolved_directories else None
    )
    envelope_bytes = _envelope_size(current_dir)

    for i, message in enumerate(messages):
        dir_i = resolved_directories[i]
        # Split when resolved directory context changes
        if dir_i != current_dir:
            if i > start:
                slices.append((start, i, current_dir))
            start = i
            current_dir = dir_i
            batch_bytes = 0
            envelope_bytes = _envelope_size(current_dir)

        mutation_bytes = _framed_bytes(_MUTATIONS_TAG_BYTES, message.ByteSize())
        grown = envelope_bytes + _framed_bytes(
            _BATCH_TAG_BYTES, batch_bytes + mutation_bytes
        )
        if batch_bytes and grown > max_record_bytes:
            slices.append((start, i, current_dir))
            start = i
            batch_bytes = 0

        if not batch_bytes:
            alone = envelope_bytes + _framed_bytes(_BATCH_TAG_BYTES, mutation_bytes)
            if alone > max_record_bytes:
                raise RecordTooLargeError(
                    f"mutation at position {i} of version {fdb_version} needs a "
                    f"{alone}-byte record; max_record_bytes is {max_record_bytes}",
                    index=i,
                    record_bytes=alone,
                    max_record_bytes=max_record_bytes,
                )
        batch_bytes += mutation_bytes

    if messages:
        slices.append((start, len(messages), current_dir))

    version_end = serialize_version_end(
        fdb_version=fdb_version,
        total_mutations=len(messages),
        stream_name=stream_name,
        bridge_timestamp_ns=bridge_timestamp_ns,
        stream_id=stream_id,
        source_id=source_id,
    )
    if len(version_end) > max_record_bytes:
        raise RecordTooLargeError(
            f"version end of version {fdb_version} needs a {len(version_end)}-byte "
            f"record; max_record_bytes is {max_record_bytes}",
            index=None,
            record_bytes=len(version_end),
            max_record_bytes=max_record_bytes,
        )

    records = [
        _wire.record(
            stream_name,
            bridge_timestamp_ns,
            directory=slice_dir,
            stream_id=stream_id,
            source_id=source_id,
            batch=mutations_pb2.FDBMutationBatch(mutations=messages[a:b]),
        )
        for a, b, slice_dir in slices
    ]
    records.append(version_end)
    return records
