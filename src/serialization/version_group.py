"""A whole version group as its ordered record sequence."""

from collections.abc import Iterable

from google.protobuf.message import Message

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization import _checks
from src.serialization.errors import RecordTooLargeError
from src.serialization.model import NativeMutation
from src.serialization.serializer import (
    _mutation_message,
    _read_native,
    _timestamp,
    serialize_version_end,
)


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


def serialize_version_group(
    mutations: Iterable[NativeMutation],
    *,
    fdb_version: int,
    max_record_bytes: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> list[bytes]:
    """Serialize a whole version group as batch records closed by its version end.

    Slicing is greedy in native order: a slice is closed only when appending the
    next mutation would push its record over `max_record_bytes`. The caller
    guarantees what cannot be verified here: `mutations` is the whole group,
    unfiltered and in native order. A slice of a group belongs in `serialize_batch`;
    passed here it yields a wrong `total_mutations`.

    Args:
        mutations: Every native mutation of the group; consumed exactly once, in order.
        fdb_version: Commit version of the version group.
        max_record_bytes: Upper bound on `len()` of each returned record.
        stream_name: The CDC stream's registered name.
        bridge_timestamp_ns: Wall-clock build time, integer ns since the Unix epoch.

    Returns:
        Zero or more batch records covering positions `0 .. n-1` in order, then
        exactly one version-end record with `total_mutations == n`.

    Raises:
        InputTypeError: If a keyword or a native-mutation attribute has the wrong
            Python type, `mutations` is not iterable, or an element lacks one of
            `type`, `param1`, `param2`. `index` is the element's position.
        InputValueError: If a keyword or a type code is outside its accepted domain.
        RecordTooLargeError: If one mutation's one-element batch record, or the
            version-end record, exceeds `max_record_bytes`. `index` is the
            mutation's position, `None` for the version end; `record_bytes` is the
            size of the record that did not fit. Nothing is returned.
    """
    _checks.fdb_version(fdb_version)
    _checks.max_record_bytes(max_record_bytes)
    _checks.stream_name(stream_name)
    _checks.bridge_timestamp_ns(bridge_timestamp_ns)
    natives: list[tuple[int, bytes, bytes]] = []
    for i, mutation in enumerate(_checks.mutations(mutations)):
        # Checked per element, before the read, so a group past uint32 positions
        # fails here rather than inside protobuf after the whole group is held.
        _checks.assigned_sequence_no(i, index=i)
        natives.append(_read_native(mutation, index=i))
    messages = [
        _mutation_message(*native, fdb_version, i) for i, native in enumerate(natives)
    ]
    timestamp = _timestamp(bridge_timestamp_ns)
    envelope_bytes = mutations_pb2.FDBMutationRecord(
        stream_name=stream_name, bridge_timestamp=timestamp
    ).ByteSize()

    # Record sizes are computed, never measured by serializing: a batch record is the
    # envelope plus one framed batch whose payload is the sum of its framed mutations.
    slices: list[tuple[int, int]] = []
    start = 0
    batch_bytes = 0
    for i, message in enumerate(messages):
        mutation_bytes = _framed_bytes(_MUTATIONS_TAG_BYTES, message.ByteSize())
        grown = envelope_bytes + _framed_bytes(
            _BATCH_TAG_BYTES, batch_bytes + mutation_bytes
        )
        if batch_bytes and grown > max_record_bytes:
            slices.append((start, i))
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
        slices.append((start, len(messages)))

    version_end = serialize_version_end(
        fdb_version=fdb_version,
        total_mutations=len(messages),
        stream_name=stream_name,
        bridge_timestamp_ns=bridge_timestamp_ns,
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
        mutations_pb2.FDBMutationRecord(
            stream_name=stream_name,
            bridge_timestamp=timestamp,
            batch=mutations_pb2.FDBMutationBatch(mutations=messages[a:b]),
        ).SerializeToString()
        for a, b in slices
    ]
    records.append(version_end)
    return records
