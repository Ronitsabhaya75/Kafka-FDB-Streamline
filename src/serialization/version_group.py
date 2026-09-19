"""A whole version group as its ordered record sequence."""

from collections.abc import Iterable

from google.protobuf.message import Message

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization import _checks, _wire
from src.serialization.errors import RecordTooLargeError
from src.serialization.model import NativeMutation
from src.serialization.serializer import serialize_version_end


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
    """Serialize a whole version group as batch records plus its version end.

    Slicing is greedy in native order. A slice closes only when the next mutation
    would push its record over `max_record_bytes`. The caller must pass the whole
    group, unfiltered and in native order. A slice of a group belongs in
    `serialize_batch`. Passed here, it gets a wrong `total_mutations`.

    Args:
        mutations: Every native mutation of the group. Consumed once, in order.
        fdb_version: Commit version of the version group.
        max_record_bytes: Upper bound on `len()` of each returned record.
        stream_name: The stream's registered name.
        bridge_timestamp_ns: Build time, integer ns since the Unix epoch.

    Returns:
        Batch records covering positions `0 .. n-1` in order, then one version-end
        record with `total_mutations == n`. An empty group returns the version end
        alone.

    Raises:
        InputTypeError: A keyword or attribute has the wrong Python type, `mutations`
            is not iterable, or an element lacks `type`, `param1` or `param2`.
            `index` is the element's position.
        InputValueError: A keyword or a type code is out of range.
        RecordTooLargeError: One mutation alone, or the version end, does not fit
            `max_record_bytes`. A mutation is never split.
    """
    _checks.fdb_version(fdb_version)
    _checks.max_record_bytes(max_record_bytes)
    _checks.envelope(stream_name, bridge_timestamp_ns)
    messages = _wire.mutation_messages(
        _checks.natives(mutations), fdb_version=fdb_version
    )
    envelope_bytes = len(_wire.record(stream_name, bridge_timestamp_ns))

    # Sizes are computed, not measured by serializing. A batch record is the envelope
    # plus one framed batch, whose payload is the sum of its framed mutations.
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
        _wire.record(
            stream_name,
            bridge_timestamp_ns,
            batch=mutations_pb2.FDBMutationBatch(mutations=messages[a:b]),
        )
        for a, b in slices
    ]
    records.append(version_end)
    return records
