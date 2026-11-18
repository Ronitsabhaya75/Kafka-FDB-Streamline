"""Validation and canonical form of multi-range CDC registrations.

FoundationDB 8.0 registers one stream over a union of 1 to 1,024 non-empty
half-open ranges in normal user key space (``fdb_c`` ``api-c.rst``, "CDC").
The native client merges overlapping, duplicate and adjacent inputs and lists
the canonical union. This module is stricter than the native client: overlap
and duplication are rejected because a resolver that produces them has a bug,
and silently merging them could change which directory owns a key.
"""

from collections.abc import Iterable
from typing import Final

from fdb_kafka_bridge.cdc.errors import CDCInvalidRangeError

KeyRange = tuple[bytes, bytes]

#: Native limit on ranges per stream (``fdb_database_register_cdc_stream``).
MAX_CDC_RANGES: Final[int] = 1024
#: Exclusive end of normal user key space; CDC rejects system keys.
USER_KEYSPACE_END: Final[bytes] = b"\xff"


def validate_key_ranges(ranges: Iterable[KeyRange]) -> tuple[KeyRange, ...]:
    """Validate a range collection and return it sorted by raw byte boundaries.

    Adjacent ranges (one's end equals the next's begin) are accepted and kept
    separate; half-open ranges that only touch do not overlap.

    Args:
        ranges: ``(begin_key, end_key)`` pairs of ``bytes``, in any order.

    Returns:
        The ranges sorted by ``(begin_key, end_key)``.

    Raises:
        CDCInvalidRangeError: If ``ranges`` is not an iterable of byte pairs, is
            empty or longer than ``MAX_CDC_RANGES``, or any range is empty,
            reversed, outside normal user key space, a duplicate, or
            overlapping another.
    """
    if isinstance(ranges, (bytes, bytearray, str)):
        raise CDCInvalidRangeError("key_ranges must be a collection of key pairs")
    try:
        items = list(ranges)
    except TypeError as exc:
        raise CDCInvalidRangeError(
            f"key_ranges must be iterable, got {type(ranges).__name__}"
        ) from exc
    if not items:
        raise CDCInvalidRangeError("key_ranges must contain at least one range")
    if len(items) > MAX_CDC_RANGES:
        raise CDCInvalidRangeError(
            f"key_ranges has {len(items)} ranges; native CDC accepts at most "
            f"{MAX_CDC_RANGES}"
        )

    checked: list[KeyRange] = []
    for index, item in enumerate(items):
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise CDCInvalidRangeError(
                f"key range at position {index} must be a (begin_key, end_key) pair"
            )
        begin, end = item
        if type(begin) is not bytes or type(end) is not bytes:
            raise CDCInvalidRangeError(
                f"key range at position {index} boundaries must be bytes"
            )
        if begin >= end:
            raise CDCInvalidRangeError(
                f"key range at position {index} must satisfy begin_key < end_key"
            )
        if end > USER_KEYSPACE_END:
            raise CDCInvalidRangeError(
                f"key range at position {index} extends into system key space "
                "(end_key > b'\\xff')"
            )
        checked.append((begin, end))

    checked.sort()
    for previous, current in zip(checked, checked[1:], strict=False):
        if previous == current:
            raise CDCInvalidRangeError(f"duplicate key range {current!r}")
        if current[0] < previous[1]:
            raise CDCInvalidRangeError(
                f"key ranges {previous!r} and {current!r} overlap"
            )
    return tuple(checked)


def canonicalize_key_ranges(ranges: Iterable[KeyRange]) -> tuple[KeyRange, ...]:
    """Return the native canonical union of validated ranges.

    Mirrors the native client: sorted, with adjacent ranges merged. This is the
    form ``list_cdc_streams()`` reports, so it is what an existing stream's
    ranges must equal.

    Args:
        ranges: Ranges already accepted by :func:`validate_key_ranges`.

    Returns:
        Sorted, disjoint, non-adjacent ranges covering the same keys.
    """
    merged: list[KeyRange] = []
    for begin, end in sorted(ranges):
        if merged and begin <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((begin, end))
    return tuple(merged)
