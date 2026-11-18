"""Unit tests for multi-range CDC range validation and canonical form."""

import pytest

from fdb_kafka_bridge.cdc.errors import CDCInvalidRangeError
from fdb_kafka_bridge.cdc.ranges import (
    MAX_CDC_RANGES,
    canonicalize_key_ranges,
    validate_key_ranges,
)


def test_validate_sorts_by_raw_bytes() -> None:
    ranges = [(b"\x20", b"\x21"), (b"\x05", b"\x06"), (b"\x10", b"\x11")]
    assert validate_key_ranges(ranges) == (
        (b"\x05", b"\x06"),
        (b"\x10", b"\x11"),
        (b"\x20", b"\x21"),
    )


def test_validate_accepts_adjacent_half_open_ranges() -> None:
    assert validate_key_ranges([(b"b", b"c"), (b"a", b"b")]) == (
        (b"a", b"b"),
        (b"b", b"c"),
    )


def test_validate_accepts_range_ending_at_user_keyspace_end() -> None:
    assert validate_key_ranges([(b"\xfe", b"\xff")]) == ((b"\xfe", b"\xff"),)


@pytest.mark.parametrize(
    ("ranges", "match"),
    [
        ([], "at least one"),
        ([(b"a", b"b"), (b"a", b"b")], "duplicate"),
        ([(b"a", b"c"), (b"b", b"d")], "overlap"),
        ([(b"a", b"z"), (b"m", b"n")], "overlap"),
        ([(b"a", b"a")], "begin_key < end_key"),
        ([(b"b", b"a")], "begin_key < end_key"),
        ([(b"\xfe", b"\xff\x00")], "system key space"),
        ([(b"\xff", b"\xff\x01")], "system key space"),
        ([("a", "b")], "must be bytes"),
        ([(bytearray(b"a"), b"b")], "must be bytes"),
        ([(b"a",)], "pair"),
        ([b"ab"], "pair"),
    ],
)
def test_validate_rejects_invalid_collections(ranges: object, match: str) -> None:
    with pytest.raises(CDCInvalidRangeError, match=match):
        validate_key_ranges(ranges)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [b"ab", "ab", 7])
def test_validate_rejects_non_collections(value: object) -> None:
    with pytest.raises(CDCInvalidRangeError):
        validate_key_ranges(value)  # type: ignore[arg-type]


def test_validate_enforces_native_range_count_limit() -> None:
    many = [
        (bytes([1, i // 256, i % 256]), bytes([1, i // 256, i % 256, 0]))
        for i in range(MAX_CDC_RANGES)
    ]
    assert len(validate_key_ranges(many)) == MAX_CDC_RANGES
    with pytest.raises(CDCInvalidRangeError, match="at most 1024"):
        validate_key_ranges([*many, (b"\x02", b"\x03")])


def test_canonicalize_merges_adjacent_like_native_listing() -> None:
    ranges = validate_key_ranges([(b"c", b"d"), (b"a", b"b"), (b"b", b"c")])
    assert canonicalize_key_ranges(ranges) == ((b"a", b"d"),)


def test_canonicalize_keeps_gaps() -> None:
    ranges = validate_key_ranges([(b"a", b"b"), (b"x", b"y")])
    assert canonicalize_key_ranges(ranges) == ((b"a", b"b"), (b"x", b"y"))
