"""Unit tests for latency_probe.py against records built by the repo serializer."""

# ruff: noqa: D103  # test names are the docs, as under tests/ (pyproject ignores D there)

from collections import namedtuple

import pytest
from latency_probe import TS_WIDTH, batch_latency_ms

from fdb_kafka_bridge.serialization import (
    serialize_version_end,
    serialize_version_group,
)
from fdbkafka.cdc.v1 import mutations_pb2

Mut = namedtuple("Mut", ["type", "param1", "param2"])
SET = 0


def _value(write_ns: int) -> bytes:
    return f"{write_ns:0{TS_WIDTH}d}|".encode() + b"x" * 80


def _group(version: int, values: list[bytes]) -> list[mutations_pb2.FDBMutationRecord]:
    raw = serialize_version_group(
        [Mut(SET, f"k{i}".encode(), v) for i, v in enumerate(values)],
        fdb_version=version,
        max_record_bytes=1_000_000,
        stream_name="s",
        bridge_timestamp_ns=1,
    )
    return [mutations_pb2.FDBMutationRecord.FromString(r) for r in raw]


def _watermark(version: int) -> mutations_pb2.FDBMutationRecord:
    raw = serialize_version_end(
        fdb_version=version, total_mutations=0, stream_name="s", bridge_timestamp_ns=1
    )
    return mutations_pb2.FDBMutationRecord.FromString(raw)


def test_uses_newest_mutation_of_last_batch() -> None:
    records = _group(10, [_value(1_000_000_000)]) + _group(
        20, [_value(2_000_000_000), _value(3_000_000_000)]
    )
    records.append(_watermark(30))

    assert batch_latency_ms(records, 3_005_000_000) == pytest.approx(5.0)


def test_value_without_separator_gives_none() -> None:
    bad = f"{1_000:0{TS_WIDTH}d}x".encode() + b"pad"
    assert batch_latency_ms(_group(10, [bad]), 2_000) is None


def test_short_or_non_numeric_stamp_gives_none() -> None:
    assert batch_latency_ms(_group(10, [b"123|"]), 2_000) is None
    assert batch_latency_ms(_group(10, [b"x" * TS_WIDTH + b"|pad"]), 2_000) is None


def test_watermark_only_gives_none() -> None:
    assert batch_latency_ms([_watermark(5)], 2_000) is None
    assert batch_latency_ms([], 2_000) is None
