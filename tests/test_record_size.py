"""Tests for the transport-neutral record size helpers."""

import os
import subprocess
import sys

import pytest

import fdb_kafka_bridge.kafka as kafka
from fdb_kafka_bridge import record_size
from fdb_kafka_bridge.serialization import serialize_version_end


def test_cdc_package_does_not_import_kafka_client() -> None:
    code = (
        "import sys, fdb_kafka_bridge.cdc; "
        "assert 'confluent_kafka' not in sys.modules, 'confluent_kafka imported'"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    subprocess.run([sys.executable, "-c", code], env=env, check=True)


@pytest.mark.parametrize(
    "name",
    [
        "DEFAULT_MESSAGE_MAX_BYTES",
        "KAFKA_MESSAGE_OVERHEAD_BYTES",
        "max_record_bytes_for_key",
    ],
)
def test_kafka_package_reexports_size_helpers(name: str) -> None:
    assert getattr(kafka, name) is getattr(record_size, name)


@pytest.mark.parametrize("stream", ["s", "stream", "€" * 10, "s" * 20_000])
@pytest.mark.parametrize(
    ("fdb_version", "total_mutations", "timestamp_ns"),
    [
        (0, 0, 0),
        (1_000, 5, 1_700_000_000_000_000_000),
        (2**63 - 1, 2**32 - 1, 253_402_300_799_999_999_999),
    ],
)
def test_every_version_end_fits_the_minimum_cap(
    stream: str, fdb_version: int, total_mutations: int, timestamp_ns: int
) -> None:
    record = serialize_version_end(
        fdb_version=fdb_version,
        total_mutations=total_mutations,
        stream_name=stream,
        bridge_timestamp_ns=timestamp_ns,
    )
    assert len(record) <= record_size.min_record_cap(stream)


def test_minimum_cap_matches_worked_example() -> None:
    assert record_size.min_record_cap("stream") == 56
