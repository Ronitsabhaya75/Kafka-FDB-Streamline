"""Unit tests for CDC mutation Protobuf mapping."""

from collections import namedtuple

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.mapper import (
    to_clear_range,
    to_fdb_mutation,
    to_fdb_mutation_batch,
    to_mutation_record,
    to_single_key_mutation,
    to_timestamp,
    to_version_end,
    to_version_index,
)

MockCdcMutation = namedtuple("MockCdcMutation", ["type", "param1", "param2"])


def test_to_timestamp_custom() -> None:
    ns = 1_700_000_000_123_456_789
    ts = to_timestamp(ns)
    assert isinstance(ts, Timestamp)
    assert ts.seconds == 1_700_000_000
    assert ts.nanos == 123_456_789


def test_to_timestamp_default_now() -> None:
    ts = to_timestamp(None)
    assert isinstance(ts, Timestamp)
    assert ts.seconds > 0


def test_to_version_index() -> None:
    idx = to_version_index(fdb_version=100_000, sequence_no=42)
    assert isinstance(idx, mutations_pb2.FDBVersionIndex)
    assert idx.fdb_version == 100_000
    assert idx.sequence_no == 42


def test_to_single_key_mutation() -> None:
    mut = to_single_key_mutation(key=b"test_key", value=b"test_val", mutation_type=0)
    assert isinstance(mut, mutations_pb2.FDBSingleKeyMutation)
    assert mut.key == b"test_key"
    assert mut.value == b"test_val"
    expected_type = mutations_pb2.FDBSingleKeyMutation.MUTATION_TYPE_SET_VALUE
    assert mut.mutation_type == expected_type


def test_to_single_key_atomic_mutation() -> None:
    mut = to_single_key_mutation(
        key=b"counter", value=b"\x01\x00\x00\x00", mutation_type=2
    )
    assert isinstance(mut, mutations_pb2.FDBSingleKeyMutation)
    assert mut.key == b"counter"
    assert mut.value == b"\x01\x00\x00\x00"
    assert mut.mutation_type == mutations_pb2.FDBSingleKeyMutation.MUTATION_TYPE_ADD


def test_to_clear_range() -> None:
    clear = to_clear_range(begin_key=b"start_key", end_key=b"end_key")
    assert isinstance(clear, mutations_pb2.FDBClearRange)
    assert clear.begin_key == b"start_key"
    assert clear.end_key == b"end_key"


def test_to_fdb_mutation_single_key() -> None:
    raw = MockCdcMutation(type=0, param1=b"my_key", param2=b"my_value")
    m = to_fdb_mutation(raw, fdb_version=500, sequence_no=3)
    assert isinstance(m, mutations_pb2.FDBMutation)
    assert m.version_index.fdb_version == 500
    assert m.version_index.sequence_no == 3
    assert m.WhichOneof("mutation") == "single_key_mutation"
    assert m.single_key_mutation.key == b"my_key"
    assert m.single_key_mutation.value == b"my_value"
    assert (
        m.single_key_mutation.mutation_type
        == mutations_pb2.FDBSingleKeyMutation.MUTATION_TYPE_SET_VALUE
    )


def test_to_fdb_mutation_clear_range() -> None:
    raw = MockCdcMutation(type=1, param1=b"k1", param2=b"k2")
    m = to_fdb_mutation(raw, fdb_version=600, sequence_no=0)
    assert isinstance(m, mutations_pb2.FDBMutation)
    assert m.version_index.fdb_version == 600
    assert m.version_index.sequence_no == 0
    assert m.WhichOneof("mutation") == "clear_range"
    assert m.clear_range.begin_key == b"k1"
    assert m.clear_range.end_key == b"k2"


def test_to_fdb_mutation_tuple() -> None:
    raw_tuple = (0, b"k_tup", b"v_tup")
    m = to_fdb_mutation(raw_tuple, fdb_version=700, sequence_no=1)
    assert m.single_key_mutation.key == b"k_tup"
    assert m.single_key_mutation.value == b"v_tup"


def test_to_fdb_mutation_batch() -> None:
    raw_list = [
        MockCdcMutation(type=0, param1=b"k1", param2=b"v1"),
        MockCdcMutation(type=1, param1=b"k2", param2=b"k3"),
        MockCdcMutation(type=2, param1=b"cnt", param2=b"\x01"),
    ]
    batch = to_fdb_mutation_batch(raw_list, fdb_version=800, first_sequence_no=5)
    assert isinstance(batch, mutations_pb2.FDBMutationBatch)
    assert len(batch.mutations) == 3
    assert batch.mutations[0].version_index.sequence_no == 5
    assert batch.mutations[1].version_index.sequence_no == 6
    assert batch.mutations[2].version_index.sequence_no == 7
    assert batch.mutations[0].WhichOneof("mutation") == "single_key_mutation"
    assert batch.mutations[1].WhichOneof("mutation") == "clear_range"
    assert batch.mutations[2].WhichOneof("mutation") == "single_key_mutation"


def test_to_version_end() -> None:
    ve = to_version_end(
        fdb_version=999, total_mutations=15, bridge_timestamp_ns=1_000_000
    )
    assert isinstance(ve, mutations_pb2.VersionEnd)
    assert ve.fdb_version == 999
    assert ve.total_mutations == 15
    assert ve.bridge_timestamp.nanos == 1_000_000


def test_to_mutation_record_with_single_mutation() -> None:
    m = to_fdb_mutation(MockCdcMutation(0, b"k", b"v"), fdb_version=100, sequence_no=0)
    record = to_mutation_record("orders", mutation=m)
    assert isinstance(record, mutations_pb2.FDBMutationRecord)
    assert record.stream_name == "orders"
    assert record.WhichOneof("record") == "mutation"
    assert record.mutation.single_key_mutation.key == b"k"


def test_to_mutation_record_with_batch() -> None:
    batch = to_fdb_mutation_batch(
        [MockCdcMutation(0, b"k", b"v")], fdb_version=100, first_sequence_no=0
    )
    record = to_mutation_record("orders", batch=batch)
    assert record.WhichOneof("record") == "batch"
    assert len(record.batch.mutations) == 1


def test_to_mutation_record_with_version_end() -> None:
    ve = to_version_end(100, 1)
    record = to_mutation_record("orders", version_end=ve)
    assert record.WhichOneof("record") == "version_end"
    assert record.version_end.fdb_version == 100


def test_to_mutation_record_validation() -> None:
    with pytest.raises(ValueError, match="Exactly one"):
        to_mutation_record("stream")

    m = to_fdb_mutation(MockCdcMutation(0, b"k", b"v"), 100, 0)
    ve = to_version_end(100, 1)
    with pytest.raises(ValueError, match="Exactly one"):
        to_mutation_record("stream", mutation=m, version_end=ve)
