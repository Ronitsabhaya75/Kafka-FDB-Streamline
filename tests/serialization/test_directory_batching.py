"""Tests for Directory Layer batch splitting, order preservation, and clear ranges."""

from __future__ import annotations

from typing import NamedTuple

import pytest
from google.protobuf import (
    descriptor_pb2,
    descriptor_pool,
    message_factory,
    timestamp_pb2,
)

from fdb_kafka_bridge.serialization.deserializer import deserialize_record
from fdb_kafka_bridge.serialization.errors import (
    InputValueError,
    SerializationError,
)
from fdb_kafka_bridge.serialization.model import (
    FDBDirectoryContext,
    Mutation,
    MutationBatch,
    MutationType,
    VersionEnd,
    deduplication_identity,
    legacy_deduplication_identity,
)
from fdb_kafka_bridge.serialization.serializer import serialize_mutation
from fdb_kafka_bridge.serialization.version_group import serialize_version_group
from fdbkafka.cdc.v1 import mutations_pb2


class _FakeNativeMutation(NamedTuple):
    type: int
    param1: bytes
    param2: bytes
    directory: FDBDirectoryContext | None = None


def test_directory_aware_batch_splitting_without_reordering() -> None:
    dir_a = FDBDirectoryContext(path=("tenants", "a"), subspace_prefix=b"\x01")
    dir_b = FDBDirectoryContext(path=("tenants", "b"), subspace_prefix=b"\x02")

    # Sequence of mutations alternating: A -> B -> A
    # If someone grouped by directory, they would reorder to A, A, B.
    # Our rule: NEVER regroup in a way that reorders!
    mutations = [
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x10", b"val_a1", dir_a),
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x02\x10", b"val_b1", dir_b),
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x20", b"val_a2", dir_a),
    ]

    records_bytes = serialize_version_group(
        mutations,
        fdb_version=1000,
        max_record_bytes=10_000,
        stream_name="orders_stream",
        bridge_timestamp_ns=1_000_000_000,
        stream_id=42,
        source_id="fdb-primary",
    )

    # Expected: 3 batches (split on dir change) + 1 stream-wide VersionEnd
    assert len(records_bytes) == 4
    records = [deserialize_record(b) for b in records_bytes]

    # Batch 0: dir_a, sequence_no = 0
    rec0 = records[0]
    assert rec0.directory == dir_a
    assert rec0.stream_id == 42
    assert rec0.source_id == "fdb-primary"
    assert isinstance(rec0.body, MutationBatch)
    assert len(rec0.body.mutations) == 1
    assert rec0.body.mutations[0].version_index.sequence_no == 0
    assert rec0.body.mutations[0].param1 == b"\x01\x10"

    # Batch 1: dir_b, sequence_no = 1
    rec1 = records[1]
    assert rec1.directory == dir_b
    assert rec1.stream_id == 42
    assert rec1.source_id == "fdb-primary"
    assert isinstance(rec1.body, MutationBatch)
    assert len(rec1.body.mutations) == 1
    assert rec1.body.mutations[0].version_index.sequence_no == 1
    assert rec1.body.mutations[0].param1 == b"\x02\x10"

    # Batch 2: dir_a, sequence_no = 2
    rec2 = records[2]
    assert rec2.directory == dir_a
    assert rec2.stream_id == 42
    assert rec2.source_id == "fdb-primary"
    assert isinstance(rec2.body, MutationBatch)
    assert len(rec2.body.mutations) == 1
    assert rec2.body.mutations[0].version_index.sequence_no == 2
    assert rec2.body.mutations[0].param1 == b"\x01\x20"

    # VersionEnd: stream-wide, total_mutations = 3, NO directory context!
    end_rec = records[3]
    assert end_rec.directory is None
    assert end_rec.stream_id == 42
    assert end_rec.source_id == "fdb-primary"
    assert isinstance(end_rec.body, VersionEnd)
    assert end_rec.body.fdb_version == 1000
    assert end_rec.body.total_mutations == 3


def test_directory_batch_split_on_max_record_bytes() -> None:
    dir_a = FDBDirectoryContext(path=("tenants", "a"), subspace_prefix=b"\x01")

    # 4 mutations all in dir_a, but max_record_bytes is tight so it must split
    mutations = [
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x01", b"x" * 200, dir_a),
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x02", b"x" * 200, dir_a),
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x03", b"x" * 200, dir_a),
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x04", b"x" * 200, dir_a),
    ]

    records_bytes = serialize_version_group(
        mutations,
        fdb_version=2000,
        max_record_bytes=400,  # Each record fits roughly 1 mutation
        stream_name="test_stream",
        bridge_timestamp_ns=2_000_000_000,
    )

    records = [deserialize_record(b) for b in records_bytes]
    # At least 4 batches + 1 VersionEnd
    assert len(records) == 5
    for i in range(4):
        assert records[i].directory == dir_a
        assert isinstance(records[i].body, MutationBatch)
        assert records[i].body.mutations[0].version_index.sequence_no == i


def test_ambiguous_clear_range_non_strict() -> None:
    dir_a = FDBDirectoryContext(path=("tenants", "a"), subspace_prefix=b"\x01")

    # Clear range crossing beyond prefix b"\x01" into b"\x03"
    mutations = [
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01\x05", b"val", dir_a),
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x00\x99", b"\x02\x00", dir_a),
    ]

    records_bytes = serialize_version_group(
        mutations,
        fdb_version=3000,
        max_record_bytes=10_000,
        stream_name="test_stream",
        bridge_timestamp_ns=1_000_000_000,
        strict_directory_clears=False,
    )

    records = [deserialize_record(b) for b in records_bytes]
    # Batch 0: dir_a
    assert records[0].directory == dir_a
    # Batch 1: ambiguous clear range -> directory context is omitted (None)
    assert records[1].directory is None
    assert isinstance(records[1].body, MutationBatch)
    assert records[1].body.mutations[0].type == MutationType.CLEAR_RANGE
    assert records[1].body.mutations[0].param1 == b"\x00\x99"
    assert records[1].body.mutations[0].param2 == b"\x02\x00"


def test_ambiguous_clear_range_strict_mode_rejects() -> None:
    dir_a = FDBDirectoryContext(path=("tenants", "a"), subspace_prefix=b"\x01")

    mutations = [
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x00\x99", b"\x02\x00", dir_a),
    ]

    with pytest.raises(SerializationError, match="Ambiguous clear range"):
        serialize_version_group(
            mutations,
            fdb_version=3000,
            max_record_bytes=10_000,
            stream_name="test_stream",
            bridge_timestamp_ns=1_000_000_000,
            strict_directory_clears=True,
        )


def test_directory_resolver_callable() -> None:
    dir_orders = FDBDirectoryContext(path=("orders",), subspace_prefix=b"ord_")
    dir_users = FDBDirectoryContext(path=("users",), subspace_prefix=b"usr_")

    def resolve(mut: _FakeNativeMutation) -> FDBDirectoryContext | None:
        if mut.param1.startswith(b"ord_"):
            return dir_orders
        if mut.param1.startswith(b"usr_"):
            return dir_users
        return None

    raw_muts = [
        _FakeNativeMutation(MutationType.SET_VALUE, b"ord_1", b"v1"),
        _FakeNativeMutation(MutationType.SET_VALUE, b"usr_1", b"v2"),
    ]

    records_bytes = serialize_version_group(
        raw_muts,
        fdb_version=4000,
        max_record_bytes=10_000,
        stream_name="app_stream",
        bridge_timestamp_ns=1000,
        directory_resolver=resolve,
    )

    records = [deserialize_record(b) for b in records_bytes]
    assert records[0].directory == dir_orders
    assert records[1].directory == dir_users


def test_v010_old_record_new_reader_compatibility() -> None:
    # Construct a legacy v0.1.0 record (only fields 1-5 set)
    legacy_proto = mutations_pb2.FDBMutationRecord(
        stream_name="legacy_stream",
        version_end=mutations_pb2.VersionEnd(
            fdb_version=5000,
            total_mutations=10,
        ),
    )
    wire = legacy_proto.SerializeToString()

    rec = deserialize_record(wire)
    assert rec.stream_name == "legacy_stream"
    assert rec.directory is None
    assert rec.stream_id is None
    assert rec.source_id is None
    assert isinstance(rec.body, VersionEnd)
    assert rec.body.fdb_version == 5000


def test_v020_new_record_old_reader_compatibility() -> None:
    # Build an actual v0.1.0 prototype in an isolated descriptor pool that
    # only defines fields 1-5 and has no knowledge of fields 6, 7, 8.
    pool = descriptor_pool.DescriptorPool()
    dp_ts = descriptor_pb2.FileDescriptorProto()
    timestamp_pb2.DESCRIPTOR.CopyToProto(dp_ts)
    pool.Add(dp_ts)

    file_dp = descriptor_pb2.FileDescriptorProto()
    file_dp.name = "fdbkafka/cdc/v1/mutations_legacy.proto"
    file_dp.package = "fdbkafka.cdc.v1.legacy"
    file_dp.syntax = "proto3"
    file_dp.dependency.append("google/protobuf/timestamp.proto")

    msg_vi = file_dp.message_type.add()
    msg_vi.name = "FDBVersionIndex"
    msg_vi.field.add(
        name="fdb_version",
        number=1,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_UINT64,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    msg_vi.field.add(
        name="sequence_no",
        number=2,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_UINT32,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )

    msg_skm = file_dp.message_type.add()
    msg_skm.name = "FDBSingleKeyMutation"
    msg_skm.field.add(
        name="key",
        number=1,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    msg_skm.field.add(
        name="value",
        number=2,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    msg_skm.field.add(
        name="mutation_type",
        number=3,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )

    msg_m = file_dp.message_type.add()
    msg_m.name = "FDBMutation"
    msg_m.field.add(
        name="version_index",
        number=1,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        type_name=".fdbkafka.cdc.v1.legacy.FDBVersionIndex",
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    msg_m.field.add(
        name="single_key_mutation",
        number=2,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        type_name=".fdbkafka.cdc.v1.legacy.FDBSingleKeyMutation",
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )

    msg_rec = file_dp.message_type.add()
    msg_rec.name = "FDBMutationRecord"
    msg_rec.field.add(
        name="stream_name",
        number=1,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    msg_rec.field.add(
        name="bridge_timestamp",
        number=2,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        type_name=".google.protobuf.Timestamp",
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    msg_rec.field.add(
        name="mutation",
        number=3,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        type_name=".fdbkafka.cdc.v1.legacy.FDBMutation",
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )

    pool.Add(file_dp)
    legacy_cls = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("fdbkafka.cdc.v1.legacy.FDBMutationRecord")
    )

    dir_ctx = FDBDirectoryContext(path=("legacy", "compat"), subspace_prefix=b"\x99")
    wire = serialize_mutation(
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x99key", b"val"),
        fdb_version=6000,
        sequence_no=0,
        stream_name="compat_stream",
        bridge_timestamp_ns=1000,
        directory=dir_ctx,
        stream_id=999,
        source_id="fdb-source-east",
    )

    # An old v0.1.0 reader parsing the wire into protobuf:
    parsed = legacy_cls()
    parsed.ParseFromString(wire)
    # Fields 1-3 decode perfectly
    assert parsed.stream_name == "compat_stream"
    assert parsed.mutation.version_index.fdb_version == 6000
    assert parsed.mutation.single_key_mutation.key == b"\x99key"
    # Unknown fields (6, 7, 8) are preserved upon re-serialization
    assert len(parsed.SerializeToString()) == len(wire)


def test_deduplication_identity() -> None:
    dir_ctx = FDBDirectoryContext(path=("test",), subspace_prefix=b"\x01")
    wire = serialize_mutation(
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01key", b"val"),
        fdb_version=7000,
        sequence_no=5,
        stream_name="stream_dedup",
        bridge_timestamp_ns=1000,
        directory=dir_ctx,
        stream_id=88,
        source_id="source-prod-1",
    )

    rec = deserialize_record(wire)
    assert isinstance(rec.body, Mutation)
    # Production identity: (source_id, stream_id, fdb_version, sequence_no)
    assert deduplication_identity(rec, rec.body) == (
        "source-prod-1",
        88,
        7000,
        5,
    )
    # Legacy fallback: (fdb_version, sequence_no)
    assert legacy_deduplication_identity(rec.body) == (7000, 5)

    # stream_id == 0 boundary test: must decode as 0, not None
    wire_zero = serialize_mutation(
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01key", b"val"),
        fdb_version=7000,
        sequence_no=5,
        stream_name="stream_dedup",
        bridge_timestamp_ns=1000,
        directory=dir_ctx,
        stream_id=0,
        source_id="source-prod-0",
    )
    rec_zero = deserialize_record(wire_zero)
    assert rec_zero.stream_id == 0
    assert deduplication_identity(rec_zero, rec_zero.body) == (
        "source-prod-0",
        0,
        7000,
        5,
    )

    # stream_id == UINT64_MAX boundary test
    uint64_max = (1 << 64) - 1
    wire_max = serialize_mutation(
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01key", b"val"),
        fdb_version=7000,
        sequence_no=5,
        stream_name="stream_dedup",
        bridge_timestamp_ns=1000,
        directory=dir_ctx,
        stream_id=uint64_max,
        source_id="source-prod-max",
    )
    rec_max = deserialize_record(wire_max)
    assert rec_max.stream_id == uint64_max
    assert deduplication_identity(rec_max, rec_max.body) == (
        "source-prod-max",
        uint64_max,
        7000,
        5,
    )

    # Empty source_id is rejected on serialization
    with pytest.raises(InputValueError, match="source_id must not be empty"):
        serialize_mutation(
            _FakeNativeMutation(MutationType.SET_VALUE, b"\x01key", b"val"),
            fdb_version=7000,
            sequence_no=5,
            stream_name="stream_dedup",
            bridge_timestamp_ns=1000,
            source_id="",
        )

    # Wire containing empty source_id string normalizes to None on decode
    wire_empty_source = mutations_pb2.FDBMutationRecord(
        stream_name="stream_dedup",
        source_id="",
        version_end=mutations_pb2.VersionEnd(fdb_version=7000),
    ).SerializeToString()
    rec_empty_source = deserialize_record(wire_empty_source)
    assert rec_empty_source.source_id is None


def test_directory_record_golden_bytes() -> None:
    golden_bytes = bytes.fromhex(
        "0a066f7264657273120b0880e2cfaa0610959aef3a1a140a0508e8071003120b0a04"
        "15016b001203ff007632170a0774656e616e74730a0874656e616e742d6112021501"
        "382a4208736f757263652d31"
    )
    dir_ctx = FDBDirectoryContext(
        path=("tenants", "tenant-a"),
        subspace_prefix=b"\x15\x01",
    )
    wire = serialize_mutation(
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x15\x01k\x00", b"\xff\x00v"),
        fdb_version=1000,
        sequence_no=3,
        stream_name="orders",
        bridge_timestamp_ns=1_700_000_000_123_456_789,
        directory=dir_ctx,
        stream_id=42,
        source_id="source-1",
    )
    assert wire == golden_bytes
    rec = deserialize_record(wire)
    assert rec.directory == dir_ctx
    assert rec.stream_id == 42
    assert rec.source_id == "source-1"
