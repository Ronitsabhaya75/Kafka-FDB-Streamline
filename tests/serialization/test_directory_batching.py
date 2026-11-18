"""Tests for Directory Layer batch splitting, order preservation, and clear ranges."""

from __future__ import annotations

from dataclasses import replace
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
    DecodeFailure,
    InputTypeError,
    InputValueError,
    RecordDecodeError,
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
        directory_resolver=lambda m: m.directory,
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
        directory_resolver=lambda m: m.directory,
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
        directory_resolver=lambda m: m.directory,
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

    # Case 1: Directory context present but clear range crosses directory boundary
    mutations_cross = [
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x00\x99", b"\x02\x00", dir_a),
    ]

    with pytest.raises(
        InputValueError,
        match=(
            r"^Clear range crosses directory boundaries at "
            r"version 3000, sequence 0$"
        ),
    ) as exc_info:
        serialize_version_group(
            mutations_cross,
            fdb_version=3000,
            max_record_bytes=10_000,
            stream_name="test_stream",
            bridge_timestamp_ns=1_000_000_000,
            directory_resolver=lambda _: dir_a,
            strict_directory_clears=True,
        )
    assert exc_info.value.field == "mutations"
    assert exc_info.value.index == 0
    # Verify exception message does not leak raw keys
    assert "\\x00\\x99" not in str(exc_info.value)
    assert "\\x02\\x00" not in str(exc_info.value)

    # Case 2: strict_directory_clears=True without directory_resolver rejected up front
    mutations_no_dir = [
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x01\x10", b"\x01\x20", None),
    ]
    with pytest.raises(
        InputValueError,
        match=r"^strict_directory_clears=True requires a directory_resolver$",
    ) as exc_info2:
        serialize_version_group(
            mutations_no_dir,
            fdb_version=3001,
            max_record_bytes=10_000,
            stream_name="test_stream",
            bridge_timestamp_ns=1_000_000_000,
            strict_directory_clears=True,
        )
    assert exc_info2.value.field == "strict_directory_clears"

    # Case 3: Resolver supplied but returns None for a clear
    with pytest.raises(
        InputValueError,
        match=(
            r"^Unresolved clear range under strict_directory_clears at "
            r"version 3002, sequence 0$"
        ),
    ) as exc_info3:
        serialize_version_group(
            mutations_no_dir,
            fdb_version=3002,
            max_record_bytes=10_000,
            stream_name="test_stream",
            bridge_timestamp_ns=1_000_000_000,
            directory_resolver=lambda _: None,
            strict_directory_clears=True,
        )
    assert exc_info3.value.field == "mutations"
    assert exc_info3.value.index == 0
    assert "\\x01\\x10" not in str(exc_info3.value)


def test_clear_range_within_subspace_preserves_directory() -> None:
    dir_a = FDBDirectoryContext(path=("tenants", "a"), subspace_prefix=b"\x01")
    mutations = [
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x01\x10", b"\x01\x20", dir_a),
    ]

    # In strict mode: succeeds and preserves directory
    records_strict = serialize_version_group(
        mutations,
        fdb_version=3100,
        max_record_bytes=10_000,
        stream_name="test_stream",
        bridge_timestamp_ns=1_000_000_000,
        directory_resolver=lambda _: dir_a,
        strict_directory_clears=True,
    )
    rec_s = deserialize_record(records_strict[0])
    assert rec_s.directory == dir_a

    # In non-strict mode: succeeds and preserves directory
    records_non_strict = serialize_version_group(
        mutations,
        fdb_version=3101,
        max_record_bytes=10_000,
        stream_name="test_stream",
        bridge_timestamp_ns=1_000_000_000,
        directory_resolver=lambda _: dir_a,
        strict_directory_clears=False,
    )
    rec_ns = deserialize_record(records_non_strict[0])
    assert rec_ns.directory == dir_a


def test_mutation_key_mismatch_with_resolved_prefix_rejected() -> None:
    dir_a = FDBDirectoryContext(path=("tenants", "a"), subspace_prefix=b"\x01")
    # Point mutation key starts with \x02, but directory prefix is \x01
    mutations = [
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x02\x10", b"val", dir_a),
    ]
    with pytest.raises(
        InputValueError, match="Mutation key does not start with"
    ) as exc_info:
        serialize_version_group(
            mutations,
            fdb_version=3200,
            max_record_bytes=10_000,
            stream_name="test_stream",
            bridge_timestamp_ns=1000,
            directory_resolver=lambda _: dir_a,
        )
    assert exc_info.value.field == "mutations"
    assert exc_info.value.index == 0


def test_record_sizing_accounts_for_directory() -> None:
    dir_a = FDBDirectoryContext(
        path=("deeply", "nested", "tenant", "subspace", "for", "testing"),
        subspace_prefix=b"\x01\x02\x03\x04",
    )
    mutations = [
        _FakeNativeMutation(
            MutationType.SET_VALUE, b"\x01\x02\x03\x04key1", b"val1", dir_a
        ),
        _FakeNativeMutation(
            MutationType.SET_VALUE, b"\x01\x02\x03\x04key2", b"val2", dir_a
        ),
    ]
    # Measure record size of a single mutation with directory
    single0 = serialize_version_group(
        mutations[:1],
        fdb_version=3300,
        max_record_bytes=10_000,
        stream_name="stream",
        bridge_timestamp_ns=1000,
        directory_resolver=lambda _: dir_a,
    )
    # Budget fits either mutation alone with directory (accounting for
    # non-zero sequence_no), but not both together
    budget = len(single0[0]) + 5
    records = serialize_version_group(
        mutations,
        fdb_version=3300,
        max_record_bytes=budget,
        stream_name="stream",
        bridge_timestamp_ns=1000,
        directory_resolver=lambda _: dir_a,
    )
    assert len(records) == 3  # 2 mutation batches + 1 version end
    for r in records:
        assert len(r) <= budget


def test_bad_resolver_result_on_clear_raises_input_type_error() -> None:
    # Bad resolver returning a non-FDBDirectoryContext object on CLEAR_RANGE
    mutations = [
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x01\x10", b"\x01\x20"),
    ]
    with pytest.raises(InputTypeError, match="directory must be FDBDirectoryContext"):
        serialize_version_group(
            mutations,
            fdb_version=3400,
            max_record_bytes=10_000,
            stream_name="test_stream",
            bridge_timestamp_ns=1000,
            directory_resolver=lambda _: "not_a_context",  # type: ignore[return-value]
        )


def test_ambiguous_clear_range_non_strict_mode_omits_directory() -> None:
    # Verify both unresolved clear range input paths succeed in non-strict mode
    muts = [
        _FakeNativeMutation(MutationType.CLEAR_RANGE, b"\x01\x10", b"\x01\x20", None),
    ]
    # No resolver
    records = serialize_version_group(
        muts,
        fdb_version=3003,
        max_record_bytes=10_000,
        stream_name="test_stream",
        bridge_timestamp_ns=1_000_000_000,
        strict_directory_clears=False,
    )
    rec = deserialize_record(records[0])
    assert rec.directory is None

    # Resolver returning None
    records_resolver = serialize_version_group(
        muts,
        fdb_version=3004,
        max_record_bytes=10_000,
        stream_name="test_stream",
        bridge_timestamp_ns=1_000_000_000,
        directory_resolver=lambda _: None,
        strict_directory_clears=False,
    )
    rec_res = deserialize_record(records_resolver[0])
    assert rec_res.directory is None


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
    # 1. Construct a legacy v0.1.0 VersionEnd record (only fields 1-5 set)
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

    # 2. Construct a legacy v0.1.0 Mutation record
    legacy_mut = mutations_pb2.FDBMutationRecord(
        stream_name="legacy_stream",
        mutation=mutations_pb2.FDBMutation(
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                mutation_type=(
                    mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
                ),
                key=b"k1",
                value=b"v1",
            ),
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=5000, sequence_no=0
            ),
        ),
    ).SerializeToString()
    rec_mut = deserialize_record(legacy_mut)
    assert rec_mut.stream_name == "legacy_stream"
    assert rec_mut.directory is None
    assert rec_mut.stream_id is None
    assert rec_mut.source_id is None
    assert isinstance(rec_mut.body, Mutation)


def test_v020_new_record_old_reader_compatibility() -> None:
    # Build an actual v0.1.0 prototype in an isolated descriptor pool by copying
    # the real descriptor and keeping only fields 1-5.
    pool = descriptor_pool.DescriptorPool()
    dp_ts = descriptor_pb2.FileDescriptorProto()
    timestamp_pb2.DESCRIPTOR.CopyToProto(dp_ts)
    pool.Add(dp_ts)

    file_dp = descriptor_pb2.FileDescriptorProto()
    mutations_pb2.DESCRIPTOR.CopyToProto(file_dp)
    for msg in file_dp.message_type:
        if msg.name == "FDBMutationRecord":
            kept = [f for f in msg.field if f.number <= 5]
            del msg.field[:]
            msg.field.extend(kept)
            del msg.oneof_decl[1:]
    pool.Add(file_dp)

    legacy_cls = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("fdbkafka.cdc.v1.FDBMutationRecord")
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


def test_version_end_with_directory_rejected_on_decode() -> None:
    wire = mutations_pb2.FDBMutationRecord(
        stream_name="orders",
        version_end=mutations_pb2.VersionEnd(fdb_version=1000, total_mutations=1),
        directory=mutations_pb2.FDBDirectoryContext(
            path=["tenants", "a"], subspace_prefix=b"\x01"
        ),
    ).SerializeToString()
    with pytest.raises(
        RecordDecodeError, match="VersionEnd record must not carry directory"
    ) as exc_info:
        deserialize_record(wire)
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE


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
    # Production identity when both present:
    # (source_id, stream_id, fdb_version, sequence_no)
    assert deduplication_identity(rec, rec.body) == (
        "source-prod-1",
        88,
        7000,
        5,
    )
    # Legacy fallback: (fdb_version, sequence_no)
    assert legacy_deduplication_identity(rec.body) == (7000, 5)

    # Mismatched presence raises InputValueError:
    rec_no_source = replace(rec, source_id=None)
    with pytest.raises(
        InputValueError,
        match="source_id and stream_id must both be present or both be None",
    ):
        deduplication_identity(rec_no_source, rec.body)

    rec_no_stream = replace(rec, stream_id=None)
    with pytest.raises(
        InputValueError,
        match="source_id and stream_id must both be present or both be None",
    ):
        deduplication_identity(rec_no_stream, rec.body)

    # Fallback when both are None:
    rec_neither = replace(rec, source_id=None, stream_id=None)
    assert deduplication_identity(rec_neither, rec.body) == (7000, 5)

    # stream_id == 0 boundary test: must decode as 0, not None, and preserve 4-tuple
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


def test_directory_context_surrogate_path_rejected() -> None:
    # An unpaired surrogate cannot be encoded to UTF-8
    bad_dir = FDBDirectoryContext(
        path=("tenants", "\ud800"),
        subspace_prefix=b"\x01",
    )
    with pytest.raises(InputValueError, match="not UTF-8-encodable") as exc_info:
        serialize_mutation(
            _FakeNativeMutation(MutationType.SET_VALUE, b"\x01k", b"v"),
            fdb_version=1000,
            sequence_no=1,
            stream_name="test_stream",
            bridge_timestamp_ns=1000,
            directory=bad_dir,
        )
    assert exc_info.value.field == "directory.path"


def test_directory_context_empty_subspace_prefix_rejected() -> None:
    bad_dir = FDBDirectoryContext(
        path=("tenants", "a"),
        subspace_prefix=b"",
    )
    with pytest.raises(
        InputValueError, match="subspace_prefix must not be empty"
    ) as exc_info:
        serialize_mutation(
            _FakeNativeMutation(MutationType.SET_VALUE, b"\x01k", b"v"),
            fdb_version=1000,
            sequence_no=1,
            stream_name="test_stream",
            bridge_timestamp_ns=1000,
            directory=bad_dir,
        )
    assert exc_info.value.field == "directory.subspace_prefix"


def test_directory_context_empty_path_rejected() -> None:
    bad_dir = FDBDirectoryContext(
        path=(),
        subspace_prefix=b"\x01",
    )
    with pytest.raises(InputValueError, match="path must not be empty") as exc_info:
        serialize_mutation(
            _FakeNativeMutation(MutationType.SET_VALUE, b"\x01k", b"v"),
            fdb_version=1000,
            sequence_no=1,
            stream_name="test_stream",
            bridge_timestamp_ns=1000,
            directory=bad_dir,
        )
    assert exc_info.value.field == "directory.path"


def test_directory_context_list_path_normalised() -> None:
    dir_list = FDBDirectoryContext(
        path=["tenants", "a"],  # type: ignore[arg-type]
        subspace_prefix=b"\x01",
    )
    dir_tuple = FDBDirectoryContext(
        path=("tenants", "a"),
        subspace_prefix=b"\x01",
    )
    assert dir_list.path == ("tenants", "a")
    assert dir_list == dir_tuple

    # Batching loop sees them as equal and does not split
    mutations = [
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01k1", b"v1"),
        _FakeNativeMutation(MutationType.SET_VALUE, b"\x01k2", b"v2"),
    ]
    dirs = [dir_list, dir_tuple]

    def resolver(mut: _FakeNativeMutation) -> FDBDirectoryContext:
        idx = mutations.index(mut)
        return dirs[idx]

    records = serialize_version_group(
        mutations,
        fdb_version=2000,
        max_record_bytes=100_000,
        stream_name="test_stream",
        bridge_timestamp_ns=1000,
        directory_resolver=resolver,
    )
    # One batch record + one VersionEnd record
    assert len(records) == 2
    rec = deserialize_record(records[0])
    assert isinstance(rec.body, MutationBatch)
    assert len(rec.body.mutations) == 2


def test_read_side_directory_verification() -> None:
    # Wire with key not starting with subspace_prefix fails closed
    wire_bad_key = mutations_pb2.FDBMutationRecord(
        stream_name="test_stream",
        directory=mutations_pb2.FDBDirectoryContext(
            path=["tenants", "a"], subspace_prefix=b"\x01"
        ),
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                mutation_type=0, key=b"\x02foreign_key", value=b"val"
            ),
        ),
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as exc_info:
        deserialize_record(wire_bad_key)
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE
    assert "does not start with directory subspace prefix" in str(exc_info.value)

    # Wire with clear range outside subspace fails closed
    wire_bad_clear = mutations_pb2.FDBMutationRecord(
        stream_name="test_stream",
        directory=mutations_pb2.FDBDirectoryContext(
            path=["tenants", "a"], subspace_prefix=b"\x01"
        ),
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0),
            clear_range=mutations_pb2.FDBClearRange(
                begin_key=b"\x01begin", end_key=b"\x02end"
            ),
        ),
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as exc_info:
        deserialize_record(wire_bad_clear)
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE
    assert "falls outside directory subspace prefix" in str(exc_info.value)

    # Wire with empty subspace_prefix fails closed
    wire_empty_prefix = mutations_pb2.FDBMutationRecord(
        stream_name="test_stream",
        directory=mutations_pb2.FDBDirectoryContext(
            path=["tenants", "a"], subspace_prefix=b""
        ),
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                mutation_type=0, key=b"key", value=b"val"
            ),
        ),
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as exc_info:
        deserialize_record(wire_empty_prefix)
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE
    assert "empty path or subspace prefix" in str(exc_info.value)

    # Wire with empty path fails closed
    wire_empty_path = mutations_pb2.FDBMutationRecord(
        stream_name="test_stream",
        directory=mutations_pb2.FDBDirectoryContext(path=[], subspace_prefix=b"\x01"),
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                mutation_type=0, key=b"\x01key", value=b"val"
            ),
        ),
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as exc_info:
        deserialize_record(wire_empty_path)
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE
    assert "empty path or subspace prefix" in str(exc_info.value)
