"""
Unit tests for fdbkafka.cdc.v1.mutations_pb2

Covers every message type defined in proto/fdbkafka/cdc/v1/mutations.proto:
  - FDBVersionIndex
  - VersionEnd
  - FDBSingleKeyMutation (all MutationType enum values matching FDB's CDC enum)
  - FDBClearRange
  - FDBMutation (oneof: single_key_mutation | clear_range)
  - FDBMutationBatch
  - FDBMutationRecord (oneof: mutation | version_end | batch)

Run from the container:
    PYTHONPATH=protobuf/gen python3 -m pytest protobuf/tests/test_mutations_proto.py -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gen"))

from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


class TestFDBVersionIndex(unittest.TestCase):
    """Tests for the FDBVersionIndex message."""

    def test_create_and_serialize(self) -> None:
        vi = mutations_pb2.FDBVersionIndex(fdb_version=102450000, sequence_no=7)
        self.assertEqual(vi.fdb_version, 102450000)
        self.assertEqual(vi.sequence_no, 7)

        data = vi.SerializeToString()
        parsed = mutations_pb2.FDBVersionIndex.FromString(data)
        self.assertEqual(parsed.fdb_version, 102450000)
        self.assertEqual(parsed.sequence_no, 7)

    def test_defaults_are_zero(self) -> None:
        vi = mutations_pb2.FDBVersionIndex()
        self.assertEqual(vi.fdb_version, 0)
        self.assertEqual(vi.sequence_no, 0)


class TestVersionEnd(unittest.TestCase):
    """Tests for the VersionEnd message."""

    def test_roundtrip(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()

        ve = mutations_pb2.VersionEnd(
            fdb_version=999999,
            total_mutations=42,
            bridge_timestamp=ts,
        )
        data = ve.SerializeToString()
        parsed = mutations_pb2.VersionEnd.FromString(data)

        self.assertEqual(parsed.fdb_version, 999999)
        self.assertEqual(parsed.total_mutations, 42)
        self.assertEqual(parsed.bridge_timestamp.seconds, ts.seconds)

    def test_optional_fields_default(self) -> None:
        ve = mutations_pb2.VersionEnd(fdb_version=1)
        self.assertEqual(ve.total_mutations, 0)
        self.assertFalse(ve.HasField("bridge_timestamp"))


class TestFDBSingleKeyMutation(unittest.TestCase):
    """Tests for FDBSingleKeyMutation."""

    EXPECTED_ENUM_VALUES = {
        "MUTATION_TYPE_SET_VALUE": 0,
        "MUTATION_TYPE_CLEAR_RANGE": 1,
        "MUTATION_TYPE_ADD": 2,
        "MUTATION_TYPE_AND": 6,
        "MUTATION_TYPE_OR": 7,
        "MUTATION_TYPE_XOR": 8,
        "MUTATION_TYPE_APPEND_IF_FITS": 9,
        "MUTATION_TYPE_MAX": 12,
        "MUTATION_TYPE_MIN": 13,
        "MUTATION_TYPE_SET_VERSIONSTAMPED_KEY": 14,
        "MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE": 15,
        "MUTATION_TYPE_BYTE_MIN": 16,
        "MUTATION_TYPE_BYTE_MAX": 17,
        "MUTATION_TYPE_MIN_V2": 18,
        "MUTATION_TYPE_AND_V2": 19,
        "MUTATION_TYPE_COMPARE_AND_CLEAR": 20,
    }

    def test_mutation_type_enum_values_match_fdb_cdc(self) -> None:
        """Verify each MutationType enum matches the exact FDB CDC integer value."""
        mutation_type = mutations_pb2.FDBSingleKeyMutation.MutationType
        for name, expected_val in self.EXPECTED_ENUM_VALUES.items():
            with self.subTest(enum_name=name):
                actual_val = mutation_type.Value(name)
                self.assertEqual(
                    actual_val,
                    expected_val,
                    f"{name} should have value {expected_val}, got {actual_val}",
                )

    def test_set_value_roundtrip(self) -> None:
        mutation = mutations_pb2.FDBSingleKeyMutation()
        mutation.key = b"test_key"
        mutation.value = b"test_value"
        mutation.mutation_type = (
            mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
        )

        serialized = mutation.SerializeToString()
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(serialized)

        self.assertEqual(parsed.key, b"test_key")
        self.assertEqual(parsed.value, b"test_value")
        self.assertEqual(
            parsed.mutation_type,
            mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )

    def test_all_mutation_types(self) -> None:
        """Verify every MutationType enum value survives serialization."""
        mutation_type = mutations_pb2.FDBSingleKeyMutation.MutationType
        for name, value in mutation_type.items():
            with self.subTest(mutation_type=name):
                m = mutations_pb2.FDBSingleKeyMutation(
                    key=b"k", value=b"v", mutation_type=value
                )
                parsed = mutations_pb2.FDBSingleKeyMutation.FromString(
                    m.SerializeToString()
                )
                self.assertEqual(parsed.mutation_type, value)

    def test_empty_key_and_value(self) -> None:
        m = mutations_pb2.FDBSingleKeyMutation(key=b"", value=b"")
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(parsed.key, b"")
        self.assertEqual(parsed.value, b"")

    def test_large_value(self) -> None:
        """Keys/values up to 100 KB (FDB limit is ~100 KB for values)."""
        big_value = b"\xab" * 100_000
        m = mutations_pb2.FDBSingleKeyMutation(key=b"big", value=big_value)
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(len(parsed.value), 100_000)
        self.assertEqual(parsed.value, big_value)


class TestFDBClearRange(unittest.TestCase):
    """Tests for the FDBClearRange message."""

    def test_roundtrip(self) -> None:
        cr = mutations_pb2.FDBClearRange(
            begin_key=b"users:0000",
            end_key=b"users:\xff",
        )
        parsed = mutations_pb2.FDBClearRange.FromString(cr.SerializeToString())
        self.assertEqual(parsed.begin_key, b"users:0000")
        self.assertEqual(parsed.end_key, b"users:\xff")

    def test_single_key_clear(self) -> None:
        """Clear range with begin == end is empty but still valid proto."""
        cr = mutations_pb2.FDBClearRange(begin_key=b"k", end_key=b"k")
        parsed = mutations_pb2.FDBClearRange.FromString(cr.SerializeToString())
        self.assertEqual(parsed.begin_key, parsed.end_key)


class TestFDBMutation(unittest.TestCase):
    """Tests for the FDBMutation oneof wrapper."""

    def _make_version_index(
        self, ver: int = 100, seq: int = 0
    ) -> mutations_pb2.FDBVersionIndex:
        return mutations_pb2.FDBVersionIndex(fdb_version=ver, sequence_no=seq)

    def test_single_key_mutation_variant(self) -> None:
        single = mutations_pb2.FDBSingleKeyMutation(
            key=b"k1",
            value=b"v1",
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        mut = mutations_pb2.FDBMutation(
            version_index=self._make_version_index(200, 1),
            single_key_mutation=single,
        )
        parsed = mutations_pb2.FDBMutation.FromString(mut.SerializeToString())

        self.assertEqual(parsed.WhichOneof("mutation"), "single_key_mutation")
        self.assertEqual(parsed.version_index.fdb_version, 200)
        self.assertEqual(parsed.single_key_mutation.key, b"k1")

    def test_clear_range_variant(self) -> None:
        cr = mutations_pb2.FDBClearRange(begin_key=b"a", end_key=b"z")
        mut = mutations_pb2.FDBMutation(
            version_index=self._make_version_index(300, 2),
            clear_range=cr,
        )
        parsed = mutations_pb2.FDBMutation.FromString(mut.SerializeToString())

        self.assertEqual(parsed.WhichOneof("mutation"), "clear_range")
        self.assertEqual(parsed.clear_range.begin_key, b"a")
        self.assertEqual(parsed.clear_range.end_key, b"z")

    def test_oneof_mutual_exclusion(self) -> None:
        """Setting clear_range should clear single_key_mutation and vice-versa."""
        mut = mutations_pb2.FDBMutation()
        mut.single_key_mutation.key = b"k"
        self.assertEqual(mut.WhichOneof("mutation"), "single_key_mutation")

        mut.clear_range.begin_key = b"a"
        self.assertEqual(mut.WhichOneof("mutation"), "clear_range")
        # single_key_mutation is now cleared by the oneof
        self.assertEqual(mut.single_key_mutation.key, b"")


class TestFDBMutationBatch(unittest.TestCase):
    """Tests for high-throughput batching."""

    def test_batch_with_multiple_mutations(self) -> None:
        mutations = []
        for i in range(100):
            single = mutations_pb2.FDBSingleKeyMutation(
                key=f"key:{i}".encode(),
                value=f"val:{i}".encode(),
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            )
            mut = mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=1000, sequence_no=i
                ),
                single_key_mutation=single,
            )
            mutations.append(mut)

        batch = mutations_pb2.FDBMutationBatch(
            mutations=mutations,
        )

        data = batch.SerializeToString()
        parsed = mutations_pb2.FDBMutationBatch.FromString(data)

        self.assertEqual(len(parsed.mutations), 100)
        self.assertEqual(parsed.mutations[0].single_key_mutation.key, b"key:0")
        self.assertEqual(parsed.mutations[99].single_key_mutation.key, b"key:99")
        self.assertEqual(parsed.mutations[50].version_index.sequence_no, 50)

    def test_empty_batch(self) -> None:
        batch = mutations_pb2.FDBMutationBatch()
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())
        self.assertEqual(len(parsed.mutations), 0)

    def test_mixed_mutation_types_in_batch(self) -> None:
        """Batch containing both single_key_mutation and clear_range entries."""
        set_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=500, sequence_no=0),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=b"write_key",
                value=b"data",
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        )
        clear_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=500, sequence_no=1),
            clear_range=mutations_pb2.FDBClearRange(
                begin_key=b"old:", end_key=b"old:\xff"
            ),
        )

        batch = mutations_pb2.FDBMutationBatch(
            mutations=[set_mut, clear_mut],
        )
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())

        self.assertEqual(
            parsed.mutations[0].WhichOneof("mutation"), "single_key_mutation"
        )
        self.assertEqual(parsed.mutations[1].WhichOneof("mutation"), "clear_range")


class TestFDBMutationRecord(unittest.TestCase):
    """Tests for the top-level Kafka record envelope."""

    def _make_timestamp(self) -> Timestamp:
        ts = Timestamp()
        ts.GetCurrentTime()
        return ts

    def test_full_kafka_envelope_roundtrip(self) -> None:
        single_mut = mutations_pb2.FDBSingleKeyMutation(
            key=b"users:1001",
            value=b'{"name": "Alice"}',
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        fdb_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=102450000,
                sequence_no=0,
            ),
            single_key_mutation=single_mut,
        )
        ts = self._make_timestamp()
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-user-stream",
            bridge_timestamp=ts,
            mutation=fdb_mut,
        )

        kafka_payload = record.SerializeToString()
        received = mutations_pb2.FDBMutationRecord.FromString(kafka_payload)

        self.assertEqual(received.stream_name, "cdc-user-stream")
        self.assertEqual(received.WhichOneof("record"), "mutation")
        self.assertEqual(received.mutation.version_index.fdb_version, 102450000)
        self.assertEqual(received.mutation.single_key_mutation.key, b"users:1001")
        self.assertEqual(
            received.mutation.single_key_mutation.value, b'{"name": "Alice"}'
        )
        self.assertEqual(received.bridge_timestamp.seconds, ts.seconds)
        self.assertGreater(len(kafka_payload), 0)

    def test_version_end_record(self) -> None:
        """FDBMutationRecord carrying a VersionEnd payload."""
        ve = mutations_pb2.VersionEnd(
            fdb_version=102450000,
            total_mutations=15,
            bridge_timestamp=self._make_timestamp(),
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-user-stream",
            bridge_timestamp=self._make_timestamp(),
            version_end=ve,
        )
        parsed = mutations_pb2.FDBMutationRecord.FromString(record.SerializeToString())

        self.assertEqual(parsed.WhichOneof("record"), "version_end")
        self.assertEqual(parsed.version_end.fdb_version, 102450000)
        self.assertEqual(parsed.version_end.total_mutations, 15)

    def test_batch_record(self) -> None:
        """FDBMutationRecord carrying a FDBMutationBatch payload."""
        mutations = [
            mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=5000, sequence_no=i
                ),
                single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                    key=f"batch_key:{i}".encode(),
                    value=b"v",
                    mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
                ),
            )
            for i in range(10)
        ]
        batch = mutations_pb2.FDBMutationBatch(
            mutations=mutations,
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-batch-stream",
            bridge_timestamp=self._make_timestamp(),
            batch=batch,
        )
        parsed = mutations_pb2.FDBMutationRecord.FromString(record.SerializeToString())

        self.assertEqual(parsed.WhichOneof("record"), "batch")
        self.assertEqual(len(parsed.batch.mutations), 10)

    def test_record_oneof_mutual_exclusion(self) -> None:
        """Only one of mutation / version_end / batch can be set at a time."""
        record = mutations_pb2.FDBMutationRecord(stream_name="test")

        record.mutation.single_key_mutation.key = b"k"
        self.assertEqual(record.WhichOneof("record"), "mutation")
        record.version_end.fdb_version = 1
        self.assertEqual(record.WhichOneof("record"), "version_end")
        record.batch.mutations.add()
        self.assertEqual(record.WhichOneof("record"), "batch")

    def test_payload_size_is_compact(self) -> None:
        """Protobuf binary encoding should be significantly smaller than JSON."""
        single_mut = mutations_pb2.FDBSingleKeyMutation(
            key=b"users:1001",
            value=b'{"name": "Alice"}',
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        fdb_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=102450000, sequence_no=0
            ),
            single_key_mutation=single_mut,
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-user-stream",
            bridge_timestamp=self._make_timestamp(),
            mutation=fdb_mut,
        )
        payload = record.SerializeToString()

        self.assertLess(len(payload), 200, "Protobuf payload should be compact")
        self.assertGreater(len(payload), 50, "Payload should contain real data")


class TestCorruptedInput(unittest.TestCase):
    """Bridge must handle corrupt Kafka messages gracefully."""

    def test_corrupted_bytes_raises(self) -> None:
        """Corrupted wire format with invalid wire type must raise DecodeError."""
        invalid_wire_type = b"\x0f\x01\x02\x03"
        with self.assertRaises(DecodeError):
            mutations_pb2.FDBMutationRecord.FromString(invalid_wire_type)

    def test_truncated_bytes_raises(self) -> None:
        """Truncated length-delimited payload must raise DecodeError."""
        record = mutations_pb2.FDBMutationRecord(
            stream_name="truncation-test-with-long-name",
        )
        record.mutation.single_key_mutation.key = b"important_key_for_truncation"
        full_payload = record.SerializeToString()

        truncated = full_payload[: len(full_payload) - 5]
        with self.assertRaises(DecodeError):
            mutations_pb2.FDBMutationRecord.FromString(truncated)

    def test_completely_empty_bytes(self) -> None:
        """Empty bytes should produce a default (empty) message."""
        parsed = mutations_pb2.FDBMutationRecord.FromString(b"")
        self.assertEqual(parsed.stream_name, "")
        self.assertIsNone(parsed.WhichOneof("record"))

    def test_random_bytes_do_not_crash(self) -> None:
        """Parser must never segfault or unhandled-crash on random input."""
        import random

        for _ in range(50):
            noise = bytes(random.randint(0, 255) for _ in range(random.randint(1, 500)))
            try:
                mutations_pb2.FDBMutationRecord.FromString(noise)
            except DecodeError:
                pass


class TestForwardCompatibility(unittest.TestCase):
    """Old consumers must silently ignore new fields — Protobuf forward compat."""

    def test_unknown_fields_preserved(self) -> None:
        record = mutations_pb2.FDBMutationRecord(stream_name="compat-test")
        record.mutation.single_key_mutation.key = b"known_key"
        original_bytes = record.SerializeToString()

        future_bytes = original_bytes + b"\xf8\x06\x2a"
        parsed = mutations_pb2.FDBMutationRecord.FromString(future_bytes)
        reserialized = parsed.SerializeToString()
        self.assertIn(b"\xf8\x06\x2a", reserialized)


class TestEmptyStreamNameValidation(unittest.TestCase):
    """Validate behavior when stream_name is empty."""

    def test_empty_stream_name_is_valid_proto(self) -> None:
        record = mutations_pb2.FDBMutationRecord(stream_name="")
        data = record.SerializeToString()
        parsed = mutations_pb2.FDBMutationRecord.FromString(data)

        self.assertEqual(parsed.stream_name, "")

    def test_whitespace_only_stream_name(self) -> None:
        record = mutations_pb2.FDBMutationRecord(stream_name="   ")
        parsed = mutations_pb2.FDBMutationRecord.FromString(record.SerializeToString())
        self.assertEqual(parsed.stream_name, "   ")


class TestVersionOrdering(unittest.TestCase):
    """Version ordering semantics for the CDC bridge."""

    def test_version_index_ordering_within_version(self) -> None:
        v1 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0)
        v2 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=1)
        v3 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=2)

        self.assertEqual(v1.fdb_version, v2.fdb_version)
        self.assertLess(v1.sequence_no, v2.sequence_no)
        self.assertLess(v2.sequence_no, v3.sequence_no)

    def test_version_index_ordering_across_versions(self) -> None:
        v1 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0)
        v2 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=1)
        v3 = mutations_pb2.FDBVersionIndex(fdb_version=101, sequence_no=0)

        self.assertLess(v1.sequence_no, v2.sequence_no)
        self.assertLess(v2.fdb_version, v3.fdb_version)

    def test_version_index_tuple_comparison(self) -> None:
        tuples = [(100, 2), (50, 0), (100, 0), (200, 1), (100, 1)]
        sorted_tuples = sorted(tuples)
        self.assertEqual(
            sorted_tuples,
            [(50, 0), (100, 0), (100, 1), (100, 2), (200, 1)],
        )


class TestStressAndEdgeCases(unittest.TestCase):
    """Stress tests and boundary condition validation."""

    def test_large_batch_serialization(self) -> None:
        """Serialize a batch of 1,000 mutations and measure roundtrip correctness."""
        mutations = [
            mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=100_000, sequence_no=i
                ),
                single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                    key=f"user:{i:06d}:profile".encode(),
                    value=f'{{"id": {i}, "status": "active"}}'.encode(),
                    mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
                ),
            )
            for i in range(1000)
        ]
        record = mutations_pb2.FDBMutationRecord(
            stream_name="large-batch-test",
            batch=mutations_pb2.FDBMutationBatch(mutations=mutations),
        )

        serialized = record.SerializeToString()
        parsed = mutations_pb2.FDBMutationRecord.FromString(serialized)

        self.assertEqual(len(parsed.batch.mutations), 1000)
        self.assertEqual(
            parsed.batch.mutations[500].single_key_mutation.key,
            b"user:000500:profile",
        )

    def test_max_uint64_version(self) -> None:
        """FDB versions are 64-bit integers — test boundary values."""
        max_u64 = 2**64 - 1
        vi = mutations_pb2.FDBVersionIndex(fdb_version=max_u64, sequence_no=2**32 - 1)
        data = vi.SerializeToString()
        parsed = mutations_pb2.FDBVersionIndex.FromString(data)
        self.assertEqual(parsed.fdb_version, max_u64)
        self.assertEqual(parsed.sequence_no, 2**32 - 1)

    def test_binary_safety_in_keys_and_values(self) -> None:
        """FDB keys and values can contain arbitrary bytes including nulls."""
        arbitrary_bytes = bytes(range(256))
        m = mutations_pb2.FDBSingleKeyMutation(
            key=arbitrary_bytes,
            value=arbitrary_bytes,
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(parsed.key, arbitrary_bytes)
        self.assertEqual(parsed.value, arbitrary_bytes)


class TestThreadSafety(unittest.TestCase):
    """Protobuf message creation and serialization should be thread-safe."""

    def test_concurrent_serialization(self) -> None:
        import threading

        errors = []

        def serialize_many(thread_id: int) -> None:
            for i in range(200):
                try:
                    m = mutations_pb2.FDBSingleKeyMutation(
                        key=f"t{thread_id}:k{i}".encode(),
                        value=f"v{i}".encode(),
                        mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
                    )
                    data = m.SerializeToString()
                    parsed = mutations_pb2.FDBSingleKeyMutation.FromString(data)
                    assert parsed.key == f"t{thread_id}:k{i}".encode()
                    assert parsed.value == f"v{i}".encode()
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=serialize_many, args=(tid,)) for tid in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Thread safety violations: {errors}")

    def test_concurrent_record_building(self) -> None:
        """Multiple threads building full FDBMutationRecord envelopes concurrently."""
        import threading

        errors = []

        def build_records(thread_id: int) -> None:
            for i in range(500):
                try:
                    ts = Timestamp()
                    ts.GetCurrentTime()
                    record = mutations_pb2.FDBMutationRecord(
                        stream_name=f"stream-{thread_id}",
                        bridge_timestamp=ts,
                        mutation=mutations_pb2.FDBMutation(
                            version_index=mutations_pb2.FDBVersionIndex(
                                fdb_version=thread_id * 10000 + i,
                                sequence_no=0,
                            ),
                            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                                key=f"t{thread_id}:k{i}".encode(),
                                value=b"data",
                            ),
                        ),
                    )
                    payload = record.SerializeToString()
                    parsed = mutations_pb2.FDBMutationRecord.FromString(payload)
                    assert parsed.stream_name == f"stream-{thread_id}"
                    assert (
                        parsed.mutation.version_index.fdb_version
                        == thread_id * 10000 + i
                    )
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=build_records, args=(tid,)) for tid in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Concurrent record errors: {errors}")


if __name__ == "__main__":
    unittest.main()
