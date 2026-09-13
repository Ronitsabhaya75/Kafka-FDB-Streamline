"""
Unit tests for fdb.kafka.cdc.v1.mutations_pb2

Covers every message type defined in proto/fdb/kafka/cdc/v1/mutations.proto:
  - FDBVersionIndex
  - VersionEnd
  - FDBSingleKeyMutation (all MutationType enum values)
  - FDBClearRange
  - FDBMutation (oneof: single_key_mutation | clear_range)
  - FDBMutationBatch
  - FDBMutationRecord (oneof: mutation | version_end | batch)

Run from the container:
    PYTHONPATH=src/gen python3 -m pytest tests/proto_buf_tests/unit-test/test_mutations_proto.py -v
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src", "gen"))

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.message import DecodeError
from fdb.kafka.cdc.v1 import mutations_pb2


class TestFDBVersionIndex(unittest.TestCase):
    """Tests for the FDBVersionIndex message."""

    def test_create_and_serialize(self):
        vi = mutations_pb2.FDBVersionIndex(fdb_version=102450000, sequence_no=7)
        self.assertEqual(vi.fdb_version, 102450000)
        self.assertEqual(vi.sequence_no, 7)

        data = vi.SerializeToString()
        parsed = mutations_pb2.FDBVersionIndex.FromString(data)
        self.assertEqual(parsed.fdb_version, 102450000)
        self.assertEqual(parsed.sequence_no, 7)

    def test_defaults_are_zero(self):
        vi = mutations_pb2.FDBVersionIndex()
        self.assertEqual(vi.fdb_version, 0)
        self.assertEqual(vi.sequence_no, 0)


class TestVersionEnd(unittest.TestCase):
    """Tests for the VersionEnd message."""

    def test_roundtrip(self):
        ts = Timestamp()
        ts.GetCurrentTime()

        ve = mutations_pb2.VersionEnd(
            fdb_version=999999,
            total_mutations=42,
            commit_timestamp=ts,
        )
        data = ve.SerializeToString()
        parsed = mutations_pb2.VersionEnd.FromString(data)

        self.assertEqual(parsed.fdb_version, 999999)
        self.assertEqual(parsed.total_mutations, 42)
        self.assertEqual(parsed.commit_timestamp.seconds, ts.seconds)

    def test_optional_fields_default(self):
        ve = mutations_pb2.VersionEnd(fdb_version=1)
        self.assertEqual(ve.total_mutations, 0)
        self.assertFalse(ve.HasField("commit_timestamp"))


class TestFDBSingleKeyMutation(unittest.TestCase):
    """Tests for FDBSingleKeyMutation — the first test run manually in Docker."""

    def test_set_value_roundtrip(self):
        """Reproduces the initial manual test: create, serialize, deserialize."""
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

    def test_all_mutation_types(self):
        """Verify every MutationType enum value survives serialization."""
        MutationType = mutations_pb2.FDBSingleKeyMutation.MutationType
        for name, value in MutationType.items():
            with self.subTest(mutation_type=name):
                m = mutations_pb2.FDBSingleKeyMutation(
                    key=b"k", value=b"v", mutation_type=value
                )
                parsed = mutations_pb2.FDBSingleKeyMutation.FromString(
                    m.SerializeToString()
                )
                self.assertEqual(parsed.mutation_type, value)

    def test_empty_key_and_value(self):
        m = mutations_pb2.FDBSingleKeyMutation(key=b"", value=b"")
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(
            m.SerializeToString()
        )
        self.assertEqual(parsed.key, b"")
        self.assertEqual(parsed.value, b"")

    def test_large_value(self):
        """Keys/values up to 100 KB (FDB limit is ~100 KB for values)."""
        big_value = b"\xAB" * 100_000
        m = mutations_pb2.FDBSingleKeyMutation(key=b"big", value=big_value)
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(
            m.SerializeToString()
        )
        self.assertEqual(len(parsed.value), 100_000)
        self.assertEqual(parsed.value, big_value)


class TestFDBClearRange(unittest.TestCase):
    """Tests for the FDBClearRange message."""

    def test_roundtrip(self):
        cr = mutations_pb2.FDBClearRange(
            begin_key=b"users:0000",
            end_key=b"users:\xff",
        )
        parsed = mutations_pb2.FDBClearRange.FromString(cr.SerializeToString())
        self.assertEqual(parsed.begin_key, b"users:0000")
        self.assertEqual(parsed.end_key, b"users:\xff")

    def test_single_key_clear(self):
        """Clear range where begin == end is technically empty, but still valid proto."""
        cr = mutations_pb2.FDBClearRange(begin_key=b"k", end_key=b"k")
        parsed = mutations_pb2.FDBClearRange.FromString(cr.SerializeToString())
        self.assertEqual(parsed.begin_key, parsed.end_key)


class TestFDBMutation(unittest.TestCase):
    """Tests for the FDBMutation oneof wrapper."""

    def _make_version_index(self, ver=100, seq=0):
        return mutations_pb2.FDBVersionIndex(fdb_version=ver, sequence_no=seq)

    def test_single_key_mutation_variant(self):
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

    def test_clear_range_variant(self):
        cr = mutations_pb2.FDBClearRange(begin_key=b"a", end_key=b"z")
        mut = mutations_pb2.FDBMutation(
            version_index=self._make_version_index(300, 2),
            clear_range=cr,
        )
        parsed = mutations_pb2.FDBMutation.FromString(mut.SerializeToString())

        self.assertEqual(parsed.WhichOneof("mutation"), "clear_range")
        self.assertEqual(parsed.clear_range.begin_key, b"a")
        self.assertEqual(parsed.clear_range.end_key, b"z")

    def test_oneof_mutual_exclusion(self):
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

    def test_batch_with_multiple_mutations(self):
        mutations = []
        for i in range(100):
            single = mutations_pb2.FDBSingleKeyMutation(
                key=f"key:{i}".encode(),
                value=f"val:{i}".encode(),
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            )
            mut = mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(fdb_version=1000, sequence_no=i),
                single_key_mutation=single,
            )
            mutations.append(mut)

        batch = mutations_pb2.FDBMutationBatch(
            mutations=mutations,
            stream_name="high-throughput-stream",
        )

        data = batch.SerializeToString()
        parsed = mutations_pb2.FDBMutationBatch.FromString(data)

        self.assertEqual(len(parsed.mutations), 100)
        self.assertEqual(parsed.stream_name, "high-throughput-stream")
        self.assertEqual(parsed.mutations[0].single_key_mutation.key, b"key:0")
        self.assertEqual(parsed.mutations[99].single_key_mutation.key, b"key:99")
        self.assertEqual(parsed.mutations[50].version_index.sequence_no, 50)

    def test_empty_batch(self):
        batch = mutations_pb2.FDBMutationBatch()
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())
        self.assertEqual(len(parsed.mutations), 0)
        self.assertEqual(parsed.stream_name, "")

    def test_mixed_mutation_types_in_batch(self):
        """Batch containing both single_key_mutation and clear_range entries."""
        set_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=500, sequence_no=0),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=b"write_key", value=b"data",
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        )
        clear_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=500, sequence_no=1),
            clear_range=mutations_pb2.FDBClearRange(begin_key=b"old:", end_key=b"old:\xff"),
        )

        batch = mutations_pb2.FDBMutationBatch(
            mutations=[set_mut, clear_mut],
            stream_name="mixed-stream",
        )
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())

        self.assertEqual(parsed.mutations[0].WhichOneof("mutation"), "single_key_mutation")
        self.assertEqual(parsed.mutations[1].WhichOneof("mutation"), "clear_range")


class TestFDBMutationRecord(unittest.TestCase):
    """Tests for the top-level Kafka record envelope — the full pipeline test from Docker."""

    def _make_timestamp(self):
        ts = Timestamp()
        ts.GetCurrentTime()
        return ts

    def test_full_kafka_envelope_roundtrip(self):
        """
        Reproduces the full end-to-end test that was run successfully in Docker:
          FDBSingleKeyMutation -> FDBMutation -> FDBMutationRecord -> serialize -> parse
        """
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
            timestamp=ts,
            mutation=fdb_mut,
        )

        kafka_payload = record.SerializeToString()
        received = mutations_pb2.FDBMutationRecord.FromString(kafka_payload)

        self.assertEqual(received.stream_name, "cdc-user-stream")
        self.assertEqual(received.WhichOneof("record"), "mutation")
        self.assertEqual(received.mutation.version_index.fdb_version, 102450000)
        self.assertEqual(received.mutation.single_key_mutation.key, b"users:1001")
        self.assertEqual(received.mutation.single_key_mutation.value, b'{"name": "Alice"}')
        self.assertGreater(len(kafka_payload), 0)

    def test_version_end_record(self):
        """FDBMutationRecord carrying a VersionEnd payload."""
        ve = mutations_pb2.VersionEnd(
            fdb_version=102450000,
            total_mutations=15,
            commit_timestamp=self._make_timestamp(),
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-user-stream",
            timestamp=self._make_timestamp(),
            version_end=ve,
        )
        parsed = mutations_pb2.FDBMutationRecord.FromString(record.SerializeToString())

        self.assertEqual(parsed.WhichOneof("record"), "version_end")
        self.assertEqual(parsed.version_end.fdb_version, 102450000)
        self.assertEqual(parsed.version_end.total_mutations, 15)

    def test_batch_record(self):
        """FDBMutationRecord carrying a FDBMutationBatch payload."""
        mutations = [
            mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(fdb_version=5000, sequence_no=i),
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
            stream_name="batch-inner-stream",
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-batch-stream",
            timestamp=self._make_timestamp(),
            batch=batch,
        )
        parsed = mutations_pb2.FDBMutationRecord.FromString(record.SerializeToString())

        self.assertEqual(parsed.WhichOneof("record"), "batch")
        self.assertEqual(len(parsed.batch.mutations), 10)
        self.assertEqual(parsed.batch.stream_name, "batch-inner-stream")

    def test_record_oneof_mutual_exclusion(self):
        """Only one of mutation / version_end / batch can be set at a time."""
        record = mutations_pb2.FDBMutationRecord(stream_name="test")

        record.mutation.single_key_mutation.key = b"k"
        self.assertEqual(record.WhichOneof("record"), "mutation")
        record.version_end.fdb_version = 1
        self.assertEqual(record.WhichOneof("record"), "version_end")
        record.batch.stream_name = "b"
        self.assertEqual(record.WhichOneof("record"), "batch")

    def test_payload_size_is_compact(self):
        """Protobuf binary encoding should be significantly smaller than JSON."""
        single_mut = mutations_pb2.FDBSingleKeyMutation(
            key=b"users:1001",
            value=b'{"name": "Alice"}',
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        fdb_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=102450000, sequence_no=0),
            single_key_mutation=single_mut,
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-user-stream",
            timestamp=self._make_timestamp(),
            mutation=fdb_mut,
        )
        payload = record.SerializeToString()

        self.assertLess(len(payload), 200, "Protobuf payload should be compact")
        self.assertGreater(len(payload), 50, "Payload should contain real data")


class TestCorruptedInput(unittest.TestCase):
    """Bridge must handle corrupt Kafka messages gracefully."""

    def test_corrupted_bytes_raises(self):
        """Garbage bytes must not silently produce a valid message."""
        garbage = b"corrupted garbage bytes \x00\xff\xfe\xfd"
        try:
            parsed = mutations_pb2.FDBMutationRecord.FromString(garbage)
            self.assertIsNone(parsed.WhichOneof("record"))
        except DecodeError:
            pass

    def test_truncated_bytes_raises(self):
        """Truncated payload must raise or produce incomplete data."""
        record = mutations_pb2.FDBMutationRecord(
            stream_name="truncation-test",
        )
        record.mutation.single_key_mutation.key = b"important_key"
        full_payload = record.SerializeToString()
        truncated = full_payload[: len(full_payload) // 2]
        try:
            parsed = mutations_pb2.FDBMutationRecord.FromString(truncated)
            self.assertNotEqual(
                parsed.mutation.single_key_mutation.key,
                b"important_key",
            )
        except DecodeError:
            pass

    def test_completely_empty_bytes(self):
        """Empty bytes should produce a default (empty) message."""
        parsed = mutations_pb2.FDBMutationRecord.FromString(b"")
        self.assertEqual(parsed.stream_name, "")
        self.assertIsNone(parsed.WhichOneof("record"))

    def test_random_bytes_do_not_crash(self):
        """Parser must never segfault or crash on random input."""
        import random
        for _ in range(50):
            noise = bytes(random.randint(0, 255) for _ in range(random.randint(1, 500)))
            try:
                mutations_pb2.FDBMutationRecord.FromString(noise)
            except DecodeError:
                pass

class TestForwardCompatibility(unittest.TestCase):
    """Old consumers must silently ignore new fields — Protobuf forward compat."""

    def test_unknown_fields_preserved(self):
        """
        Simulate a future schema adding an extra field by manually appending
        a varint tag+value to a serialized message. An older consumer
        (our current generated code) should still parse the known fields
        correctly and preserve or ignore the unknown bytes.
        """
        record = mutations_pb2.FDBMutationRecord(stream_name="compat-test")
        record.mutation.single_key_mutation.key = b"known_key"
        original_bytes = record.SerializeToString()
        future_bytes = original_bytes + b"\xf8\x06\x2a"
        parsed = mutations_pb2.FDBMutationRecord.FromString(future_bytes)
        self.assertEqual(parsed.stream_name, "compat-test")
        self.assertEqual(parsed.mutation.single_key_mutation.key, b"known_key")

    def test_reserialize_preserves_unknown_fields(self):
        """Unknown fields should survive a parse → serialize round-trip."""
        record = mutations_pb2.FDBMutationRecord(stream_name="roundtrip")
        original_bytes = record.SerializeToString()
        future_bytes = original_bytes + b"\xf8\x06\x2a"
        parsed = mutations_pb2.FDBMutationRecord.FromString(future_bytes)
        reserialized = parsed.SerializeToString()
        self.assertIn(b"\xf8\x06\x2a", reserialized)


class TestEmptyStreamNameValidation(unittest.TestCase):
    """Validate behavior when stream_name is empty."""

    def test_empty_stream_name_is_valid_proto(self):
        """
        Protobuf allows empty strings (it's the default for string fields).
        The bridge layer should reject empty stream_name at the application
        level, not at the serialization level.
        """
        record = mutations_pb2.FDBMutationRecord(stream_name="")
        data = record.SerializeToString()
        parsed = mutations_pb2.FDBMutationRecord.FromString(data)

        self.assertEqual(parsed.stream_name, "")

    def test_whitespace_only_stream_name(self):
        """Whitespace-only names are technically valid proto but bad practice."""
        record = mutations_pb2.FDBMutationRecord(stream_name="   ")
        parsed = mutations_pb2.FDBMutationRecord.FromString(
            record.SerializeToString()
        )
        self.assertEqual(parsed.stream_name, "   ")

    def test_batch_empty_stream_name(self):
        """FDBMutationBatch.stream_name is optional — empty is the default."""
        batch = mutations_pb2.FDBMutationBatch()
        self.assertEqual(batch.stream_name, "")


class TestVersionOrdering(unittest.TestCase):
    """Version ordering semantics for the CDC bridge."""

    def test_version_index_ordering_within_version(self):
        """sequence_no differentiates mutations within the same fdb_version."""
        v1 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0)
        v2 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=1)
        v3 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=2)

        self.assertEqual(v1.fdb_version, v2.fdb_version)
        self.assertLess(v1.sequence_no, v2.sequence_no)
        self.assertLess(v2.sequence_no, v3.sequence_no)

    def test_version_index_ordering_across_versions(self):
        """fdb_version must be monotonically increasing across commits."""
        v1 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0)
        v2 = mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=1)
        v3 = mutations_pb2.FDBVersionIndex(fdb_version=101, sequence_no=0)

        # v1 < v2 (same version, higher sequence)
        self.assertLess(v1.sequence_no, v2.sequence_no)
        # v2 < v3 (higher version resets sequence)
        self.assertLess(v2.fdb_version, v3.fdb_version)

    def test_version_index_tuple_comparison(self):
        """(fdb_version, sequence_no) tuples should sort correctly."""
        indices = [
            mutations_pb2.FDBVersionIndex(fdb_version=200, sequence_no=3),
            mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=0),
            mutations_pb2.FDBVersionIndex(fdb_version=100, sequence_no=1),
            mutations_pb2.FDBVersionIndex(fdb_version=200, sequence_no=0),
        ]
        sorted_tuples = sorted(
            [(vi.fdb_version, vi.sequence_no) for vi in indices]
        )
        expected = [(100, 0), (100, 1), (200, 0), (200, 3)]
        self.assertEqual(sorted_tuples, expected)

    def test_version_end_signals_boundary(self):
        """VersionEnd.fdb_version should match the last mutation's version."""
        last_mutation_version = 102450000
        ve = mutations_pb2.VersionEnd(
            fdb_version=last_mutation_version,
            total_mutations=15,
        )
        self.assertEqual(ve.fdb_version, last_mutation_version)


class TestBatchSizeBoundary(unittest.TestCase):
    """Kafka default max message size is 1MB — test near boundary."""

    def test_large_batch_near_1mb(self):
        """
        1000 mutations × ~1KB values ≈ 1MB — approaches Kafka's
        default max.request.size. Bridge should be aware of this limit.
        """
        mutations = []
        for i in range(1000):
            single = mutations_pb2.FDBSingleKeyMutation(
                key=f"key:{i:04d}".encode(),
                value=b"\x42" * 1000,
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            )
            mut = mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(fdb_version=9999, sequence_no=i),
                single_key_mutation=single,
            )
            mutations.append(mut)

        batch = mutations_pb2.FDBMutationBatch(
            mutations=mutations,
            stream_name="large-batch-test",
        )
        payload = batch.SerializeToString()

        parsed = mutations_pb2.FDBMutationBatch.FromString(payload)
        self.assertEqual(len(parsed.mutations), 1000)

        payload_kb = len(payload) / 1024
        payload_mb = payload_kb / 1024
        print(f"\n  [INFO] 1000-mutation batch payload: {payload_kb:.1f} KB ({payload_mb:.2f} MB)")

        self.assertGreater(len(payload), 500_000, "Batch should be substantial")

    def test_single_mutation_with_max_value(self):
        """FDB max value size is 100KB — test a single mutation at that limit."""
        big_value = b"\xAB" * 100_000
        single = mutations_pb2.FDBSingleKeyMutation(
            key=b"max_value_key",
            value=big_value,
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        record = mutations_pb2.FDBMutationRecord(
            stream_name="big-value-stream",
            mutation=mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(fdb_version=1, sequence_no=0),
                single_key_mutation=single,
            ),
        )
        payload = record.SerializeToString()
        parsed = mutations_pb2.FDBMutationRecord.FromString(payload)

        self.assertEqual(len(parsed.mutation.single_key_mutation.value), 100_000)
        print(f"\n  [INFO] 100KB value payload: {len(payload) / 1024:.1f} KB")


class TestConcurrentSerialization(unittest.TestCase):
    """Thread safety — critical for the CDC bridge which processes in parallel."""

    def test_concurrent_serialization_no_corruption(self):
        """Multiple threads serializing simultaneously must not corrupt."""
        import threading

        errors = []

        def serialize_many(thread_id):
            for i in range(1000):
                try:
                    m = mutations_pb2.FDBSingleKeyMutation(
                        key=f"t{thread_id}:k{i}".encode(),
                        value=f"v{i}".encode(),
                        mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
                    )
                    data = m.SerializeToString()
                    parsed = mutations_pb2.FDBSingleKeyMutation.FromString(data)
                    # Verify integrity
                    assert parsed.key == f"t{thread_id}:k{i}".encode()
                    assert parsed.value == f"v{i}".encode()
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=serialize_many, args=(tid,))
            for tid in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Thread safety violations: {errors}")

    def test_concurrent_record_building(self):
        """Multiple threads building full FDBMutationRecord envelopes concurrently."""
        import threading

        errors = []

        def build_records(thread_id):
            for i in range(500):
                try:
                    ts = Timestamp()
                    ts.GetCurrentTime()
                    record = mutations_pb2.FDBMutationRecord(
                        stream_name=f"stream-{thread_id}",
                        timestamp=ts,
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
                    assert parsed.mutation.version_index.fdb_version == thread_id * 10000 + i
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=build_records, args=(tid,))
            for tid in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Concurrent record errors: {errors}")


if __name__ == "__main__":
    unittest.main()
