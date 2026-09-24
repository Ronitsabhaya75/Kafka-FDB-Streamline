"""Validation and edge-case tests for boundary conditions, corruption, and safety."""

import random
import unittest

from google.protobuf.message import DecodeError

from fdbkafka.cdc.v1 import mutations_pb2


class TestIntegerBoundaries(unittest.TestCase):
    """Test boundary values for 64-bit version and 32-bit sequence numbers."""

    def test_max_uint64_version(self) -> None:
        max_u64 = 2**64 - 1
        max_u32 = 2**32 - 1
        vi = mutations_pb2.FDBVersionIndex(fdb_version=max_u64, sequence_no=max_u32)
        parsed = mutations_pb2.FDBVersionIndex.FromString(vi.SerializeToString())
        self.assertEqual(parsed.fdb_version, max_u64)
        self.assertEqual(parsed.sequence_no, max_u32)

    def test_version_zero(self) -> None:
        vi = mutations_pb2.FDBVersionIndex(fdb_version=0, sequence_no=0)
        parsed = mutations_pb2.FDBVersionIndex.FromString(vi.SerializeToString())
        self.assertEqual(parsed.fdb_version, 0)
        self.assertEqual(parsed.sequence_no, 0)

    def test_version_ordering_monotonicity(self) -> None:
        """Verify sorting coordinates for versions across commit boundaries."""
        coords = [(100, 2), (50, 0), (100, 0), (200, 1), (100, 1)]
        sorted_coords = sorted(coords)
        self.assertEqual(
            sorted_coords,
            [(50, 0), (100, 0), (100, 1), (100, 2), (200, 1)],
        )


class TestBinarySafetyAndPayloadSizes(unittest.TestCase):
    """Test binary safety across arbitrary bytes, nulls, and large values."""

    def test_all_256_byte_values(self) -> None:
        """FDB keys and values must accept all 256 byte values without mangling."""
        all_bytes = bytes(range(256))
        mut_type = (
            mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
        )
        m = mutations_pb2.FDBSingleKeyMutation(
            key=all_bytes,
            value=all_bytes,
            mutation_type=mut_type,
        )
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(parsed.key, all_bytes)
        self.assertEqual(parsed.value, all_bytes)

    def test_embedded_null_bytes(self) -> None:
        """Embedded nulls are standard in FDB tuple encoding and must not truncate."""
        key_with_nulls = b"\x01users\x00\x01\x00\x00\x02data"
        val_with_nulls = b"\x00\x00\x00\x00binary\x00payload\x00"
        mut_type = (
            mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
        )
        m = mutations_pb2.FDBSingleKeyMutation(
            key=key_with_nulls,
            value=val_with_nulls,
            mutation_type=mut_type,
        )
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(parsed.key, key_with_nulls)
        self.assertEqual(parsed.value, val_with_nulls)

    def test_empty_key_and_value(self) -> None:
        m = mutations_pb2.FDBSingleKeyMutation(key=b"", value=b"")
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(parsed.key, b"")
        self.assertEqual(parsed.value, b"")

    def test_large_value_limit(self) -> None:
        """FDB values up to 100 KB must serialize and deserialize cleanly."""
        payload_100kb = b"\xfa" * 100_000
        mut_type = (
            mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
        )
        m = mutations_pb2.FDBSingleKeyMutation(
            key=b"large_key",
            value=payload_100kb,
            mutation_type=mut_type,
        )
        data = m.SerializeToString()
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(data)
        self.assertEqual(len(parsed.value), 100_000)
        self.assertEqual(parsed.value, payload_100kb)

    def test_single_key_clear_range(self) -> None:
        """Clear range where begin == end represents empty interval."""
        cr = mutations_pb2.FDBClearRange(begin_key=b"k", end_key=b"k")
        parsed = mutations_pb2.FDBClearRange.FromString(cr.SerializeToString())
        self.assertEqual(parsed.begin_key, parsed.end_key)


class TestWireCorruptionAndErrors(unittest.TestCase):
    """Test parser behavior against invalid wire format and corrupted bytes."""

    def test_invalid_wire_type_raises_decode_error(self) -> None:
        """Field tag with wire type 7 is invalid in proto3; must raise DecodeError."""
        invalid_wire_type = b"\x0f\x01\x02\x03"
        with self.assertRaises(DecodeError):
            mutations_pb2.FDBMutationRecord.FromString(invalid_wire_type)

    def test_truncated_length_delimited_field_raises(self) -> None:
        """Truncating inside a length-delimited submessage must raise DecodeError."""
        record = mutations_pb2.FDBMutationRecord(stream_name="truncation-test-stream")
        record.mutation.single_key_mutation.key = b"very_long_important_key_string"
        full_bytes = record.SerializeToString()
        truncated = full_bytes[: len(full_bytes) - 6]
        with self.assertRaises(DecodeError):
            mutations_pb2.FDBMutationRecord.FromString(truncated)

    def test_truncated_varint_raises_decode_error(self) -> None:
        """Varint with MSB continuation bit set indefinitely must raise DecodeError."""
        corrupted_varint = b"\x08\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01"
        with self.assertRaises(DecodeError):
            mutations_pb2.FDBVersionIndex.FromString(corrupted_varint)

    def test_completely_empty_bytes(self) -> None:
        """Empty byte string decodes cleanly into default message."""
        parsed = mutations_pb2.FDBMutationRecord.FromString(b"")
        self.assertEqual(parsed.stream_name, "")
        self.assertIsNone(parsed.WhichOneof("record"))

    def test_random_byte_noise_robustness(self) -> None:
        """Parser must never crash with unhandled exception on random input."""
        for _ in range(50):
            noise_len = random.randint(1, 256)
            noise = bytes(random.randint(0, 255) for _ in range(noise_len))
            try:
                mutations_pb2.FDBMutationRecord.FromString(noise)
            except DecodeError:
                pass


class TestForwardCompatibility(unittest.TestCase):
    """Test schema evolution: unknown tags are preserved without dropping data."""

    def test_unknown_field_tags_preserved(self) -> None:
        record = mutations_pb2.FDBMutationRecord(stream_name="evolution-test")
        record.mutation.single_key_mutation.key = b"known_key"
        known_bytes = record.SerializeToString()

        # Simulate a future field (field tag 99, varint value 42 -> 0x318, 0x2a)
        future_tag = b"\xf8\x06\x2a"
        future_bytes = known_bytes + future_tag
        parsed = mutations_pb2.FDBMutationRecord.FromString(future_bytes)

        # Existing known fields should still be accessible
        self.assertEqual(parsed.stream_name, "evolution-test")
        self.assertEqual(parsed.mutation.single_key_mutation.key, b"known_key")

        # Re-serializing must preserve unknown fields
        reserialized = parsed.SerializeToString()
        self.assertIn(future_tag, reserialized)


class TestStreamNameEdgeCases(unittest.TestCase):
    """Test stream_name variations."""

    def test_empty_stream_name(self) -> None:
        rec = mutations_pb2.FDBMutationRecord(stream_name="")
        parsed = mutations_pb2.FDBMutationRecord.FromString(rec.SerializeToString())
        self.assertEqual(parsed.stream_name, "")

    def test_whitespace_stream_name(self) -> None:
        rec = mutations_pb2.FDBMutationRecord(stream_name="   \t\n  ")
        parsed = mutations_pb2.FDBMutationRecord.FromString(rec.SerializeToString())
        self.assertEqual(parsed.stream_name, "   \t\n  ")

    def test_long_stream_name(self) -> None:
        long_name = "dir:" + "subpath/" * 100
        rec = mutations_pb2.FDBMutationRecord(stream_name=long_name)
        parsed = mutations_pb2.FDBMutationRecord.FromString(rec.SerializeToString())
        self.assertEqual(parsed.stream_name, long_name)


if __name__ == "__main__":
    unittest.main()
