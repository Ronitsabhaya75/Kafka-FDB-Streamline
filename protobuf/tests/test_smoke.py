"""Smoke tests verifying sanity, imports, default values, and enum mappings."""

import sys
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


class TestSmokeSanityAndImports(unittest.TestCase):
    """Sanity checks for package namespace and imports."""

    def test_import_namespace_is_clean(self) -> None:
        """Verify importing fdbkafka does not collide with or create an fdb module."""
        import fdbkafka

        self.assertTrue(hasattr(fdbkafka, "cdc"))
        if "fdb" in sys.modules:
            fdb_mod = sys.modules["fdb"]
            # Ensure fdb is the actual FDB package, not pointing into protobuf/gen
            self.assertNotIn("protobuf/gen/fdb", getattr(fdb_mod, "__file__", ""))

    def test_all_message_classes_exposed(self) -> None:
        """Verify all expected message classes exist on mutations_pb2."""
        expected_classes = (
            "FDBVersionIndex",
            "VersionEnd",
            "FDBSingleKeyMutation",
            "FDBClearRange",
            "FDBMutation",
            "FDBMutationBatch",
            "FDBMutationRecord",
        )
        for class_name in expected_classes:
            self.assertTrue(
                hasattr(mutations_pb2, class_name),
                f"Missing expected class: {class_name}",
            )


class TestSmokeMessageDefaults(unittest.TestCase):
    """Smoke checks for default message field values."""

    def test_version_index_defaults(self) -> None:
        vi = mutations_pb2.FDBVersionIndex()
        self.assertEqual(vi.fdb_version, 0)
        self.assertEqual(vi.sequence_no, 0)

    def test_version_end_defaults(self) -> None:
        ve = mutations_pb2.VersionEnd()
        self.assertEqual(ve.fdb_version, 0)
        self.assertEqual(ve.total_mutations, 0)
        self.assertFalse(ve.HasField("bridge_timestamp"))

    def test_single_key_mutation_defaults(self) -> None:
        m = mutations_pb2.FDBSingleKeyMutation()
        self.assertEqual(m.key, b"")
        self.assertEqual(m.value, b"")
        self.assertEqual(m.mutation_type, 0)

    def test_clear_range_defaults(self) -> None:
        cr = mutations_pb2.FDBClearRange()
        self.assertEqual(cr.begin_key, b"")
        self.assertEqual(cr.end_key, b"")

    def test_mutation_wrapper_defaults(self) -> None:
        mut = mutations_pb2.FDBMutation()
        self.assertFalse(mut.HasField("version_index"))
        self.assertIsNone(mut.WhichOneof("mutation"))

    def test_mutation_batch_defaults(self) -> None:
        batch = mutations_pb2.FDBMutationBatch()
        self.assertEqual(len(batch.mutations), 0)

    def test_mutation_record_defaults(self) -> None:
        rec = mutations_pb2.FDBMutationRecord()
        self.assertEqual(rec.stream_name, "")
        self.assertFalse(rec.HasField("bridge_timestamp"))
        self.assertIsNone(rec.WhichOneof("record"))


class TestSmokeEnumMapping(unittest.TestCase):
    """Smoke test ensuring 1:1 parity with FoundationDB native CDC codes."""

    EXPECTED_ENUM_MAPPINGS = {
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

    def test_all_enum_values_match_fdb(self) -> None:
        enum_type = mutations_pb2.FDBSingleKeyMutation.MutationType
        for name, expected_code in self.EXPECTED_ENUM_MAPPINGS.items():
            with self.subTest(enum_name=name):
                actual = enum_type.Value(name)
                self.assertEqual(
                    actual,
                    expected_code,
                    f"Enum {name} expected {expected_code} but got {actual}",
                )


class TestSmokeSerializationRoundtrips(unittest.TestCase):
    """Quick serialization roundtrip sanity for each message."""

    def test_version_index_roundtrip(self) -> None:
        vi = mutations_pb2.FDBVersionIndex(fdb_version=42000, sequence_no=3)
        parsed = mutations_pb2.FDBVersionIndex.FromString(vi.SerializeToString())
        self.assertEqual(parsed.fdb_version, 42000)
        self.assertEqual(parsed.sequence_no, 3)

    def test_version_end_roundtrip(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        ve = mutations_pb2.VersionEnd(
            fdb_version=50000,
            total_mutations=10,
            bridge_timestamp=ts,
        )
        parsed = mutations_pb2.VersionEnd.FromString(ve.SerializeToString())
        self.assertEqual(parsed.fdb_version, 50000)
        self.assertEqual(parsed.total_mutations, 10)
        self.assertEqual(parsed.bridge_timestamp.seconds, ts.seconds)

    def test_single_key_mutation_roundtrip(self) -> None:
        mut_type = (
            mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
        )
        m = mutations_pb2.FDBSingleKeyMutation(
            key=b"k1",
            value=b"v1",
            mutation_type=mut_type,
        )
        parsed = mutations_pb2.FDBSingleKeyMutation.FromString(m.SerializeToString())
        self.assertEqual(parsed.key, b"k1")
        self.assertEqual(parsed.value, b"v1")
        self.assertEqual(parsed.mutation_type, mut_type)

    def test_clear_range_roundtrip(self) -> None:
        cr = mutations_pb2.FDBClearRange(begin_key=b"a", end_key=b"z")
        parsed = mutations_pb2.FDBClearRange.FromString(cr.SerializeToString())
        self.assertEqual(parsed.begin_key, b"a")
        self.assertEqual(parsed.end_key, b"z")


if __name__ == "__main__":
    unittest.main()
