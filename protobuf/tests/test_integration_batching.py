"""Integration and batching tests covering high-throughput containers.

Tests mixed mutations, batching mechanisms, and concurrency.
"""

import json
import threading
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


class TestHighThroughputBatching(unittest.TestCase):
    """Tests for FDBMutationBatch high-volume containers."""

    def test_batch_100_mutations(self) -> None:
        mutations = []
        for i in range(100):
            single = mutations_pb2.FDBSingleKeyMutation(
                key=f"user:{i:04d}".encode(),
                value=f"value_{i}".encode(),
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            )
            mut = mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=1000, sequence_no=i
                ),
                single_key_mutation=single,
            )
            mutations.append(mut)

        batch = mutations_pb2.FDBMutationBatch(mutations=mutations)
        serialized = batch.SerializeToString()
        parsed = mutations_pb2.FDBMutationBatch.FromString(serialized)

        self.assertEqual(len(parsed.mutations), 100)
        self.assertEqual(parsed.mutations[0].single_key_mutation.key, b"user:0000")
        self.assertEqual(parsed.mutations[99].single_key_mutation.key, b"user:0099")
        self.assertEqual(parsed.mutations[50].version_index.sequence_no, 50)

    def test_large_batch_1000_mutations(self) -> None:
        mutations = [
            mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=500_000, sequence_no=i
                ),
                single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                    key=f"doc:{i:06d}".encode(),
                    value=f'{{"doc_id": {i}, "status": "indexed"}}'.encode(),
                    mutation_type=(
                        mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE
                    ),
                ),
            )
            for i in range(1000)
        ]
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-high-throughput-topic",
            batch=mutations_pb2.FDBMutationBatch(mutations=mutations),
        )

        serialized = record.SerializeToString()
        parsed = mutations_pb2.FDBMutationRecord.FromString(serialized)

        self.assertEqual(parsed.WhichOneof("record"), "batch")
        self.assertEqual(len(parsed.batch.mutations), 1000)
        self.assertEqual(
            parsed.batch.mutations[500].single_key_mutation.key, b"doc:000500"
        )


class TestMixedMutationBatches(unittest.TestCase):
    """Test batches containing both writes and range clear deletes."""

    def test_interleaved_writes_and_clears(self) -> None:
        set_mut1 = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=200, sequence_no=0),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=b"k1",
                value=b"v1",
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        )
        clear_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=200, sequence_no=1),
            clear_range=mutations_pb2.FDBClearRange(
                begin_key=b"old:a", end_key=b"old:z"
            ),
        )
        set_mut2 = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=200, sequence_no=2),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=b"k2",
                value=b"v2",
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_APPEND_IF_FITS,
            ),
        )

        batch = mutations_pb2.FDBMutationBatch(
            mutations=[set_mut1, clear_mut, set_mut2]
        )
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())

        self.assertEqual(len(parsed.mutations), 3)
        self.assertEqual(
            parsed.mutations[0].WhichOneof("mutation"), "single_key_mutation"
        )
        self.assertEqual(parsed.mutations[1].WhichOneof("mutation"), "clear_range")
        self.assertEqual(
            parsed.mutations[2].WhichOneof("mutation"), "single_key_mutation"
        )
        self.assertEqual(parsed.mutations[1].clear_range.begin_key, b"old:a")
        self.assertEqual(parsed.mutations[1].clear_range.end_key, b"old:z")


class TestEnvelopeIntegration(unittest.TestCase):
    """Test top-level Kafka envelope handling different payload variants."""

    def test_record_oneof_mutual_exclusion(self) -> None:
        record = mutations_pb2.FDBMutationRecord(stream_name="test-stream")

        record.mutation.single_key_mutation.key = b"k"
        self.assertEqual(record.WhichOneof("record"), "mutation")

        record.version_end.fdb_version = 100
        self.assertEqual(record.WhichOneof("record"), "version_end")

        record.batch.mutations.add()
        self.assertEqual(record.WhichOneof("record"), "batch")

    def test_wire_compactness_vs_json(self) -> None:
        """Protobuf wire representation must be significantly more compact than JSON."""
        single = mutations_pb2.FDBSingleKeyMutation(
            key=b"users:1001:profile",
            value=b'{"name": "Alice", "role": "admin"}',
            mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
        )
        fdb_mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=102450000, sequence_no=0
            ),
            single_key_mutation=single,
        )
        ts = Timestamp()
        ts.GetCurrentTime()
        record = mutations_pb2.FDBMutationRecord(
            stream_name="cdc-users-stream",
            bridge_timestamp=ts,
            mutation=fdb_mut,
        )

        proto_bytes = record.SerializeToString()

        json_repr = {
            "stream_name": "cdc-users-stream",
            "bridge_timestamp": {"seconds": ts.seconds, "nanos": ts.nanos},
            "mutation": {
                "version_index": {"fdb_version": 102450000, "sequence_no": 0},
                "single_key_mutation": {
                    "key": "users:1001:profile",
                    "value": '{"name": "Alice", "role": "admin"}',
                    "mutation_type": "MUTATION_TYPE_SET_VALUE",
                },
            },
        }
        json_bytes = json.dumps(json_repr).encode()

        self.assertLess(len(proto_bytes), len(json_bytes))
        self.assertLess(len(proto_bytes), 150)


class TestThreadSafety(unittest.TestCase):
    """Protobuf message construction and serialization must be safe across threads."""

    def test_concurrent_batch_serialization(self) -> None:
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            for i in range(150):
                try:
                    m = mutations_pb2.FDBSingleKeyMutation(
                        key=f"t{thread_id}:k{i}".encode(),
                        value=f"v{i}".encode(),
                        mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
                    )
                    data = m.SerializeToString()
                    parsed = mutations_pb2.FDBSingleKeyMutation.FromString(data)
                    assert parsed.key == f"t{thread_id}:k{i}".encode()
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=worker, args=(tid,)) for tid in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Concurrent serialization errors: {errors}")


if __name__ == "__main__":
    unittest.main()
