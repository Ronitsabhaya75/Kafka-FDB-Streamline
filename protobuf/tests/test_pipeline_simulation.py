"""Pipeline simulation tests.

Simulates the logical CDC -> Bridge -> Kafka -> Consumer serialization lifecycle
using Protobuf envelopes. Live FDB CDC consumer integration is tested in
tests/test_cdc_e2e.py.
"""

import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


class TestCDCPipelineSimulation(unittest.TestCase):
    """Simulates the lifecycle from CDC mutation capture to downstream consumer."""

    def _encode_cdc_mutation(
        self,
        fdb_version: int,
        seq_no: int,
        mut_type: int,
        param1: bytes,
        param2: bytes,
    ) -> mutations_pb2.FDBMutation:
        """Helper simulating the bridge transformation of raw FDB CDC mutation tuple."""
        ver_idx = mutations_pb2.FDBVersionIndex(
            fdb_version=fdb_version, sequence_no=seq_no
        )

        if mut_type == 1:  # FDB_CDC_MUTATION_TYPE_CLEAR_RANGE
            return mutations_pb2.FDBMutation(
                version_index=ver_idx,
                clear_range=mutations_pb2.FDBClearRange(
                    begin_key=param1, end_key=param2
                ),
            )

        return mutations_pb2.FDBMutation(
            version_index=ver_idx,
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=param1,
                value=param2,
                mutation_type=mut_type,
            ),
        )

    def test_full_pipeline_mutation_batch_and_audit(self) -> None:
        """Simulate complete flow:
        1. FDB commits multiple transactions under version 1050000.
        2. Bridge converts raw mutations into FDBMutationBatch envelope.
        3. Serializes to binary Kafka message.
        4. Consumer deserializes and verifies data integrity.
        5. Bridge emits VersionEnd boundary; consumer validates audit count.
        """
        stream_name = "fdb-cdc:accounts-directory"

        # 1. Raw mutations from FDB CDC stream
        raw_fdb_mutations = [
            (0, b"acc:1001:balance", (5000).to_bytes(8, "little")),  # SET_VALUE
            (2, b"acc:1001:balance", (100).to_bytes(8, "little")),  # ADD
            (1, b"acc:temporary:0000", b"acc:temporary:\xff"),  # CLEAR_RANGE
            (9, b"acc:1001:ledger", b"deposit:+$100\n"),  # APPEND_IF_FITS
        ]

        # 2. Bridge encodes mutations into an FDBMutationBatch
        commit_version = 1050000
        encoded_mutations = [
            self._encode_cdc_mutation(commit_version, seq, mtype, p1, p2)
            for seq, (mtype, p1, p2) in enumerate(raw_fdb_mutations)
        ]

        ts = Timestamp()
        ts.GetCurrentTime()

        data_record = mutations_pb2.FDBMutationRecord(
            stream_name=stream_name,
            bridge_timestamp=ts,
            batch=mutations_pb2.FDBMutationBatch(mutations=encoded_mutations),
        )

        # 3. Simulate Kafka transport: publish raw bytes
        kafka_payload_bytes = data_record.SerializeToString()
        self.assertGreater(len(kafka_payload_bytes), 0)

        # 4. Downstream consumer receives and parses Kafka record
        consumed_record = mutations_pb2.FDBMutationRecord.FromString(
            kafka_payload_bytes
        )

        self.assertEqual(consumed_record.stream_name, stream_name)
        self.assertEqual(consumed_record.WhichOneof("record"), "batch")

        # Accumulate consumed mutations
        received_mutations = list(consumed_record.batch.mutations)
        self.assertEqual(len(received_mutations), 4)

        # Validate mutation payloads
        m0 = received_mutations[0]
        self.assertEqual(m0.single_key_mutation.key, b"acc:1001:balance")
        self.assertEqual(m0.single_key_mutation.mutation_type, 0)

        m2 = received_mutations[2]
        self.assertEqual(m2.WhichOneof("mutation"), "clear_range")
        self.assertEqual(m2.clear_range.begin_key, b"acc:temporary:0000")
        self.assertEqual(m2.clear_range.end_key, b"acc:temporary:\xff")

        # 5. Bridge emits VersionEnd to signal commit boundary
        version_end_envelope = mutations_pb2.FDBMutationRecord(
            stream_name=stream_name,
            bridge_timestamp=ts,
            version_end=mutations_pb2.VersionEnd(
                fdb_version=commit_version,
                total_mutations=len(encoded_mutations),
                bridge_timestamp=ts,
            ),
        )

        ve_bytes = version_end_envelope.SerializeToString()
        consumed_ve_record = mutations_pb2.FDBMutationRecord.FromString(ve_bytes)

        self.assertEqual(consumed_ve_record.WhichOneof("record"), "version_end")
        self.assertEqual(consumed_ve_record.version_end.fdb_version, commit_version)

        # Audit: count reported in VersionEnd matches actual received mutation count
        self.assertEqual(
            consumed_ve_record.version_end.total_mutations,
            len(received_mutations),
        )

    def test_multi_version_watermark_stream(self) -> None:
        """Test consumer tracks commit watermarks and rejects out-of-order versions."""
        stream_name = "fdb-cdc:stream-ordering"

        def consume_version(record_bytes: bytes, current_watermark: int) -> int:
            parsed = mutations_pb2.FDBMutationRecord.FromString(record_bytes)
            ver = parsed.mutation.version_index.fdb_version
            if ver <= current_watermark:
                msg = f"Out-of-order version {ver}; watermark is {current_watermark}"
                raise ValueError(msg)
            return ver

        watermark = 0
        for ver in [100_000, 100_050, 100_100, 100_250]:
            mut = self._encode_cdc_mutation(ver, 0, 0, b"key", b"val")
            rec = mutations_pb2.FDBMutationRecord(stream_name=stream_name, mutation=mut)
            watermark = consume_version(rec.SerializeToString(), watermark)

        self.assertEqual(watermark, 100_250)

        # Feed an out-of-order / regressive version and assert it is rejected
        stale_mut = self._encode_cdc_mutation(100_050, 0, 0, b"key", b"val")
        stale_rec = mutations_pb2.FDBMutationRecord(
            stream_name=stream_name, mutation=stale_mut
        )
        with self.assertRaises(ValueError) as ctx:
            consume_version(stale_rec.SerializeToString(), watermark)
        self.assertIn("Out-of-order version 100050", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
