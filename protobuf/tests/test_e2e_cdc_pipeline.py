"""End-to-End (E2E) pipeline tests.

Simulates the full CDC -> Bridge -> Kafka -> Consumer lifecycle.
"""

import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


class TestEndToEndCDCPipeline(unittest.TestCase):
    """Simulates realistic lifecycle from FDB CDC capture to downstream consumer."""

    def _bridge_encode_cdc_mutation(
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
        """
        Simulate complete flow:
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
        encoded_mutations: list[mutations_pb2.FDBMutation] = []
        commit_version = 1050000
        for seq, (mtype, p1, p2) in enumerate(raw_fdb_mutations):
            encoded_mutations.append(
                self._bridge_encode_cdc_mutation(commit_version, seq, mtype, p1, p2)
            )

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
        self.assertEqual(len(consumed_record.batch.mutations), len(raw_fdb_mutations))

        # Validate each mutation payload preserved exactly
        m0 = consumed_record.batch.mutations[0]
        self.assertEqual(m0.single_key_mutation.key, b"acc:1001:balance")
        self.assertEqual(m0.single_key_mutation.mutation_type, 0)

        m2 = consumed_record.batch.mutations[2]
        self.assertEqual(m2.WhichOneof("mutation"), "clear_range")
        self.assertEqual(m2.clear_range.begin_key, b"acc:temporary:0000")
        self.assertEqual(m2.clear_range.end_key, b"acc:temporary:\xff")

        # 5. Bridge emits VersionEnd to signal commit boundary
        version_end_envelope = mutations_pb2.FDBMutationRecord(
            stream_name=stream_name,
            bridge_timestamp=ts,
            version_end=mutations_pb2.VersionEnd(
                fdb_version=commit_version,
                total_mutations=len(raw_fdb_mutations),
                bridge_timestamp=ts,
            ),
        )

        ve_bytes = version_end_envelope.SerializeToString()
        consumed_ve_record = mutations_pb2.FDBMutationRecord.FromString(ve_bytes)

        self.assertEqual(consumed_ve_record.WhichOneof("record"), "version_end")
        self.assertEqual(consumed_ve_record.version_end.fdb_version, commit_version)
        self.assertEqual(consumed_ve_record.version_end.total_mutations, 4)

        # Audit: count in VersionEnd matches received mutations
        self.assertEqual(
            consumed_ve_record.version_end.total_mutations,
            len(consumed_record.batch.mutations),
        )

    def test_multi_version_watermark_stream(self) -> None:
        """Test stream progression across multiple commit versions."""
        stream_name = "fdb-cdc:stream-ordering"
        versions = [100_000, 100_050, 100_100, 100_250]

        last_observed_version = 0

        for ver in versions:
            mut = self._bridge_encode_cdc_mutation(ver, 0, 0, b"key", b"val")
            rec = mutations_pb2.FDBMutationRecord(
                stream_name=stream_name,
                mutation=mut,
            )

            # Transport
            wire = rec.SerializeToString()
            parsed = mutations_pb2.FDBMutationRecord.FromString(wire)

            current_version = parsed.mutation.version_index.fdb_version
            self.assertGreater(current_version, last_observed_version)
            last_observed_version = current_version


if __name__ == "__main__":
    unittest.main()
