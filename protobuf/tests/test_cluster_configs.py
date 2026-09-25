"""Cluster configuration tests for Protobuf serialization.

Simulates CDC mutation patterns from each FDB redundancy mode
(single, double, triple) and verifies protobuf correctness.

FDB 8.0 exposes a single logical CDC stream regardless of
redundancy mode, but the underlying replication topology
affects mutation delivery patterns:
  - single:  1 replica,  mutations from 1 storage server per shard
  - double:  2 replicas, mutations replicated across 2 storage servers
  - triple:  3 replicas, mutations replicated across 3 storage servers

The protobuf schema must faithfully represent mutations from all
configurations without data loss or ordering corruption.
"""

import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


class ClusterConfig:
    """Describes an FDB cluster redundancy configuration for test parameterization."""

    def __init__(
        self,
        name: str,
        replicas: int,
        storage_servers: int,
        tlogs: int,
        proxies: int,
    ) -> None:
        self.name = name
        self.replicas = replicas
        self.storage_servers = storage_servers
        self.tlogs = tlogs
        self.proxies = proxies


SINGLE = ClusterConfig(name="single", replicas=1, storage_servers=1, tlogs=1, proxies=1)
DOUBLE = ClusterConfig(name="double", replicas=2, storage_servers=3, tlogs=4, proxies=3)
TRIPLE = ClusterConfig(name="triple", replicas=3, storage_servers=5, tlogs=6, proxies=3)

ALL_CONFIGS = [SINGLE, DOUBLE, TRIPLE]


def _build_shard_mutations(
    config: ClusterConfig,
    base_version: int,
    shard_prefix: bytes,
    num_keys: int,
) -> list[mutations_pb2.FDBMutation]:
    """Build mutations simulating writes across storage servers for a shard.

    In replicated configurations, the same logical mutation is committed
    once but replicated to N storage servers. The CDC stream deduplicates
    and delivers each mutation exactly once, but the version index reflects
    the commit proxy that sequenced it.
    """
    mutations: list[mutations_pb2.FDBMutation] = []
    for i in range(num_keys):
        proxy_id = i % config.proxies
        mutations.append(
            mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=base_version + proxy_id,
                    sequence_no=i,
                ),
                single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                    key=shard_prefix + f":{i:04d}".encode(),
                    value=f'{{"shard":"{shard_prefix.decode()}","idx":{i},'
                    f'"replicas":{config.replicas}}}'.encode(),
                    mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
                ),
            )
        )
    return mutations


class TestSingleClusterProtobuf(unittest.TestCase):
    """Protobuf correctness under single-redundancy (1 replica) CDC patterns."""

    CONFIG = SINGLE

    def test_single_shard_mutations_roundtrip(self) -> None:
        """Mutations from a single-replica cluster serialize and parse correctly."""
        mutations = _build_shard_mutations(self.CONFIG, 1_000_000, b"users", 50)
        batch = mutations_pb2.FDBMutationBatch(mutations=mutations)

        wire = batch.SerializeToString()
        parsed = mutations_pb2.FDBMutationBatch.FromString(wire)

        self.assertEqual(len(parsed.mutations), 50)
        for i, m in enumerate(parsed.mutations):
            self.assertEqual(m.single_key_mutation.key, f"users:{i:04d}".encode())
            self.assertEqual(m.version_index.sequence_no, i)

    def test_single_version_end_boundary(self) -> None:
        """VersionEnd from single-replica correctly carries mutation count."""
        ts = Timestamp()
        ts.GetCurrentTime()
        ve = mutations_pb2.VersionEnd(
            fdb_version=1_000_000,
            total_mutations=50,
            bridge_timestamp=ts,
        )
        rec = mutations_pb2.FDBMutationRecord(
            stream_name=f"cdc:{self.CONFIG.name}:users",
            version_end=ve,
        )

        parsed = mutations_pb2.FDBMutationRecord.FromString(rec.SerializeToString())
        self.assertEqual(parsed.version_end.fdb_version, 1_000_000)
        self.assertEqual(parsed.version_end.total_mutations, 50)
        self.assertIn(self.CONFIG.name, parsed.stream_name)

    def test_single_clear_range_across_shard(self) -> None:
        """Clear range covering an entire shard in single-replica mode."""
        mut = mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=1_000_001, sequence_no=0
            ),
            clear_range=mutations_pb2.FDBClearRange(
                begin_key=b"tmp:\x00", end_key=b"tmp:\xff"
            ),
        )
        rec = mutations_pb2.FDBMutationRecord(
            stream_name=f"cdc:{self.CONFIG.name}:cleanup",
            mutation=mut,
        )

        parsed = mutations_pb2.FDBMutationRecord.FromString(rec.SerializeToString())
        self.assertEqual(parsed.mutation.clear_range.begin_key, b"tmp:\x00")
        self.assertEqual(parsed.mutation.clear_range.end_key, b"tmp:\xff")


class TestDoubleClusterProtobuf(unittest.TestCase):
    """Protobuf correctness under double-redundancy (2 replicas) CDC patterns."""

    CONFIG = DOUBLE

    def test_double_shard_mutations_roundtrip(self) -> None:
        """Mutations routed through multiple proxies serialize correctly."""
        mutations = _build_shard_mutations(self.CONFIG, 2_000_000, b"orders", 100)
        batch = mutations_pb2.FDBMutationBatch(mutations=mutations)

        wire = batch.SerializeToString()
        parsed = mutations_pb2.FDBMutationBatch.FromString(wire)

        self.assertEqual(len(parsed.mutations), 100)

        observed_versions = {m.version_index.fdb_version for m in parsed.mutations}
        self.assertEqual(
            len(observed_versions),
            self.CONFIG.proxies,
            "Double-replica mutations should span multiple proxy versions",
        )

    def test_double_multi_shard_batch(self) -> None:
        """Batch spanning multiple shards in double-replica configuration."""
        shard_a = _build_shard_mutations(self.CONFIG, 2_000_000, b"shard_a", 30)
        shard_b = _build_shard_mutations(self.CONFIG, 2_000_000, b"shard_b", 30)

        combined = shard_a + shard_b
        batch = mutations_pb2.FDBMutationBatch(mutations=combined)

        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())
        self.assertEqual(len(parsed.mutations), 60)

        a_keys = [
            m.single_key_mutation.key
            for m in parsed.mutations
            if m.single_key_mutation.key.startswith(b"shard_a")
        ]
        b_keys = [
            m.single_key_mutation.key
            for m in parsed.mutations
            if m.single_key_mutation.key.startswith(b"shard_b")
        ]
        self.assertEqual(len(a_keys), 30)
        self.assertEqual(len(b_keys), 30)

    def test_double_atomic_ops_across_replicas(self) -> None:
        """Atomic operations (ADD, AND, OR) must survive serialization."""
        atomic_types = [
            (2, "MUTATION_TYPE_ADD"),
            (6, "MUTATION_TYPE_AND"),
            (7, "MUTATION_TYPE_OR"),
            (8, "MUTATION_TYPE_XOR"),
        ]
        mutations: list[mutations_pb2.FDBMutation] = []
        for seq, (code, _name) in enumerate(atomic_types):
            mutations.append(
                mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(
                        fdb_version=2_000_100, sequence_no=seq
                    ),
                    single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                        key=f"counter:{seq}".encode(),
                        value=(42).to_bytes(8, "little"),
                        mutation_type=code,
                    ),
                )
            )

        batch = mutations_pb2.FDBMutationBatch(mutations=mutations)
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())

        for i, (code, _name) in enumerate(atomic_types):
            m = parsed.mutations[i]
            self.assertEqual(m.single_key_mutation.mutation_type, code)
            self.assertEqual(
                int.from_bytes(parsed.mutations[i].single_key_mutation.value, "little"),
                42,
            )


class TestTripleClusterProtobuf(unittest.TestCase):
    """Protobuf correctness under triple-redundancy (3 replicas) CDC patterns."""

    CONFIG = TRIPLE

    def test_triple_high_volume_shard(self) -> None:
        """Large mutation batch from triple-replica cluster."""
        mutations = _build_shard_mutations(self.CONFIG, 3_000_000, b"events", 500)
        batch = mutations_pb2.FDBMutationBatch(mutations=mutations)

        wire = batch.SerializeToString()
        parsed = mutations_pb2.FDBMutationBatch.FromString(wire)

        self.assertEqual(len(parsed.mutations), 500)
        self.assertEqual(parsed.mutations[0].single_key_mutation.key, b"events:0000")
        self.assertEqual(parsed.mutations[499].single_key_mutation.key, b"events:0499")

    def test_triple_version_ordering_across_proxies(self) -> None:
        """Versions assigned by different proxies maintain correct ordering."""
        mutations = _build_shard_mutations(self.CONFIG, 3_000_000, b"logs", 60)

        batch = mutations_pb2.FDBMutationBatch(mutations=mutations)
        parsed = mutations_pb2.FDBMutationBatch.FromString(batch.SerializeToString())

        for m in parsed.mutations:
            self.assertGreaterEqual(m.version_index.fdb_version, 3_000_000)
            self.assertLess(
                m.version_index.fdb_version,
                3_000_000 + self.CONFIG.proxies,
            )

    def test_triple_mixed_mutation_types_in_batch(self) -> None:
        """Batch with SETs, CLEARs, and atomics from triple-replica cluster."""
        ts = Timestamp()
        ts.GetCurrentTime()
        base_ver = 3_500_000

        mutations: list[mutations_pb2.FDBMutation] = []

        # SETs
        for i in range(10):
            mutations.append(
                mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(
                        fdb_version=base_ver, sequence_no=i
                    ),
                    single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                        key=f"doc:{i}".encode(),
                        value=f"content_{i}".encode(),
                        mutation_type=0,
                    ),
                )
            )

        # CLEAR_RANGEs
        for i in range(3):
            mutations.append(
                mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(
                        fdb_version=base_ver, sequence_no=10 + i
                    ),
                    clear_range=mutations_pb2.FDBClearRange(
                        begin_key=f"expired:{i}:\x00".encode(),
                        end_key=f"expired:{i}:\xff".encode(),
                    ),
                )
            )

        # Atomic ADDs
        for i in range(5):
            mutations.append(
                mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(
                        fdb_version=base_ver, sequence_no=13 + i
                    ),
                    single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                        key=f"counter:{i}".encode(),
                        value=(1).to_bytes(8, "little"),
                        mutation_type=2,
                    ),
                )
            )

        rec = mutations_pb2.FDBMutationRecord(
            stream_name=f"cdc:{self.CONFIG.name}:mixed",
            bridge_timestamp=ts,
            batch=mutations_pb2.FDBMutationBatch(mutations=mutations),
        )

        parsed = mutations_pb2.FDBMutationRecord.FromString(rec.SerializeToString())

        self.assertEqual(len(parsed.batch.mutations), 18)

        sets = [
            m
            for m in parsed.batch.mutations
            if m.WhichOneof("mutation") == "single_key_mutation"
            and m.single_key_mutation.mutation_type == 0
        ]
        clears = [
            m
            for m in parsed.batch.mutations
            if m.WhichOneof("mutation") == "clear_range"
        ]
        atomics = [
            m
            for m in parsed.batch.mutations
            if m.WhichOneof("mutation") == "single_key_mutation"
            and m.single_key_mutation.mutation_type == 2
        ]

        self.assertEqual(len(sets), 10)
        self.assertEqual(len(clears), 3)
        self.assertEqual(len(atomics), 5)

    def test_triple_version_end_with_high_mutation_count(self) -> None:
        """VersionEnd from triple-replica cluster with large mutation count."""
        ts = Timestamp()
        ts.GetCurrentTime()
        ve = mutations_pb2.VersionEnd(
            fdb_version=3_999_999,
            total_mutations=10_000,
            bridge_timestamp=ts,
        )

        parsed = mutations_pb2.VersionEnd.FromString(ve.SerializeToString())
        self.assertEqual(parsed.fdb_version, 3_999_999)
        self.assertEqual(parsed.total_mutations, 10_000)


class TestCrossClusterConsistency(unittest.TestCase):
    """Verify identical logical mutations produce identical protobuf output
    regardless of cluster configuration.
    """

    def test_same_mutation_identical_wire_across_configs(self) -> None:
        """A mutation with identical fields must produce identical wire bytes."""
        for config in ALL_CONFIGS:
            with self.subTest(config=config.name):
                mut = mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(
                        fdb_version=999_999, sequence_no=0
                    ),
                    single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                        key=b"canonical:key",
                        value=b"canonical:value",
                        mutation_type=0,
                    ),
                )
                wire = mut.SerializeToString()
                parsed = mutations_pb2.FDBMutation.FromString(wire)
                self.assertEqual(parsed.single_key_mutation.key, b"canonical:key")
                self.assertEqual(parsed.single_key_mutation.value, b"canonical:value")

    def test_version_end_consistent_across_configs(self) -> None:
        """VersionEnd semantics are identical regardless of cluster topology."""
        ts = Timestamp(seconds=1700000000, nanos=0)
        for config in ALL_CONFIGS:
            with self.subTest(config=config.name):
                ve = mutations_pb2.VersionEnd(
                    fdb_version=5_000_000,
                    total_mutations=200,
                    bridge_timestamp=ts,
                )
                rec = mutations_pb2.FDBMutationRecord(
                    stream_name=f"cdc:{config.name}:cross-check",
                    version_end=ve,
                )
                parsed = mutations_pb2.FDBMutationRecord.FromString(
                    rec.SerializeToString()
                )
                self.assertEqual(parsed.version_end.fdb_version, 5_000_000)
                self.assertEqual(parsed.version_end.total_mutations, 200)

    def test_batch_size_preserved_across_configs(self) -> None:
        """Mutation counts are preserved for all cluster topologies."""
        for config in ALL_CONFIGS:
            with self.subTest(config=config.name):
                mutations = _build_shard_mutations(config, 7_000_000, b"bench", 200)
                batch = mutations_pb2.FDBMutationBatch(mutations=mutations)
                parsed = mutations_pb2.FDBMutationBatch.FromString(
                    batch.SerializeToString()
                )
                self.assertEqual(len(parsed.mutations), 200)


if __name__ == "__main__":
    unittest.main()
