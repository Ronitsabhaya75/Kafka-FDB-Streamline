"""Unit tests for fdbkafka.bridge.state.v1.state_pb2.

Covers every message type defined in proto/fdbkafka/bridge/state/v1/state.proto:
  - KafkaSinkIdentity
  - CDCCheckpoint
  - LeaderLease
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gen"))

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.bridge.state.v1 import state_pb2


class TestKafkaSinkIdentity(unittest.TestCase):
    """Tests for KafkaSinkIdentity message."""

    def test_roundtrip(self) -> None:
        sink = state_pb2.KafkaSinkIdentity(
            cluster_id="kafka-prod-cluster-01",
            topic="fdb-mutations-cdc",
        )
        self.assertEqual(sink.cluster_id, "kafka-prod-cluster-01")
        self.assertEqual(sink.topic, "fdb-mutations-cdc")

        data = sink.SerializeToString()
        parsed = state_pb2.KafkaSinkIdentity.FromString(data)
        self.assertEqual(parsed.cluster_id, "kafka-prod-cluster-01")
        self.assertEqual(parsed.topic, "fdb-mutations-cdc")


class TestCDCCheckpoint(unittest.TestCase):
    """Tests for CDCCheckpoint message."""

    def test_roundtrip(self) -> None:
        ts = Timestamp(seconds=1700000000, nanos=123456789)
        sink = state_pb2.KafkaSinkIdentity(
            cluster_id="kafka-prod-01",
            topic="orders-cdc",
        )
        cp = state_pb2.CDCCheckpoint(
            format_version=1,
            source_id="fdb-cluster-primary",
            stream_name="orders_stream",
            stream_id=987654321,
            last_consumed_version=150000000,
            sink=sink,
            updating_instance_id="bridge-worker-pod-42",
            update_timestamp=ts,
            lease_epoch=7,
            range_config_fingerprint="sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4",
        )

        data = cp.SerializeToString()
        parsed = state_pb2.CDCCheckpoint.FromString(data)
        self.assertEqual(parsed.format_version, 1)
        self.assertEqual(parsed.source_id, "fdb-cluster-primary")
        self.assertEqual(parsed.stream_name, "orders_stream")
        self.assertEqual(parsed.stream_id, 987654321)
        self.assertEqual(parsed.last_consumed_version, 150000000)
        self.assertEqual(parsed.sink.cluster_id, "kafka-prod-01")
        self.assertEqual(parsed.sink.topic, "orders-cdc")
        self.assertEqual(parsed.updating_instance_id, "bridge-worker-pod-42")
        self.assertEqual(parsed.update_timestamp.seconds, 1700000000)
        self.assertEqual(parsed.update_timestamp.nanos, 123456789)
        self.assertEqual(parsed.lease_epoch, 7)
        self.assertEqual(
            parsed.range_config_fingerprint,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4",
        )


class TestLeaderLease(unittest.TestCase):
    """Tests for LeaderLease message."""

    def test_roundtrip(self) -> None:
        ts = Timestamp(seconds=1700000100, nanos=500000000)
        lease = state_pb2.LeaderLease(
            source_id="fdb-cluster-primary",
            stream_name="orders_stream",
            owner_instance_id="bridge-worker-pod-42",
            lease_epoch=12,
            heartbeat_sequence=345,
            expiration_timestamp=ts,
        )

        data = lease.SerializeToString()
        parsed = state_pb2.LeaderLease.FromString(data)
        self.assertEqual(parsed.source_id, "fdb-cluster-primary")
        self.assertEqual(parsed.stream_name, "orders_stream")
        self.assertEqual(parsed.owner_instance_id, "bridge-worker-pod-42")
        self.assertEqual(parsed.lease_epoch, 12)
        self.assertEqual(parsed.heartbeat_sequence, 345)
        self.assertEqual(parsed.expiration_timestamp.seconds, 1700000100)
        self.assertEqual(parsed.expiration_timestamp.nanos, 500000000)
