"""Tests for state serialization and deserialization (checkpoints and leases)."""

from __future__ import annotations

import pytest

from fdb_kafka_bridge.serialization.errors import (
    DecodeFailure,
    InputTypeError,
    InputValueError,
    RecordDecodeError,
)
from fdb_kafka_bridge.serialization.model import (
    CDCCheckpoint,
    KafkaSinkIdentity,
    LeaderLease,
)
from fdb_kafka_bridge.serialization.state import (
    deserialize_checkpoint,
    deserialize_leader_lease,
    serialize_checkpoint,
    serialize_leader_lease,
)
from fdbkafka.bridge.state.v1 import state_pb2


def test_checkpoint_roundtrip() -> None:
    sink = KafkaSinkIdentity(cluster_id="cluster-prod-01", topic="orders-topic")
    cp = CDCCheckpoint(
        format_version=1,
        source_id="fdb-primary-dc",
        stream_name="orders_stream",
        stream_id=12345678,
        last_consumed_version=99887766,
        sink=sink,
        updating_instance_id="bridge-instance-1",
        update_timestamp_ns=1700000000_123456789,
        lease_epoch=42,
        range_config_fingerprint="sha256:fingerprint_hash_abc123",
    )

    data = serialize_checkpoint(cp)
    assert isinstance(data, bytes)

    decoded = deserialize_checkpoint(data)
    assert decoded == cp


def test_checkpoint_golden_bytes() -> None:
    sink = KafkaSinkIdentity(cluster_id="c1", topic="t1")
    cp = CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="stream1",
        stream_id=100,
        last_consumed_version=200,
        sink=sink,
        updating_instance_id="inst1",
        update_timestamp_ns=1000_000_000,
        lease_epoch=2,
        range_config_fingerprint="fp1",
    )
    wire = serialize_checkpoint(cp)
    # Wire bytes must decode cleanly back to identical model
    restored = deserialize_checkpoint(wire)
    assert restored == cp
    # Direct proto parse check
    msg = state_pb2.CDCCheckpoint.FromString(wire)
    assert msg.format_version == 1
    assert msg.source_id == "s1"
    assert msg.sink.cluster_id == "c1"
    assert msg.sink.topic == "t1"


def test_checkpoint_validation_errors() -> None:
    sink = KafkaSinkIdentity(cluster_id="c1", topic="t1")

    with pytest.raises(InputTypeError, match="checkpoint must be CDCCheckpoint"):
        serialize_checkpoint("not_a_checkpoint")  # type: ignore[arg-type]

    with pytest.raises(InputValueError, match="format_version must be int >= 1"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=0,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=None,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    with pytest.raises(InputValueError, match="stream_name must be non-empty str"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=None,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )


def test_checkpoint_decode_errors() -> None:
    with pytest.raises(InputTypeError, match="data must be bytes"):
        deserialize_checkpoint("not_bytes")  # type: ignore[arg-type]

    with pytest.raises(RecordDecodeError) as exc_info:
        deserialize_checkpoint(b"\xff\xff\xff\xff\xff")
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE

    # Missing sink
    raw_no_sink = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
    ).SerializeToString()

    with pytest.raises(RecordDecodeError, match="missing Kafka sink identity"):
        deserialize_checkpoint(raw_no_sink)

    # Empty sink topic
    raw_empty_topic = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic=""),
    ).SerializeToString()

    with pytest.raises(RecordDecodeError, match="empty cluster_id or topic"):
        deserialize_checkpoint(raw_empty_topic)


def test_leader_lease_roundtrip() -> None:
    lease = LeaderLease(
        source_id="fdb-primary",
        stream_name="tenant_stream",
        owner_instance_id="pod-node-alpha",
        lease_epoch=15,
        heartbeat_sequence=789,
        expiration_timestamp_ns=1700000500_000000000,
    )

    data = serialize_leader_lease(lease)
    assert isinstance(data, bytes)

    decoded = deserialize_leader_lease(data)
    assert decoded == lease


def test_leader_lease_decode_errors() -> None:
    with pytest.raises(InputTypeError, match="data must be bytes"):
        deserialize_leader_lease(12345)  # type: ignore[arg-type]

    with pytest.raises(RecordDecodeError):
        deserialize_leader_lease(b"\xff\xff\xff\xff\xff")

    # Missing owner_instance_id
    raw_no_owner = state_pb2.LeaderLease(
        source_id="s1",
        stream_name="st1",
        owner_instance_id="",
    ).SerializeToString()

    with pytest.raises(RecordDecodeError, match="missing owner_instance_id"):
        deserialize_leader_lease(raw_no_owner)
