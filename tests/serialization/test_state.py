"""Tests for v0.2.0 bridge control-plane state serialization and deserialization."""

from __future__ import annotations

from dataclasses import replace

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from fdb_kafka_bridge.serialization.errors import (
    DecodeFailure,
    InputTypeError,
    InputValueError,
    StateDecodeError,
)
from fdb_kafka_bridge.serialization.model import (
    CDCCheckpoint,
    KafkaSinkIdentity,
    LeaderLease,
)
from fdb_kafka_bridge.serialization.state import (
    INT64_MAX,
    MAX_TIMESTAMP_NS,
    UINT64_MAX,
    deserialize_checkpoint,
    deserialize_leader_lease,
    serialize_checkpoint,
    serialize_leader_lease,
)
from fdbkafka.bridge.state.v1 import state_pb2

CP_GOLDEN = bytes.fromhex(
    "0801120e6664622d7072696d6172792d64631a0d6f72646572735f73747265616d20cec2f105"
    "2896d5d02f321f0a0f636c75737465722d70726f642d3031120c6f72646572732d746f706963"
    "3a116272696467652d696e7374616e63652d31420b0880e2cfaa0610959aef3a482a521e7368"
    "613235363a66696e6765727072696e745f686173685f616263313233"
)

LEASE_GOLDEN = bytes.fromhex(
    "0801120b6664622d7072696d6172791a0d74656e616e745f73747265616d220e706f642d6e6f"
    "64652d616c706861280f3095063a0608f4e5cfaa06402a"
)


def _sample_checkpoint() -> CDCCheckpoint:
    sink = KafkaSinkIdentity(cluster_id="cluster-prod-01", topic="orders-topic")
    return CDCCheckpoint(
        format_version=1,
        source_id="fdb-primary-dc",
        stream_name="orders_stream",
        stream_id=12345678,
        last_consumed_version=99887766,
        sink=sink,
        updating_instance_id="bridge-instance-1",
        update_timestamp_ns=1700000000_123456789,
        lease_epoch=42,
        range_config_fingerprint=b"sha256:fingerprint_hash_abc123",
    )


def _sample_lease() -> LeaderLease:
    return LeaderLease(
        format_version=1,
        source_id="fdb-primary",
        stream_name="tenant_stream",
        owner_instance_id="pod-node-alpha",
        lease_epoch=15,
        heartbeat_sequence=789,
        expiration_timestamp_ns=1700000500_000000000,
        stream_id=42,
    )


def test_checkpoint_roundtrip() -> None:
    cp = _sample_checkpoint()
    data = serialize_checkpoint(cp)
    assert isinstance(data, bytes)
    decoded = deserialize_checkpoint(data)
    assert decoded == cp


def test_checkpoint_golden_bytes() -> None:
    cp = _sample_checkpoint()
    wire = serialize_checkpoint(cp)
    assert wire == CP_GOLDEN
    restored = deserialize_checkpoint(CP_GOLDEN)
    assert restored == cp


def test_checkpoint_fresh_cursor_and_boundary_values() -> None:
    sink = KafkaSinkIdentity(cluster_id="c1", topic="t1")

    # Boundary 1: minimums (fresh cursor -1, epoch 0, stream_id None)
    cp_fresh = CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="str1",
        stream_id=None,
        last_consumed_version=-1,
        sink=sink,
        updating_instance_id="i1",
        update_timestamp_ns=0,
        lease_epoch=0,
        range_config_fingerprint=b"",
    )
    wire_fresh = serialize_checkpoint(cp_fresh)
    decoded_fresh = deserialize_checkpoint(wire_fresh)
    assert decoded_fresh.last_consumed_version == -1
    assert decoded_fresh.stream_id is None
    assert decoded_fresh.update_timestamp_ns == 0
    assert decoded_fresh.lease_epoch == 0

    # Boundary 2: maximums
    cp_max = CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="str1",
        stream_id=UINT64_MAX,
        last_consumed_version=INT64_MAX,
        sink=sink,
        updating_instance_id="i1",
        update_timestamp_ns=MAX_TIMESTAMP_NS,
        lease_epoch=UINT64_MAX,
        range_config_fingerprint=b"\xff" * 32,
    )
    wire_max = serialize_checkpoint(cp_max)
    decoded_max = deserialize_checkpoint(wire_max)
    assert decoded_max.stream_id == UINT64_MAX
    assert decoded_max.last_consumed_version == INT64_MAX
    assert decoded_max.update_timestamp_ns == MAX_TIMESTAMP_NS
    assert decoded_max.lease_epoch == UINT64_MAX
    assert decoded_max.range_config_fingerprint == b"\xff" * 32


@pytest.mark.parametrize(
    ("changes", "expected_field", "exc_type", "match"),
    [
        ({"format_version": 0}, "format_version", InputValueError, "format_version"),
        ({"format_version": 2}, "format_version", InputValueError, "format_version"),
        ({"format_version": "1"}, "format_version", InputTypeError, "format_version"),
        ({"source_id": ""}, "source_id", InputValueError, "source_id"),
        ({"source_id": 123}, "source_id", InputTypeError, "source_id"),
        ({"stream_name": ""}, "stream_name", InputValueError, "stream_name"),
        ({"stream_name": 123}, "stream_name", InputTypeError, "stream_name"),
        ({"stream_id": -1}, "stream_id", InputValueError, "stream_id"),
        ({"stream_id": UINT64_MAX + 1}, "stream_id", InputValueError, "stream_id"),
        ({"stream_id": "1"}, "stream_id", InputTypeError, "stream_id"),
        (
            {"last_consumed_version": -2},
            "last_consumed_version",
            InputValueError,
            "last_consumed_version",
        ),
        (
            {"last_consumed_version": INT64_MAX + 1},
            "last_consumed_version",
            InputValueError,
            "last_consumed_version",
        ),
        (
            {"last_consumed_version": "1"},
            "last_consumed_version",
            InputTypeError,
            "last_consumed_version",
        ),
        ({"sink": "not_a_sink"}, "sink", InputTypeError, "sink"),
        (
            {"sink": KafkaSinkIdentity(cluster_id="", topic="t1")},
            "sink.cluster_id",
            InputValueError,
            "sink.cluster_id",
        ),
        (
            {"sink": KafkaSinkIdentity(cluster_id="c1", topic="")},
            "sink.topic",
            InputValueError,
            "sink.topic",
        ),
        (
            {"updating_instance_id": ""},
            "updating_instance_id",
            InputValueError,
            "updating_instance_id",
        ),
        (
            {"updating_instance_id": 123},
            "updating_instance_id",
            InputTypeError,
            "updating_instance_id",
        ),
        (
            {"update_timestamp_ns": -1},
            "update_timestamp_ns",
            InputValueError,
            "update_timestamp_ns",
        ),
        (
            {"update_timestamp_ns": MAX_TIMESTAMP_NS + 1},
            "update_timestamp_ns",
            InputValueError,
            "update_timestamp_ns",
        ),
        (
            {"update_timestamp_ns": "1"},
            "update_timestamp_ns",
            InputTypeError,
            "update_timestamp_ns",
        ),
        ({"lease_epoch": -1}, "lease_epoch", InputValueError, "lease_epoch"),
        (
            {"lease_epoch": UINT64_MAX + 1},
            "lease_epoch",
            InputValueError,
            "lease_epoch",
        ),
        ({"lease_epoch": "1"}, "lease_epoch", InputTypeError, "lease_epoch"),
        (
            {"range_config_fingerprint": "not_bytes"},
            "range_config_fingerprint",
            InputTypeError,
            "range_config_fingerprint",
        ),
        (
            {"source_id": "\ud800"},
            "source_id",
            InputValueError,
            "not UTF-8-encodable",
        ),
        (
            {"stream_name": "\ud800"},
            "stream_name",
            InputValueError,
            "not UTF-8-encodable",
        ),
        (
            {"sink": KafkaSinkIdentity(cluster_id="\ud800", topic="t1")},
            "sink.cluster_id",
            InputValueError,
            "not UTF-8-encodable",
        ),
        (
            {"sink": KafkaSinkIdentity(cluster_id="c1", topic="\ud800")},
            "sink.topic",
            InputValueError,
            "not UTF-8-encodable",
        ),
        (
            {"updating_instance_id": "\ud800"},
            "updating_instance_id",
            InputValueError,
            "not UTF-8-encodable",
        ),
    ],
)
def test_checkpoint_validation_errors(
    changes: dict[str, object],
    expected_field: str,
    exc_type: type[Exception],
    match: str,
) -> None:
    cp = replace(_sample_checkpoint(), **changes)  # type: ignore[arg-type]
    with pytest.raises(exc_type, match=match) as exc_info:
        serialize_checkpoint(cp)
    assert getattr(exc_info.value, "field", None) == expected_field


def test_huge_int_does_not_crash_error_rendering() -> None:
    cp = replace(_sample_checkpoint(), stream_id=10**5000)
    with pytest.raises(InputValueError, match="stream_id") as exc_info:
        serialize_checkpoint(cp)
    assert exc_info.value.field == "stream_id"
    assert "bits" in str(exc_info.value)


def test_checkpoint_decode_errors() -> None:
    with pytest.raises(InputTypeError, match="data must be bytes"):
        deserialize_checkpoint("not_bytes")  # type: ignore[arg-type]

    with pytest.raises(StateDecodeError) as exc_info:
        deserialize_checkpoint(b"\xff\xff\xff\xff\xff")
    assert exc_info.value.reason == DecodeFailure.MALFORMED_WIRE

    # Unsupported format versions on wire
    for bad_ver in (0, 2):
        wire_bad_ver = state_pb2.CDCCheckpoint(
            format_version=bad_ver,
            source_id="s1",
            stream_name="st1",
            updating_instance_id="i1",
            sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
            update_timestamp=Timestamp(seconds=100, nanos=0),
        ).SerializeToString()
        with pytest.raises(StateDecodeError, match="format_version") as exc_v:
            deserialize_checkpoint(wire_bad_ver)
        assert exc_v.value.reason == DecodeFailure.UNSUPPORTED_FORMAT_VERSION

    # Out of range version on wire
    wire_overflow_ver = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        last_consumed_version=-2,
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(StateDecodeError, match="last_consumed_version") as exc_ov:
        deserialize_checkpoint(wire_overflow_ver)
    assert exc_ov.value.reason == DecodeFailure.VERSION_OUT_OF_RANGE

    # Missing / empty identities on wire
    wire_empty_source = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(StateDecodeError, match="source_id") as exc_s:
        deserialize_checkpoint(wire_empty_source)
    assert exc_s.value.reason == DecodeFailure.MALFORMED_WIRE

    # Missing required sink
    wire_no_sink = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(StateDecodeError, match="sink"):
        deserialize_checkpoint(wire_no_sink)


def test_leader_lease_roundtrip() -> None:
    lease = _sample_lease()
    data = serialize_leader_lease(lease)
    assert isinstance(data, bytes)
    decoded = deserialize_leader_lease(data)
    assert decoded == lease


def test_leader_lease_golden_bytes() -> None:
    lease = _sample_lease()
    wire = serialize_leader_lease(lease)
    assert wire == LEASE_GOLDEN
    restored = deserialize_leader_lease(LEASE_GOLDEN)
    assert restored == lease


@pytest.mark.parametrize(
    ("changes", "expected_field", "exc_type", "match"),
    [
        ({"format_version": 0}, "format_version", InputValueError, "format_version"),
        ({"format_version": 2}, "format_version", InputValueError, "format_version"),
        ({"format_version": "1"}, "format_version", InputTypeError, "format_version"),
        ({"source_id": ""}, "source_id", InputValueError, "source_id"),
        ({"source_id": 123}, "source_id", InputTypeError, "source_id"),
        ({"stream_name": ""}, "stream_name", InputValueError, "stream_name"),
        ({"stream_name": 123}, "stream_name", InputTypeError, "stream_name"),
        (
            {"owner_instance_id": ""},
            "owner_instance_id",
            InputValueError,
            "owner_instance_id",
        ),
        (
            {"owner_instance_id": 123},
            "owner_instance_id",
            InputTypeError,
            "owner_instance_id",
        ),
        ({"lease_epoch": -1}, "lease_epoch", InputValueError, "lease_epoch"),
        (
            {"lease_epoch": UINT64_MAX + 1},
            "lease_epoch",
            InputValueError,
            "lease_epoch",
        ),
        ({"lease_epoch": "1"}, "lease_epoch", InputTypeError, "lease_epoch"),
        (
            {"heartbeat_sequence": -1},
            "heartbeat_sequence",
            InputValueError,
            "heartbeat_sequence",
        ),
        (
            {"heartbeat_sequence": UINT64_MAX + 1},
            "heartbeat_sequence",
            InputValueError,
            "heartbeat_sequence",
        ),
        (
            {"heartbeat_sequence": "1"},
            "heartbeat_sequence",
            InputTypeError,
            "heartbeat_sequence",
        ),
        (
            {"expiration_timestamp_ns": -1},
            "expiration_timestamp_ns",
            InputValueError,
            "expiration_timestamp_ns",
        ),
        (
            {"expiration_timestamp_ns": MAX_TIMESTAMP_NS + 1},
            "expiration_timestamp_ns",
            InputValueError,
            "expiration_timestamp_ns",
        ),
        (
            {"expiration_timestamp_ns": "1"},
            "expiration_timestamp_ns",
            InputTypeError,
            "expiration_timestamp_ns",
        ),
        ({"stream_id": -1}, "stream_id", InputValueError, "stream_id"),
        ({"stream_id": UINT64_MAX + 1}, "stream_id", InputValueError, "stream_id"),
        (
            {"source_id": "\ud800"},
            "source_id",
            InputValueError,
            "not UTF-8-encodable",
        ),
        (
            {"stream_name": "\ud800"},
            "stream_name",
            InputValueError,
            "not UTF-8-encodable",
        ),
        (
            {"owner_instance_id": "\ud800"},
            "owner_instance_id",
            InputValueError,
            "not UTF-8-encodable",
        ),
    ],
)
def test_leader_lease_validation_errors(
    changes: dict[str, object],
    expected_field: str,
    exc_type: type[Exception],
    match: str,
) -> None:
    lease = replace(_sample_lease(), **changes)  # type: ignore[arg-type]
    with pytest.raises(exc_type, match=match) as exc_info:
        serialize_leader_lease(lease)
    assert getattr(exc_info.value, "field", None) == expected_field


def test_leader_lease_decode_errors() -> None:
    with pytest.raises(InputTypeError, match="data must be bytes"):
        deserialize_leader_lease(12345)  # type: ignore[arg-type]

    with pytest.raises(StateDecodeError):
        deserialize_leader_lease(b"\xff\xff\xff\xff\xff")

    # Unsupported format version
    wire_bad_v = state_pb2.LeaderLease(
        format_version=2,
        source_id="s1",
        stream_name="st1",
        owner_instance_id="pod1",
        expiration_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(StateDecodeError, match="format_version") as exc_bv:
        deserialize_leader_lease(wire_bad_v)
    assert exc_bv.value.reason == DecodeFailure.UNSUPPORTED_FORMAT_VERSION
