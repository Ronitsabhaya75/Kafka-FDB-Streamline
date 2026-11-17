"""Tests for v0.2.0 bridge control-plane state serialization and deserialization."""

from __future__ import annotations

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

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
    INT64_MAX,
    MAX_TIMESTAMP_NS,
    MAX_TIMESTAMP_SECONDS,
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
    "0a0b6664622d7072696d617279120d74656e616e745f73747265616d1a0e706f642d6e6f6465"
    "2d616c706861200f289506320608f4e5cfaa06"
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
        range_config_fingerprint="sha256:fingerprint_hash_abc123",
    )


def _sample_lease() -> LeaderLease:
    return LeaderLease(
        source_id="fdb-primary",
        stream_name="tenant_stream",
        owner_instance_id="pod-node-alpha",
        lease_epoch=15,
        heartbeat_sequence=789,
        expiration_timestamp_ns=1700000500_000000000,
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


def test_checkpoint_uint64_and_epoch_boundaries() -> None:
    sink = KafkaSinkIdentity(cluster_id="c1", topic="t1")

    # Boundary 1: minimums (0) and epoch zero timestamp (0 ns)
    cp_zero = CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="str1",
        stream_id=0,
        last_consumed_version=0,
        sink=sink,
        updating_instance_id="i1",
        update_timestamp_ns=0,
        lease_epoch=0,
        range_config_fingerprint="fp0",
    )
    wire_zero = serialize_checkpoint(cp_zero)
    decoded_zero = deserialize_checkpoint(wire_zero)
    assert decoded_zero == cp_zero
    assert decoded_zero.stream_id == 0
    assert decoded_zero.last_consumed_version == 0
    assert decoded_zero.lease_epoch == 0
    assert decoded_zero.update_timestamp_ns == 0

    # Boundary 2: maximums (last_consumed_version=INT64_MAX, stream_id=UINT64_MAX,
    # lease_epoch=UINT64_MAX, update_timestamp_ns=MAX_TIMESTAMP_NS)
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
        range_config_fingerprint="fp_max",
    )
    wire_max = serialize_checkpoint(cp_max)
    decoded_max = deserialize_checkpoint(wire_max)
    assert decoded_max == cp_max
    assert decoded_max.stream_id == UINT64_MAX
    assert decoded_max.last_consumed_version == INT64_MAX
    assert decoded_max.lease_epoch == UINT64_MAX
    assert decoded_max.update_timestamp_ns == MAX_TIMESTAMP_NS


def test_checkpoint_validation_errors() -> None:
    sink = KafkaSinkIdentity(cluster_id="c1", topic="t1")

    with pytest.raises(InputTypeError, match="checkpoint must be CDCCheckpoint"):
        serialize_checkpoint("not_a_checkpoint")  # type: ignore[arg-type]

    # Unsupported format versions
    for bad_ver in (0, 2, -1):
        with pytest.raises(InputValueError, match="format_version"):
            serialize_checkpoint(
                CDCCheckpoint(
                    format_version=bad_ver,
                    source_id="s1",
                    stream_name="str1",
                    stream_id=1,
                    last_consumed_version=1,
                    sink=sink,
                    updating_instance_id="i1",
                    update_timestamp_ns=1000,
                    lease_epoch=1,
                    range_config_fingerprint="fp",
                )
            )

    # Empty required string identities
    with pytest.raises(InputValueError, match="source_id"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=1000,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    with pytest.raises(InputValueError, match="stream_name"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=1000,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    with pytest.raises(InputValueError, match="updating_instance_id"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="",
                update_timestamp_ns=1000,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    # Sink errors
    with pytest.raises(InputValueError, match="sink.cluster_id"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=KafkaSinkIdentity(cluster_id="", topic="t1"),
                updating_instance_id="i1",
                update_timestamp_ns=1000,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    with pytest.raises(InputValueError, match="sink.topic"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=KafkaSinkIdentity(cluster_id="c1", topic=""),
                updating_instance_id="i1",
                update_timestamp_ns=1000,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    # stream_id uint64 domain errors
    for bad_id in (-1, UINT64_MAX + 1):
        with pytest.raises(InputValueError, match="stream_id"):
            serialize_checkpoint(
                CDCCheckpoint(
                    format_version=1,
                    source_id="s1",
                    stream_name="str1",
                    stream_id=bad_id,
                    last_consumed_version=1,
                    sink=sink,
                    updating_instance_id="i1",
                    update_timestamp_ns=1000,
                    lease_epoch=1,
                    range_config_fingerprint="fp",
                )
            )

    # last_consumed_version must be in 0..INT64_MAX (resumable CDC cursor range)
    for bad_version in (-1, INT64_MAX + 1, UINT64_MAX):
        with pytest.raises(InputValueError, match="last_consumed_version"):
            serialize_checkpoint(
                CDCCheckpoint(
                    format_version=1,
                    source_id="s1",
                    stream_name="str1",
                    stream_id=1,
                    last_consumed_version=bad_version,
                    sink=sink,
                    updating_instance_id="i1",
                    update_timestamp_ns=1000,
                    lease_epoch=1,
                    range_config_fingerprint="fp",
                )
            )

    # Negative or out-of-range timestamp
    with pytest.raises(InputValueError, match="update_timestamp_ns"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=-100,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    with pytest.raises(InputValueError, match="update_timestamp_ns"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=MAX_TIMESTAMP_NS + 1,
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    # None update_timestamp_ns is rejected
    with pytest.raises(InputTypeError, match="update_timestamp_ns"):
        serialize_checkpoint(
            CDCCheckpoint(
                format_version=1,
                source_id="s1",
                stream_name="str1",
                stream_id=1,
                last_consumed_version=1,
                sink=sink,
                updating_instance_id="i1",
                update_timestamp_ns=None,  # type: ignore[arg-type]
                lease_epoch=1,
                range_config_fingerprint="fp",
            )
        )

    # UTF-8 encodability errors (unpaired surrogates)
    surrogate = "\ud800"
    surrogate_cases: list[tuple[dict[str, object], str]] = [
        ({"source_id": surrogate}, "source_id"),
        ({"stream_name": surrogate}, "stream_name"),
        (
            {"sink": KafkaSinkIdentity(cluster_id=surrogate, topic="t1")},
            "sink.cluster_id",
        ),
        (
            {"sink": KafkaSinkIdentity(cluster_id="c1", topic=surrogate)},
            "sink.topic",
        ),
        ({"updating_instance_id": surrogate}, "updating_instance_id"),
        ({"range_config_fingerprint": surrogate}, "range_config_fingerprint"),
    ]
    for field_kwargs, expected_field in surrogate_cases:
        base_kwargs: dict[str, object] = {
            "format_version": 1,
            "source_id": "s1",
            "stream_name": "str1",
            "stream_id": 1,
            "last_consumed_version": 1,
            "sink": sink,
            "updating_instance_id": "i1",
            "update_timestamp_ns": 1000,
            "lease_epoch": 1,
            "range_config_fingerprint": "fp",
        }
        base_kwargs.update(field_kwargs)
        with pytest.raises(InputValueError, match="not UTF-8-encodable") as exc_info:
            serialize_checkpoint(CDCCheckpoint(**base_kwargs))  # type: ignore[arg-type]
        assert exc_info.value.field == expected_field


def test_checkpoint_decode_errors() -> None:
    with pytest.raises(InputTypeError, match="data must be bytes"):
        deserialize_checkpoint("not_bytes")  # type: ignore[arg-type]

    with pytest.raises(RecordDecodeError) as exc_info:
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
        with pytest.raises(RecordDecodeError, match="format_version"):
            deserialize_checkpoint(wire_bad_ver)

    # Missing / empty identities on wire
    wire_empty_source = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="source_id"):
        deserialize_checkpoint(wire_empty_source)

    wire_empty_stream = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="stream_name"):
        deserialize_checkpoint(wire_empty_stream)

    wire_empty_updater = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="updating_instance_id"):
        deserialize_checkpoint(wire_empty_updater)

    # last_consumed_version exceeding INT64_MAX on wire
    wire_bad_version = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        last_consumed_version=INT64_MAX + 1,
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="last_consumed_version"):
        deserialize_checkpoint(wire_bad_version)

    # Missing sink
    wire_no_sink = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="missing Kafka sink identity"):
        deserialize_checkpoint(wire_no_sink)

    # Empty sink topic
    wire_empty_topic = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic=""),
        update_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="empty cluster_id or topic"):
        deserialize_checkpoint(wire_empty_topic)

    # Missing update_timestamp on wire
    wire_no_ts = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="missing required update_timestamp"):
        deserialize_checkpoint(wire_no_ts)

    # Malformed timestamp nanos on wire
    wire_bad_nanos = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=100, nanos=1_000_000_000),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="Invalid timestamp nanos"):
        deserialize_checkpoint(wire_bad_nanos)

    # Out of range timestamp seconds (> 253402300799)
    wire_overflow_seconds = state_pb2.CDCCheckpoint(
        format_version=1,
        source_id="s1",
        stream_name="st1",
        updating_instance_id="i1",
        sink=state_pb2.KafkaSinkIdentity(cluster_id="c1", topic="t1"),
        update_timestamp=Timestamp(seconds=MAX_TIMESTAMP_SECONDS + 1, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="Invalid timestamp seconds"):
        deserialize_checkpoint(wire_overflow_seconds)


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


def test_leader_lease_uint64_and_epoch_boundaries() -> None:
    # Boundary 1: epoch zero expiration timestamp (0 ns) and zero epochs
    lease_zero = LeaderLease(
        source_id="s1",
        stream_name="str1",
        owner_instance_id="pod1",
        lease_epoch=0,
        heartbeat_sequence=0,
        expiration_timestamp_ns=0,
    )
    wire_zero = serialize_leader_lease(lease_zero)
    decoded_zero = deserialize_leader_lease(wire_zero)
    assert decoded_zero == lease_zero
    assert decoded_zero.expiration_timestamp_ns == 0
    assert decoded_zero.lease_epoch == 0
    assert decoded_zero.heartbeat_sequence == 0

    # Boundary 2: maximum uint64 and MAX_TIMESTAMP_NS expiration
    lease_max = LeaderLease(
        source_id="s1",
        stream_name="str1",
        owner_instance_id="pod1",
        lease_epoch=UINT64_MAX,
        heartbeat_sequence=UINT64_MAX,
        expiration_timestamp_ns=MAX_TIMESTAMP_NS,
    )
    wire_max = serialize_leader_lease(lease_max)
    decoded_max = deserialize_leader_lease(wire_max)
    assert decoded_max == lease_max
    assert decoded_max.lease_epoch == UINT64_MAX
    assert decoded_max.heartbeat_sequence == UINT64_MAX
    assert decoded_max.expiration_timestamp_ns == MAX_TIMESTAMP_NS


def test_leader_lease_validation_errors() -> None:
    with pytest.raises(InputTypeError, match="lease must be LeaderLease"):
        serialize_leader_lease("not_a_lease")  # type: ignore[arg-type]

    # Missing required identities
    with pytest.raises(InputValueError, match="source_id"):
        serialize_leader_lease(
            LeaderLease(
                source_id="",
                stream_name="str1",
                owner_instance_id="pod1",
                lease_epoch=1,
                heartbeat_sequence=1,
                expiration_timestamp_ns=1000,
            )
        )

    with pytest.raises(InputValueError, match="stream_name"):
        serialize_leader_lease(
            LeaderLease(
                source_id="s1",
                stream_name="",
                owner_instance_id="pod1",
                lease_epoch=1,
                heartbeat_sequence=1,
                expiration_timestamp_ns=1000,
            )
        )

    with pytest.raises(InputValueError, match="owner_instance_id"):
        serialize_leader_lease(
            LeaderLease(
                source_id="s1",
                stream_name="str1",
                owner_instance_id="",
                lease_epoch=1,
                heartbeat_sequence=1,
                expiration_timestamp_ns=1000,
            )
        )

    # None expiration timestamp
    with pytest.raises(InputTypeError, match="expiration_timestamp_ns"):
        serialize_leader_lease(
            LeaderLease(
                source_id="s1",
                stream_name="str1",
                owner_instance_id="pod1",
                lease_epoch=1,
                heartbeat_sequence=1,
                expiration_timestamp_ns=None,  # type: ignore[arg-type]
            )
        )

    # Negative expiration timestamp
    with pytest.raises(InputValueError, match="expiration_timestamp_ns"):
        serialize_leader_lease(
            LeaderLease(
                source_id="s1",
                stream_name="str1",
                owner_instance_id="pod1",
                lease_epoch=1,
                heartbeat_sequence=1,
                expiration_timestamp_ns=-1000,
            )
        )

    # Expiration exceeding MAX_TIMESTAMP_NS
    with pytest.raises(InputValueError, match="expiration_timestamp_ns"):
        serialize_leader_lease(
            LeaderLease(
                source_id="s1",
                stream_name="str1",
                owner_instance_id="pod1",
                lease_epoch=1,
                heartbeat_sequence=1,
                expiration_timestamp_ns=MAX_TIMESTAMP_NS + 1,
            )
        )

    # UTF-8 encodability errors (unpaired surrogates)
    surrogate = "\ud800"
    lease_surrogate_cases: list[tuple[dict[str, object], str]] = [
        ({"source_id": surrogate}, "source_id"),
        ({"stream_name": surrogate}, "stream_name"),
        ({"owner_instance_id": surrogate}, "owner_instance_id"),
    ]
    for field_kwargs, expected_field in lease_surrogate_cases:
        lease_base: dict[str, object] = {
            "source_id": "s1",
            "stream_name": "str1",
            "owner_instance_id": "pod1",
            "lease_epoch": 1,
            "heartbeat_sequence": 1,
            "expiration_timestamp_ns": 1000,
        }
        lease_base.update(field_kwargs)
        with pytest.raises(InputValueError, match="not UTF-8-encodable") as exc_info:
            serialize_leader_lease(LeaderLease(**lease_base))  # type: ignore[arg-type]
        assert exc_info.value.field == expected_field


def test_leader_lease_decode_errors() -> None:
    with pytest.raises(InputTypeError, match="data must be bytes"):
        deserialize_leader_lease(12345)  # type: ignore[arg-type]

    with pytest.raises(RecordDecodeError):
        deserialize_leader_lease(b"\xff\xff\xff\xff\xff")

    # Missing source_id on wire
    wire_no_source = state_pb2.LeaderLease(
        source_id="",
        stream_name="st1",
        owner_instance_id="pod1",
        expiration_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="source_id"):
        deserialize_leader_lease(wire_no_source)

    # Missing stream_name on wire
    wire_no_stream = state_pb2.LeaderLease(
        source_id="s1",
        stream_name="",
        owner_instance_id="pod1",
        expiration_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="stream_name"):
        deserialize_leader_lease(wire_no_stream)

    # Missing owner_instance_id on wire
    wire_no_owner = state_pb2.LeaderLease(
        source_id="s1",
        stream_name="st1",
        owner_instance_id="",
        expiration_timestamp=Timestamp(seconds=100, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="owner_instance_id"):
        deserialize_leader_lease(wire_no_owner)

    # Missing expiration timestamp on wire
    wire_no_exp = state_pb2.LeaderLease(
        source_id="s1",
        stream_name="st1",
        owner_instance_id="pod1",
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="expiration_timestamp"):
        deserialize_leader_lease(wire_no_exp)

    # Malformed timestamp nanos on wire
    wire_bad_nanos = state_pb2.LeaderLease(
        source_id="s1",
        stream_name="st1",
        owner_instance_id="pod1",
        expiration_timestamp=Timestamp(seconds=100, nanos=1_000_000_000),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="Invalid timestamp nanos"):
        deserialize_leader_lease(wire_bad_nanos)

    # Out of range timestamp seconds (> 253402300799)
    wire_overflow_seconds = state_pb2.LeaderLease(
        source_id="s1",
        stream_name="st1",
        owner_instance_id="pod1",
        expiration_timestamp=Timestamp(seconds=MAX_TIMESTAMP_SECONDS + 1, nanos=0),
    ).SerializeToString()
    with pytest.raises(RecordDecodeError, match="Invalid timestamp seconds"):
        deserialize_leader_lease(wire_overflow_seconds)
