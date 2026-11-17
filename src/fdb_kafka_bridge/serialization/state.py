"""Serialization and deserialization for v0.2.0 bridge control-plane state."""

from __future__ import annotations

from google.protobuf.message import DecodeError
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
from fdbkafka.bridge.state.v1 import state_pb2


def _timestamp_from_ns(timestamp_ns: int | None) -> Timestamp | None:
    """Convert integer Unix nanoseconds to google.protobuf.Timestamp."""
    if timestamp_ns is None:
        return None
    seconds, nanos = divmod(timestamp_ns, 10**9)
    return Timestamp(seconds=seconds, nanos=nanos)


def _ns_from_timestamp(msg: Timestamp | None) -> int | None:
    """Convert google.protobuf.Timestamp to integer Unix nanoseconds."""
    if msg is None or not (msg.seconds or msg.nanos):
        return None
    return msg.seconds * 10**9 + msg.nanos


def serialize_checkpoint(checkpoint: CDCCheckpoint) -> bytes:
    """Serialize a CDCCheckpoint state model into protobuf bytes.

    Args:
        checkpoint: Structured CDCCheckpoint instance.

    Returns:
        Protobuf-encoded state bytes.

    Raises:
        InputTypeError: If checkpoint or any field has an invalid Python type.
        InputValueError: If any value is out of its permitted domain.
    """
    if not isinstance(checkpoint, CDCCheckpoint):
        raise InputTypeError(
            f"checkpoint must be CDCCheckpoint, got {type(checkpoint).__name__}",
            field="checkpoint",
        )
    if type(checkpoint.format_version) is not int or checkpoint.format_version < 1:
        raise InputValueError(
            f"format_version must be int >= 1, got {checkpoint.format_version!r}",
            field="format_version",
        )
    if not isinstance(checkpoint.source_id, str):
        raise InputTypeError("source_id must be str", field="source_id")
    if not isinstance(checkpoint.stream_name, str) or not checkpoint.stream_name:
        raise InputValueError("stream_name must be non-empty str", field="stream_name")
    if type(checkpoint.stream_id) is not int or checkpoint.stream_id < 0:
        raise InputValueError("stream_id must be non-negative int", field="stream_id")
    if (
        type(checkpoint.last_consumed_version) is not int
        or checkpoint.last_consumed_version < 0
    ):
        raise InputValueError(
            "last_consumed_version must be non-negative int",
            field="last_consumed_version",
        )
    if not isinstance(checkpoint.sink, KafkaSinkIdentity):
        raise InputTypeError(
            f"sink must be KafkaSinkIdentity, got {type(checkpoint.sink).__name__}",
            field="sink",
        )
    if (
        not isinstance(checkpoint.sink.cluster_id, str)
        or not checkpoint.sink.cluster_id
    ):
        raise InputValueError("sink.cluster_id must be non-empty str", field="sink")
    if not isinstance(checkpoint.sink.topic, str) or not checkpoint.sink.topic:
        raise InputValueError("sink.topic must be non-empty str", field="sink")
    if (
        not isinstance(checkpoint.updating_instance_id, str)
        or not checkpoint.updating_instance_id
    ):
        raise InputValueError(
            "updating_instance_id must be non-empty str", field="updating_instance_id"
        )
    if (
        checkpoint.update_timestamp_ns is not None
        and type(checkpoint.update_timestamp_ns) is not int
    ):
        raise InputTypeError(
            "update_timestamp_ns must be int or None", field="update_timestamp_ns"
        )
    if type(checkpoint.lease_epoch) is not int or checkpoint.lease_epoch < 0:
        raise InputValueError(
            "lease_epoch must be non-negative int", field="lease_epoch"
        )
    if not isinstance(checkpoint.range_config_fingerprint, str):
        raise InputTypeError(
            "range_config_fingerprint must be str", field="range_config_fingerprint"
        )

    sink_msg = state_pb2.KafkaSinkIdentity(
        cluster_id=checkpoint.sink.cluster_id,
        topic=checkpoint.sink.topic,
    )
    ts_msg = _timestamp_from_ns(checkpoint.update_timestamp_ns)

    msg = state_pb2.CDCCheckpoint(
        format_version=checkpoint.format_version,
        source_id=checkpoint.source_id,
        stream_name=checkpoint.stream_name,
        stream_id=checkpoint.stream_id,
        last_consumed_version=checkpoint.last_consumed_version,
        sink=sink_msg,
        updating_instance_id=checkpoint.updating_instance_id,
        update_timestamp=ts_msg,
        lease_epoch=checkpoint.lease_epoch,
        range_config_fingerprint=checkpoint.range_config_fingerprint,
    )
    return msg.SerializeToString()


def deserialize_checkpoint(data: bytes) -> CDCCheckpoint:
    """Parse a CDCCheckpoint from protobuf bytes.

    Args:
        data: Raw encoded protobuf bytes.

    Returns:
        Structured CDCCheckpoint instance.

    Raises:
        InputTypeError: If data is not bytes.
        RecordDecodeError: If bytes are malformed or missing required sink identity.
    """
    if not isinstance(data, bytes):
        raise InputTypeError(
            f"data must be bytes, got {type(data).__name__}", field="data"
        )
    msg = state_pb2.CDCCheckpoint()
    try:
        msg.ParseFromString(data)
    except (DecodeError, ValueError) as exc:
        raise RecordDecodeError(
            f"{len(data)} bytes are not a parseable CDCCheckpoint",
            reason=DecodeFailure.MALFORMED_WIRE,
        ) from exc

    if msg.format_version == 0:
        raise RecordDecodeError(
            "Checkpoint format_version is 0 or missing",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.HasField("sink"):
        raise RecordDecodeError(
            "Checkpoint missing Kafka sink identity",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.sink.cluster_id or not msg.sink.topic:
        raise RecordDecodeError(
            "Checkpoint sink has empty cluster_id or topic",
            reason=DecodeFailure.MALFORMED_WIRE,
        )

    sink = KafkaSinkIdentity(cluster_id=msg.sink.cluster_id, topic=msg.sink.topic)
    update_ts = (
        _ns_from_timestamp(msg.update_timestamp)
        if msg.HasField("update_timestamp")
        else None
    )

    return CDCCheckpoint(
        format_version=msg.format_version,
        source_id=msg.source_id,
        stream_name=msg.stream_name,
        stream_id=msg.stream_id,
        last_consumed_version=msg.last_consumed_version,
        sink=sink,
        updating_instance_id=msg.updating_instance_id,
        update_timestamp_ns=update_ts,
        lease_epoch=msg.lease_epoch,
        range_config_fingerprint=msg.range_config_fingerprint,
    )


def serialize_leader_lease(lease: LeaderLease) -> bytes:
    """Serialize a LeaderLease state model into protobuf bytes.

    Args:
        lease: Structured LeaderLease instance.

    Returns:
        Protobuf-encoded state bytes.

    Raises:
        InputTypeError: If lease or any field has an invalid Python type.
        InputValueError: If any value is out of its permitted domain.
    """
    if not isinstance(lease, LeaderLease):
        raise InputTypeError(
            f"lease must be LeaderLease, got {type(lease).__name__}",
            field="lease",
        )
    if not isinstance(lease.source_id, str):
        raise InputTypeError("source_id must be str", field="source_id")
    if not isinstance(lease.stream_name, str) or not lease.stream_name:
        raise InputValueError("stream_name must be non-empty str", field="stream_name")
    if not isinstance(lease.owner_instance_id, str) or not lease.owner_instance_id:
        raise InputValueError(
            "owner_instance_id must be non-empty str", field="owner_instance_id"
        )
    if type(lease.lease_epoch) is not int or lease.lease_epoch < 0:
        raise InputValueError(
            "lease_epoch must be non-negative int", field="lease_epoch"
        )
    if type(lease.heartbeat_sequence) is not int or lease.heartbeat_sequence < 0:
        raise InputValueError(
            "heartbeat_sequence must be non-negative int", field="heartbeat_sequence"
        )
    if (
        lease.expiration_timestamp_ns is not None
        and type(lease.expiration_timestamp_ns) is not int
    ):
        raise InputTypeError(
            "expiration_timestamp_ns must be int or None",
            field="expiration_timestamp_ns",
        )

    ts_msg = _timestamp_from_ns(lease.expiration_timestamp_ns)
    msg = state_pb2.LeaderLease(
        source_id=lease.source_id,
        stream_name=lease.stream_name,
        owner_instance_id=lease.owner_instance_id,
        lease_epoch=lease.lease_epoch,
        heartbeat_sequence=lease.heartbeat_sequence,
        expiration_timestamp=ts_msg,
    )
    return msg.SerializeToString()


def deserialize_leader_lease(data: bytes) -> LeaderLease:
    """Parse a LeaderLease from protobuf bytes.

    Args:
        data: Raw encoded protobuf bytes.

    Returns:
        Structured LeaderLease instance.

    Raises:
        InputTypeError: If data is not bytes.
        RecordDecodeError: If bytes are malformed or missing required owner identity.
    """
    if not isinstance(data, bytes):
        raise InputTypeError(
            f"data must be bytes, got {type(data).__name__}", field="data"
        )
    msg = state_pb2.LeaderLease()
    try:
        msg.ParseFromString(data)
    except (DecodeError, ValueError) as exc:
        raise RecordDecodeError(
            f"{len(data)} bytes are not a parseable LeaderLease",
            reason=DecodeFailure.MALFORMED_WIRE,
        ) from exc

    if not msg.owner_instance_id:
        raise RecordDecodeError(
            "LeaderLease missing owner_instance_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )

    exp_ts = (
        _ns_from_timestamp(msg.expiration_timestamp)
        if msg.HasField("expiration_timestamp")
        else None
    )

    return LeaderLease(
        source_id=msg.source_id,
        stream_name=msg.stream_name,
        owner_instance_id=msg.owner_instance_id,
        lease_epoch=msg.lease_epoch,
        heartbeat_sequence=msg.heartbeat_sequence,
        expiration_timestamp_ns=exp_ts,
    )
