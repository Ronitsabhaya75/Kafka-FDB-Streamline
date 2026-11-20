"""Serialization and deserialization for v0.2.0 bridge control-plane state."""

from __future__ import annotations

from typing import Final

from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp

from fdb_kafka_bridge.serialization import _checks, _wire
from fdb_kafka_bridge.serialization.errors import (
    DecodeFailure,
    InputTypeError,
    StateDecodeError,
)
from fdb_kafka_bridge.serialization.model import (
    _MAX_BRIDGE_TIMESTAMP_NS,
    _MAX_VERSION,
    CDCCheckpoint,
    KafkaSinkIdentity,
    LeaderLease,
)
from fdbkafka.bridge.state.v1 import state_pb2

_CHECKPOINT_FORMAT_VERSION: Final[int] = 2
_LEASE_FORMAT_VERSION: Final[int] = 1
_SUPPORTED_CHECKPOINT_FORMAT_VERSIONS: Final[frozenset[int]] = frozenset(
    {_CHECKPOINT_FORMAT_VERSION}
)
_SUPPORTED_LEASE_FORMAT_VERSIONS: Final[frozenset[int]] = frozenset(
    {_LEASE_FORMAT_VERSION}
)
_INT64_MAX: Final[int] = _MAX_VERSION
_UINT64_MAX: Final[int] = (1 << 64) - 1
_MAX_TIMESTAMP_SECONDS: Final[int] = 253_402_300_799
_MAX_TIMESTAMP_NS: Final[int] = _MAX_BRIDGE_TIMESTAMP_NS
_NANOS_PER_SECOND: Final[int] = 10**9


def _ns_from_timestamp(msg: Timestamp) -> int:
    """Convert google.protobuf.Timestamp to integer Unix nanoseconds.

    Args:
        msg: Protobuf Timestamp message.

    Returns:
        Integer nanoseconds since Unix epoch.

    Raises:
        StateDecodeError: If timestamp is malformed or out of range.
    """
    if not (0 <= msg.nanos < _NANOS_PER_SECOND):
        raise StateDecodeError(
            f"Invalid timestamp nanos: {msg.nanos}",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not (0 <= msg.seconds <= _MAX_TIMESTAMP_SECONDS):
        raise StateDecodeError(
            f"Invalid timestamp seconds: {msg.seconds}",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    return msg.seconds * _NANOS_PER_SECOND + msg.nanos


def _validate_checkpoint_model(checkpoint: CDCCheckpoint) -> None:
    """Validate a CDCCheckpoint model before serialization.

    Args:
        checkpoint: The CDCCheckpoint instance to validate.

    Raises:
        InputTypeError: If any field has an invalid Python type.
        InputValueError: If any value is out of its permitted domain.
    """
    if not isinstance(checkpoint, CDCCheckpoint):
        raise InputTypeError(
            f"checkpoint must be CDCCheckpoint, got {type(checkpoint).__name__}",
            field="checkpoint",
        )
    _checks._integer(
        checkpoint.format_version,
        field="format_version",
        minimum=_CHECKPOINT_FORMAT_VERSION,
        maximum=_CHECKPOINT_FORMAT_VERSION,
    )
    _checks._text(checkpoint.source_id, "source_id")
    _checks._text(checkpoint.stream_name, "stream_name")

    _checks._integer(
        checkpoint.stream_id,  # type: ignore[arg-type]
        field="stream_id",
        minimum=0,
        maximum=_UINT64_MAX,
    )

    # -1 represents a fresh CDC cursor; commit versions run up to _INT64_MAX
    _checks._integer(
        checkpoint.last_consumed_version,
        field="last_consumed_version",
        minimum=-1,
        maximum=_INT64_MAX,
    )

    if not isinstance(checkpoint.sink, KafkaSinkIdentity):
        raise InputTypeError(
            f"sink must be KafkaSinkIdentity, got {type(checkpoint.sink).__name__}",
            field="sink",
        )
    _checks._text(checkpoint.sink.cluster_id, "sink.cluster_id")
    _checks._text(checkpoint.sink.topic, "sink.topic")
    _checks._text(checkpoint.updating_instance_id, "updating_instance_id")

    _checks._integer(
        checkpoint.update_timestamp_ns,
        field="update_timestamp_ns",
        minimum=0,
        maximum=_MAX_TIMESTAMP_NS,
    )
    _checks._integer(
        checkpoint.lease_epoch,
        field="lease_epoch",
        minimum=1,
        maximum=_UINT64_MAX,
    )

    if not isinstance(checkpoint.range_config_fingerprint, bytes):
        raise InputTypeError(
            f"range_config_fingerprint must be bytes, got "
            f"{type(checkpoint.range_config_fingerprint).__name__}",
            field="range_config_fingerprint",
        )


def _validate_lease_model(lease: LeaderLease) -> None:
    """Validate a LeaderLease model before serialization.

    Args:
        lease: The LeaderLease instance to validate.

    Raises:
        InputTypeError: If any field has an invalid Python type.
        InputValueError: If any value is out of its permitted domain.
    """
    if not isinstance(lease, LeaderLease):
        raise InputTypeError(
            f"lease must be LeaderLease, got {type(lease).__name__}",
            field="lease",
        )
    _checks._integer(
        lease.format_version,
        field="format_version",
        minimum=_LEASE_FORMAT_VERSION,
        maximum=_LEASE_FORMAT_VERSION,
    )
    _checks._text(lease.source_id, "source_id")
    _checks._text(lease.stream_name, "stream_name")
    _checks._text(lease.owner_instance_id, "owner_instance_id")

    _checks._integer(
        lease.lease_epoch,
        field="lease_epoch",
        minimum=1,
        maximum=_UINT64_MAX,
    )
    _checks._integer(
        lease.heartbeat_sequence,
        field="heartbeat_sequence",
        minimum=0,
        maximum=_UINT64_MAX,
    )
    _checks._integer(
        lease.expiration_timestamp_ns,
        field="expiration_timestamp_ns",
        minimum=0,
        maximum=_MAX_TIMESTAMP_NS,
    )

    if lease.stream_id is not None:
        _checks._integer(
            lease.stream_id,
            field="stream_id",
            minimum=0,
            maximum=_UINT64_MAX,
        )


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
    _validate_checkpoint_model(checkpoint)

    sink_msg = state_pb2.KafkaSinkIdentity(
        cluster_id=checkpoint.sink.cluster_id,
        topic=checkpoint.sink.topic,
    )
    ts_msg = _wire.timestamp(checkpoint.update_timestamp_ns)

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
        StateDecodeError: If bytes are malformed, unsupported, or incomplete.
    """
    if not isinstance(data, bytes):
        raise InputTypeError(
            f"data must be bytes, got {type(data).__name__}", field="data"
        )
    msg = state_pb2.CDCCheckpoint()
    try:
        msg.ParseFromString(data)
    except (DecodeError, ValueError) as exc:
        raise StateDecodeError(
            f"{len(data)} bytes are not a parseable CDCCheckpoint",
            reason=DecodeFailure.MALFORMED_WIRE,
        ) from exc

    if msg.format_version not in _SUPPORTED_CHECKPOINT_FORMAT_VERSIONS:
        raise StateDecodeError(
            f"Checkpoint format_version {msg.format_version} is unsupported",
            reason=DecodeFailure.UNSUPPORTED_FORMAT_VERSION,
        )
    if not msg.source_id:
        raise StateDecodeError(
            "Checkpoint missing or empty source_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.stream_name:
        raise StateDecodeError(
            "Checkpoint missing or empty stream_name",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not (-1 <= msg.last_consumed_version <= _INT64_MAX):
        raise StateDecodeError(
            f"Checkpoint last_consumed_version {msg.last_consumed_version} "
            f"is out of range",
            reason=DecodeFailure.VERSION_OUT_OF_RANGE,
        )
    if not msg.updating_instance_id:
        raise StateDecodeError(
            "Checkpoint missing or empty updating_instance_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.HasField("sink"):
        raise StateDecodeError(
            "Checkpoint missing Kafka sink identity",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.sink.cluster_id or not msg.sink.topic:
        raise StateDecodeError(
            "Checkpoint sink has empty cluster_id or topic",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.HasField("update_timestamp"):
        raise StateDecodeError(
            "Checkpoint missing required update_timestamp",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not (1 <= msg.lease_epoch <= _UINT64_MAX):
        raise StateDecodeError(
            f"Checkpoint lease_epoch {msg.lease_epoch} is out of range",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.HasField("stream_id"):
        raise StateDecodeError(
            "Checkpoint missing required stream_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )

    sink = KafkaSinkIdentity(cluster_id=msg.sink.cluster_id, topic=msg.sink.topic)
    update_ts = _ns_from_timestamp(msg.update_timestamp)
    stream_id = msg.stream_id

    return CDCCheckpoint(
        format_version=msg.format_version,
        source_id=msg.source_id,
        stream_name=msg.stream_name,
        stream_id=stream_id,
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
    _validate_lease_model(lease)

    ts_msg = _wire.timestamp(lease.expiration_timestamp_ns)
    msg = state_pb2.LeaderLease(
        format_version=lease.format_version,
        source_id=lease.source_id,
        stream_name=lease.stream_name,
        owner_instance_id=lease.owner_instance_id,
        lease_epoch=lease.lease_epoch,
        heartbeat_sequence=lease.heartbeat_sequence,
        expiration_timestamp=ts_msg,
        stream_id=lease.stream_id,
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
        StateDecodeError: If bytes are malformed, unsupported, or incomplete.
    """
    if not isinstance(data, bytes):
        raise InputTypeError(
            f"data must be bytes, got {type(data).__name__}", field="data"
        )
    msg = state_pb2.LeaderLease()
    try:
        msg.ParseFromString(data)
    except (DecodeError, ValueError) as exc:
        raise StateDecodeError(
            f"{len(data)} bytes are not a parseable LeaderLease",
            reason=DecodeFailure.MALFORMED_WIRE,
        ) from exc

    if msg.format_version not in _SUPPORTED_LEASE_FORMAT_VERSIONS:
        raise StateDecodeError(
            f"LeaderLease format_version {msg.format_version} is unsupported",
            reason=DecodeFailure.UNSUPPORTED_FORMAT_VERSION,
        )
    if not msg.source_id:
        raise StateDecodeError(
            "LeaderLease missing or empty source_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.stream_name:
        raise StateDecodeError(
            "LeaderLease missing or empty stream_name",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.owner_instance_id:
        raise StateDecodeError(
            "LeaderLease missing or empty owner_instance_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not (1 <= msg.lease_epoch <= _UINT64_MAX):
        raise StateDecodeError(
            f"LeaderLease lease_epoch {msg.lease_epoch} is out of range",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.HasField("expiration_timestamp"):
        raise StateDecodeError(
            "LeaderLease missing required expiration_timestamp",
            reason=DecodeFailure.MALFORMED_WIRE,
        )

    exp_ts = _ns_from_timestamp(msg.expiration_timestamp)
    stream_id = msg.stream_id if msg.HasField("stream_id") else None

    return LeaderLease(
        format_version=msg.format_version,
        source_id=msg.source_id,
        stream_name=msg.stream_name,
        owner_instance_id=msg.owner_instance_id,
        lease_epoch=msg.lease_epoch,
        heartbeat_sequence=msg.heartbeat_sequence,
        expiration_timestamp_ns=exp_ts,
        stream_id=stream_id,
    )
