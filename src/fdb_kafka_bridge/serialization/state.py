"""Serialization and deserialization for v0.2.0 bridge control-plane state."""

from __future__ import annotations

from typing import Final

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

CURRENT_FORMAT_VERSION: Final[int] = 1
SUPPORTED_FORMAT_VERSIONS: Final[frozenset[int]] = frozenset({CURRENT_FORMAT_VERSION})
INT64_MAX: Final[int] = (1 << 63) - 1
UINT64_MAX: Final[int] = (1 << 64) - 1
MAX_TIMESTAMP_SECONDS: Final[int] = 253_402_300_799
MAX_TIMESTAMP_NS: Final[int] = 253_402_300_799_999_999_999
NANOS_PER_SECOND: Final[int] = 10**9


def _timestamp_from_ns(
    timestamp_ns: int,
    *,
    field: str = "timestamp",
) -> Timestamp:
    """Convert integer Unix nanoseconds to google.protobuf.Timestamp.

    Args:
        timestamp_ns: Integer Unix nanoseconds since epoch.
        field: Name of the field for error reporting.

    Returns:
        google.protobuf.Timestamp.

    Raises:
        InputTypeError: If timestamp_ns is not int.
        InputValueError: If timestamp_ns is negative or exceeds MAX_TIMESTAMP_NS.
    """
    if type(timestamp_ns) is not int:
        raise InputTypeError(
            f"{field} must be int, got {type(timestamp_ns).__name__}",
            field=field,
        )
    if not (0 <= timestamp_ns <= MAX_TIMESTAMP_NS):
        raise InputValueError(
            f"{field} must be in 0..{MAX_TIMESTAMP_NS}, got {timestamp_ns}",
            field=field,
        )
    seconds, nanos = divmod(timestamp_ns, NANOS_PER_SECOND)
    return Timestamp(seconds=seconds, nanos=nanos)


def _ns_from_timestamp(msg: Timestamp) -> int:
    """Convert google.protobuf.Timestamp to integer Unix nanoseconds.

    Args:
        msg: Protobuf Timestamp message.

    Returns:
        Integer nanoseconds since Unix epoch.

    Raises:
        RecordDecodeError: If timestamp is malformed or out of range.
    """
    if not (0 <= msg.nanos < NANOS_PER_SECOND):
        raise RecordDecodeError(
            f"Invalid timestamp nanos: {msg.nanos}",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not (0 <= msg.seconds <= MAX_TIMESTAMP_SECONDS):
        raise RecordDecodeError(
            f"Invalid timestamp seconds: {msg.seconds}",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    return msg.seconds * NANOS_PER_SECOND + msg.nanos


def validate_checkpoint_model(checkpoint: CDCCheckpoint) -> None:
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
    if type(checkpoint.format_version) is not int:
        raise InputTypeError(
            f"format_version must be int, got "
            f"{type(checkpoint.format_version).__name__}",
            field="format_version",
        )
    if checkpoint.format_version not in SUPPORTED_FORMAT_VERSIONS:
        supported = sorted(SUPPORTED_FORMAT_VERSIONS)
        raise InputValueError(
            f"format_version must be in {supported}, got {checkpoint.format_version!r}",
            field="format_version",
        )
    if not isinstance(checkpoint.source_id, str):
        raise InputTypeError("source_id must be str", field="source_id")
    if not checkpoint.source_id:
        raise InputValueError("source_id must be non-empty str", field="source_id")

    if not isinstance(checkpoint.stream_name, str):
        raise InputTypeError("stream_name must be str", field="stream_name")
    if not checkpoint.stream_name:
        raise InputValueError("stream_name must be non-empty str", field="stream_name")

    if type(checkpoint.stream_id) is not int:
        raise InputTypeError("stream_id must be int", field="stream_id")
    if not (0 <= checkpoint.stream_id <= UINT64_MAX):
        raise InputValueError(
            f"stream_id must be in 0..{UINT64_MAX}, got {checkpoint.stream_id}",
            field="stream_id",
        )

    if type(checkpoint.last_consumed_version) is not int:
        raise InputTypeError(
            "last_consumed_version must be int", field="last_consumed_version"
        )
    if not (0 <= checkpoint.last_consumed_version <= INT64_MAX):
        raise InputValueError(
            f"last_consumed_version must be in 0..{INT64_MAX}, "
            f"got {checkpoint.last_consumed_version}",
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

    if not isinstance(checkpoint.updating_instance_id, str):
        raise InputTypeError(
            "updating_instance_id must be str", field="updating_instance_id"
        )
    if not checkpoint.updating_instance_id:
        raise InputValueError(
            "updating_instance_id must be non-empty str",
            field="updating_instance_id",
        )

    if type(checkpoint.update_timestamp_ns) is not int:
        raise InputTypeError(
            "update_timestamp_ns must be int",
            field="update_timestamp_ns",
        )
    if not (0 <= checkpoint.update_timestamp_ns <= MAX_TIMESTAMP_NS):
        raise InputValueError(
            f"update_timestamp_ns must be in 0..{MAX_TIMESTAMP_NS}, "
            f"got {checkpoint.update_timestamp_ns}",
            field="update_timestamp_ns",
        )

    if type(checkpoint.lease_epoch) is not int:
        raise InputTypeError("lease_epoch must be int", field="lease_epoch")
    if not (0 <= checkpoint.lease_epoch <= UINT64_MAX):
        raise InputValueError(
            f"lease_epoch must be in 0..{UINT64_MAX}, got {checkpoint.lease_epoch}",
            field="lease_epoch",
        )

    if not isinstance(checkpoint.range_config_fingerprint, str):
        raise InputTypeError(
            "range_config_fingerprint must be str",
            field="range_config_fingerprint",
        )


def validate_lease_model(lease: LeaderLease) -> None:
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
    if not isinstance(lease.source_id, str):
        raise InputTypeError("source_id must be str", field="source_id")
    if not lease.source_id:
        raise InputValueError("source_id must be non-empty str", field="source_id")

    if not isinstance(lease.stream_name, str):
        raise InputTypeError("stream_name must be str", field="stream_name")
    if not lease.stream_name:
        raise InputValueError("stream_name must be non-empty str", field="stream_name")

    if not isinstance(lease.owner_instance_id, str):
        raise InputTypeError("owner_instance_id must be str", field="owner_instance_id")
    if not lease.owner_instance_id:
        raise InputValueError(
            "owner_instance_id must be non-empty str",
            field="owner_instance_id",
        )

    if type(lease.lease_epoch) is not int:
        raise InputTypeError("lease_epoch must be int", field="lease_epoch")
    if not (0 <= lease.lease_epoch <= UINT64_MAX):
        raise InputValueError(
            f"lease_epoch must be in 0..{UINT64_MAX}, got {lease.lease_epoch}",
            field="lease_epoch",
        )

    if type(lease.heartbeat_sequence) is not int:
        raise InputTypeError(
            "heartbeat_sequence must be int", field="heartbeat_sequence"
        )
    if not (0 <= lease.heartbeat_sequence <= UINT64_MAX):
        raise InputValueError(
            f"heartbeat_sequence must be in 0..{UINT64_MAX}, "
            f"got {lease.heartbeat_sequence}",
            field="heartbeat_sequence",
        )

    if type(lease.expiration_timestamp_ns) is not int:
        raise InputTypeError(
            "expiration_timestamp_ns must be int",
            field="expiration_timestamp_ns",
        )
    if not (0 <= lease.expiration_timestamp_ns <= MAX_TIMESTAMP_NS):
        raise InputValueError(
            f"expiration_timestamp_ns must be in 0..{MAX_TIMESTAMP_NS}, "
            f"got {lease.expiration_timestamp_ns}",
            field="expiration_timestamp_ns",
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
    validate_checkpoint_model(checkpoint)

    sink_msg = state_pb2.KafkaSinkIdentity(
        cluster_id=checkpoint.sink.cluster_id,
        topic=checkpoint.sink.topic,
    )
    ts_msg = _timestamp_from_ns(
        checkpoint.update_timestamp_ns, field="update_timestamp_ns"
    )

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
        RecordDecodeError: If bytes are malformed, unsupported, or incomplete.
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

    if msg.format_version not in SUPPORTED_FORMAT_VERSIONS:
        raise RecordDecodeError(
            f"Checkpoint format_version {msg.format_version} is unsupported",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.source_id:
        raise RecordDecodeError(
            "Checkpoint missing or empty source_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.stream_name:
        raise RecordDecodeError(
            "Checkpoint missing or empty stream_name",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not (0 <= msg.last_consumed_version <= INT64_MAX):
        raise RecordDecodeError(
            f"Checkpoint last_consumed_version {msg.last_consumed_version} "
            f"exceeds INT64_MAX",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.updating_instance_id:
        raise RecordDecodeError(
            "Checkpoint missing or empty updating_instance_id",
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
    if not msg.HasField("update_timestamp"):
        raise RecordDecodeError(
            "Checkpoint missing required update_timestamp",
            reason=DecodeFailure.MALFORMED_WIRE,
        )

    sink = KafkaSinkIdentity(cluster_id=msg.sink.cluster_id, topic=msg.sink.topic)
    update_ts = _ns_from_timestamp(msg.update_timestamp)

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
    validate_lease_model(lease)

    ts_msg = _timestamp_from_ns(
        lease.expiration_timestamp_ns, field="expiration_timestamp_ns"
    )
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
        RecordDecodeError: If bytes are malformed, unsupported, or incomplete.
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

    if not msg.source_id:
        raise RecordDecodeError(
            "LeaderLease missing or empty source_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.stream_name:
        raise RecordDecodeError(
            "LeaderLease missing or empty stream_name",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.owner_instance_id:
        raise RecordDecodeError(
            "LeaderLease missing or empty owner_instance_id",
            reason=DecodeFailure.MALFORMED_WIRE,
        )
    if not msg.HasField("expiration_timestamp"):
        raise RecordDecodeError(
            "LeaderLease missing required expiration_timestamp",
            reason=DecodeFailure.MALFORMED_WIRE,
        )

    exp_ts = _ns_from_timestamp(msg.expiration_timestamp)

    return LeaderLease(
        source_id=msg.source_id,
        stream_name=msg.stream_name,
        owner_instance_id=msg.owner_instance_id,
        lease_epoch=msg.lease_epoch,
        heartbeat_sequence=msg.heartbeat_sequence,
        expiration_timestamp_ns=exp_ts,
    )
