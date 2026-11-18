"""`FDBMutationRecord` bytes to immutable output values."""

from google.protobuf.message import DecodeError

from fdb_kafka_bridge.serialization import _checks
from fdb_kafka_bridge.serialization.errors import (
    DecodeFailure,
    InputTypeError,
    RecordDecodeError,
)
from fdb_kafka_bridge.serialization.model import (
    _MAX_TYPE_CODE,
    _MAX_VERSION,
    FDBDirectoryContext,
    Mutation,
    MutationBatch,
    MutationType,
    Record,
    RecordBody,
    VersionEnd,
    VersionIndex,
)
from fdbkafka.cdc.v1 import mutations_pb2


def _check_version(fdb_version: int, index: int | None) -> None:
    # The wire field is uint64 but a native commit version is int64_t. Anything
    # larger cannot be serialized again.
    if fdb_version > _MAX_VERSION:
        raise RecordDecodeError(
            f"fdb_version {fdb_version}"
            f"{_checks.at(index, label='batch position')} exceeds {_MAX_VERSION}",
            reason=DecodeFailure.VERSION_OUT_OF_RANGE,
            index=index,
        )


def _mutation(message: mutations_pb2.FDBMutation, index: int | None) -> Mutation:
    # Check presence. An absent version index would otherwise read as (0, 0).
    if not message.HasField("version_index"):
        raise RecordDecodeError(
            f"mutation{_checks.at(index, label='batch position')} has no version index",
            reason=DecodeFailure.MISSING_VERSION_INDEX,
            index=index,
        )
    version_index = VersionIndex(
        message.version_index.fdb_version, message.version_index.sequence_no
    )
    _check_version(version_index.fdb_version, index)
    arm = message.WhichOneof("mutation")
    if arm is None:
        raise RecordDecodeError(
            f"mutation{_checks.at(index, label='batch position')} {version_index} "
            f"has no mutation arm",
            reason=DecodeFailure.NO_MUTATION_ARM,
            index=index,
        )
    if arm == "clear_range":
        # The clear_range arm has no type field, so the type code is synthesised.
        # `.value` because output values carry a plain int, never the enum.
        clear = message.clear_range
        code = MutationType.CLEAR_RANGE.value
        return Mutation(code, clear.begin_key, clear.end_key, version_index)
    single = message.single_key_mutation
    # mutation_type is an open enum, so any int32 parses. Only 0..255 is a type code.
    type_code = single.mutation_type
    if not 0 <= type_code <= _MAX_TYPE_CODE:
        raise RecordDecodeError(
            f"mutation{_checks.at(index, label='batch position')} {version_index} "
            f"has type code {type_code}, outside 0..{_MAX_TYPE_CODE}",
            reason=DecodeFailure.TYPE_CODE_OUT_OF_RANGE,
            index=index,
        )
    return Mutation(type_code, single.key, single.value, version_index)


def _batch(message: mutations_pb2.FDBMutationBatch) -> MutationBatch:
    mutations = tuple(_mutation(m, i) for i, m in enumerate(message.mutations))
    if not mutations:
        raise RecordDecodeError(
            "batch has zero mutations", reason=DecodeFailure.EMPTY_BATCH
        )
    first = mutations[0].version_index
    for i, mutation in enumerate(mutations):
        if mutation.version_index != (first.fdb_version, first.sequence_no + i):
            raise RecordDecodeError(
                f"batch position {i} has {mutation.version_index}; the batch "
                f"starts at {first}",
                reason=DecodeFailure.INCONSISTENT_BATCH,
                index=i,
            )
    return MutationBatch(mutations)


def _body(message: mutations_pb2.FDBMutationRecord) -> RecordBody:
    arm = message.WhichOneof("record")
    if arm is None:
        # A record body added by a newer schema lands here too. This reader sees
        # it as an unknown field.
        raise RecordDecodeError(
            "record has no record body", reason=DecodeFailure.NO_RECORD_ARM
        )
    if arm == "batch":
        return _batch(message.batch)
    if arm == "version_end":
        end = message.version_end
        _check_version(end.fdb_version, None)
        return VersionEnd(
            end.fdb_version, end.total_mutations, _bridge_timestamp_ns(end)
        )
    return _mutation(message.mutation, None)


def _bridge_timestamp_ns(
    message: mutations_pb2.FDBMutationRecord | mutations_pb2.VersionEnd,
) -> int | None:
    if not message.HasField("bridge_timestamp"):
        return None
    # Read the two fields directly. The Timestamp helpers range-check, and a
    # timestamp must never fail a read.
    timestamp = message.bridge_timestamp
    return timestamp.seconds * 10**9 + timestamp.nanos


def _verify_mutation_in_directory(
    mutation: Mutation,
    prefix: bytes,
    index: int | None,
) -> None:
    if mutation.type == MutationType.CLEAR_RANGE.value:
        if not _checks.is_range_within_subspace(
            mutation.param1, mutation.param2, prefix
        ):
            raise RecordDecodeError(
                f"clear range{_checks.at(index, label='batch position')} "
                "falls outside directory subspace prefix",
                reason=DecodeFailure.MALFORMED_WIRE,
                index=index,
            )
    else:
        if not mutation.param1.startswith(prefix):
            raise RecordDecodeError(
                f"mutation key{_checks.at(index, label='batch position')} "
                "does not start with directory subspace prefix",
                reason=DecodeFailure.MALFORMED_WIRE,
                index=index,
            )


def _verify_directory_bounds(
    body: RecordBody,
    directory: FDBDirectoryContext,
) -> None:
    prefix = directory.subspace_prefix
    if isinstance(body, Mutation):
        _verify_mutation_in_directory(body, prefix, None)
    elif isinstance(body, MutationBatch):
        for i, mutation in enumerate(body.mutations):
            _verify_mutation_in_directory(mutation, prefix, i)


def deserialize_record(data: bytes) -> Record:
    """Parse one Kafka record value.

    The record body, version indexes, type codes and batch shape are checked.
    Every returned `Mutation` / `MutationBatch` can be serialized again
    (`Mutation` satisfies `NativeMutation`). A returned `VersionEnd` may have
    `bridge_timestamp_ns` of `None` or outside the serializer's range, so it is
    not always a valid input to `serialize_version_end`. The envelope timestamp
    is returned as found and never fails the call. `stream_name` may be `""`.

    Args:
        data: The bytes of one record.

    Returns:
        The complete `Record`.

    Raises:
        InputTypeError: `data` is not `bytes`.
        RecordDecodeError: The bytes are an undecodable record. `reason` names the
            failed check and `index` the batch position, if any. For
            `MALFORMED_WIRE` the protobuf exception is the `__cause__`.
    """
    if not isinstance(data, bytes):
        raise InputTypeError(
            f"data must be bytes, got {type(data).__name__}", field="data"
        )
    message = mutations_pb2.FDBMutationRecord()
    try:
        message.ParseFromString(data)
    except (DecodeError, ValueError) as exc:
        # On invalid UTF-8, upb raises DecodeError and the pure-python backend
        # raises UnicodeDecodeError, a ValueError.
        raise RecordDecodeError(
            f"{len(data)} bytes are not a parseable FDBMutationRecord",
            reason=DecodeFailure.MALFORMED_WIRE,
        ) from exc
    directory: FDBDirectoryContext | None = None
    if message.HasField("directory"):
        if not message.directory.subspace_prefix or not message.directory.path:
            raise RecordDecodeError(
                "directory context on wire has empty path or subspace prefix",
                reason=DecodeFailure.MALFORMED_WIRE,
            )
        directory = FDBDirectoryContext(
            path=tuple(message.directory.path),
            subspace_prefix=message.directory.subspace_prefix,
        )
    stream_id = message.stream_id if message.HasField("stream_id") else None
    source_id = (
        message.source_id
        if (message.HasField("source_id") and message.source_id)
        else None
    )
    body = _body(message)
    if directory is not None:
        if isinstance(body, VersionEnd):
            raise RecordDecodeError(
                "VersionEnd record must not carry directory context",
                reason=DecodeFailure.MALFORMED_WIRE,
            )
        _verify_directory_bounds(body, directory)
    return Record(
        stream_name=message.stream_name,
        bridge_timestamp_ns=_bridge_timestamp_ns(message),
        body=body,
        directory=directory,
        stream_id=stream_id,
        source_id=source_id,
    )
