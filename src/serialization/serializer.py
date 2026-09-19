"""Native CDC mutations to `FDBMutationRecord` bytes."""

from collections.abc import Iterable
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization import _checks
from src.serialization.errors import InputTypeError, InputValueError
from src.serialization.model import DECLARED_TYPE_CODES, MutationType, NativeMutation

_CLEAR_RANGE = 1


def mutation_type_name(code: int) -> str:
    """Name a type code for display.

    Args:
        code: A type code, 0..255.

    Returns:
        The `MutationType` name of a declared type code, else `"UNDECLARED_<code>"`.

    Raises:
        InputTypeError: `code` is a `bool` or not an `int`.
        InputValueError: `code` is outside 0..255.
    """
    code = _checks.type_code(code, field="code")
    if code in DECLARED_TYPE_CODES:
        return MutationType(code).name
    return f"UNDECLARED_{code}"


def _attribute(mutation: NativeMutation, field: str, index: int | None) -> Any:
    try:
        return getattr(mutation, field)
    except AttributeError as error:
        raise InputTypeError(
            f"{type(mutation).__name__}{_checks.at(index)} is not a native mutation: "
            f"no {field!r} attribute",
            field=field,
            index=index,
        ) from error


def _read_native(
    mutation: NativeMutation, *, index: int | None = None
) -> tuple[int, bytes, bytes]:
    # Read each attribute once, by name, in this order. The reads are the
    # conformance check.
    type_code = _checks.type_code(_attribute(mutation, "type", index), index=index)
    param1 = _attribute(mutation, "param1", index)
    _checks.param(param1, field="param1", index=index)
    param2 = _attribute(mutation, "param2", index)
    _checks.param(param2, field="param2", index=index)
    return type_code, param1, param2


def _mutation_message(
    type_code: int, param1: bytes, param2: bytes, fdb_version: int, sequence_no: int
) -> mutations_pb2.FDBMutation:
    # Arguments must already be validated. Protobuf swallows a `None` kwarg instead
    # of raising.
    version_index = mutations_pb2.FDBVersionIndex(
        fdb_version=fdb_version, sequence_no=sequence_no
    )
    if type_code == _CLEAR_RANGE:
        return mutations_pb2.FDBMutation(
            version_index=version_index,
            clear_range=mutations_pb2.FDBClearRange(begin_key=param1, end_key=param2),
        )
    return mutations_pb2.FDBMutation(
        version_index=version_index,
        # The stubs type this open proto3 enum as closed. Undeclared type codes pass
        # through as raw ints.
        single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
            key=param1,
            value=param2,
            mutation_type=type_code,  # type: ignore[arg-type]
        ),
    )


def _timestamp(bridge_timestamp_ns: int) -> Timestamp:
    seconds, nanos = divmod(bridge_timestamp_ns, 10**9)
    return Timestamp(seconds=seconds, nanos=nanos)


def serialize_mutation(
    mutation: NativeMutation,
    *,
    fdb_version: int,
    sequence_no: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> bytes:
    """Serialize one native mutation as one record.

    Args:
        mutation: Forwarded as-is.
        fdb_version: Commit version of its version group.
        sequence_no: Its zero-based native position in the group.
        stream_name: The stream's registered name.
        bridge_timestamp_ns: Build time, integer ns since the Unix epoch.

    Returns:
        The record's bytes.

    Raises:
        InputTypeError: A keyword or attribute has the wrong Python type, or
            `mutation` lacks `type`, `param1` or `param2`.
        InputValueError: A keyword or the type code is out of range.
    """
    _checks.fdb_version(fdb_version)
    _checks.sequence_no(sequence_no)
    _checks.stream_name(stream_name)
    _checks.bridge_timestamp_ns(bridge_timestamp_ns)
    type_code, param1, param2 = _read_native(mutation)
    # The constructor kwarg keeps a zero Timestamp present on the wire.
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream_name,
        bridge_timestamp=_timestamp(bridge_timestamp_ns),
        mutation=_mutation_message(type_code, param1, param2, fdb_version, sequence_no),
    ).SerializeToString()


def serialize_batch(
    mutations: Iterable[NativeMutation],
    *,
    fdb_version: int,
    first_sequence_no: int = 0,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> bytes:
    """Serialize a contiguous slice of one version group as one batch record.

    Mutation i gets version index `(fdb_version, first_sequence_no + i)`. The caller
    must pass an unfiltered, native-order run of one group, and `first_sequence_no`
    must be the native position of its first element. Filtering first corrupts the
    dedup identity, and nothing here can detect it.

    Args:
        mutations: Consumed once, in order. Generators are fine.
        fdb_version: Commit version of the version group.
        first_sequence_no: Native position of the first mutation.
        stream_name: The stream's registered name.
        bridge_timestamp_ns: Build time, integer ns since the Unix epoch.

    Returns:
        The record's bytes.

    Raises:
        InputTypeError: A keyword or attribute has the wrong Python type, `mutations`
            is not iterable, or an element lacks `type`, `param1` or `param2`.
            `index` is the element's position.
        InputValueError: A value is out of range, `mutations` is empty, or an assigned
            position passes `MAX_SEQUENCE_NO`. The last reports
            `field == "sequence_no"` with that position as `index`.
    """
    _checks.fdb_version(fdb_version)
    _checks.sequence_no(first_sequence_no, field="first_sequence_no")
    _checks.stream_name(stream_name)
    _checks.bridge_timestamp_ns(bridge_timestamp_ns)
    natives: list[tuple[int, bytes, bytes]] = []
    for index, mutation in enumerate(_checks.mutations(mutations)):
        _checks.assigned_sequence_no(first_sequence_no + index, index=index)
        natives.append(_read_native(mutation, index=index))
    if not natives:
        # An empty batch carries no version, so a reader cannot tell which group
        # it belongs to.
        raise InputValueError(
            "mutations must yield at least one native mutation", field="mutations"
        )
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream_name,
        bridge_timestamp=_timestamp(bridge_timestamp_ns),
        batch=mutations_pb2.FDBMutationBatch(
            mutations=[
                _mutation_message(*native, fdb_version, first_sequence_no + i)
                for i, native in enumerate(natives)
            ]
        ),
    ).SerializeToString()


def serialize_version_end(
    *,
    fdb_version: int,
    total_mutations: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> bytes:
    """Serialize the version end that closes the group at `fdb_version`.

    Args:
        fdb_version: Commit version of the completed version group.
        total_mutations: Native mutations in the whole group, however it was sliced.
            `0` means none at this version.
        stream_name: The stream's registered name.
        bridge_timestamp_ns: Build time, integer ns since the Unix epoch.

    Returns:
        The record's bytes.

    Raises:
        InputTypeError: A keyword has the wrong Python type.
        InputValueError: A keyword is out of range.
    """
    _checks.fdb_version(fdb_version)
    _checks.total_mutations(total_mutations)
    _checks.stream_name(stream_name)
    _checks.bridge_timestamp_ns(bridge_timestamp_ns)
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream_name,
        bridge_timestamp=_timestamp(bridge_timestamp_ns),
        version_end=mutations_pb2.VersionEnd(
            fdb_version=fdb_version,
            total_mutations=total_mutations,
            bridge_timestamp=_timestamp(bridge_timestamp_ns),
        ),
    ).SerializeToString()
