"""Native CDC mutations to `FDBMutationRecord` bytes."""

from collections.abc import Iterable

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization import _checks, _wire
from src.serialization.errors import InputValueError
from src.serialization.model import NativeMutation


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
        mutation: One native mutation, read through its `type`, `param1` and
            `param2` attributes (`NativeMutation`; upstream `CdcMutation` conforms).
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
    _checks.envelope(stream_name, bridge_timestamp_ns)
    native = _checks.native(mutation)
    return _wire.record(
        stream_name,
        bridge_timestamp_ns,
        mutation=_wire.mutation_message(*native, fdb_version, sequence_no),
    )


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
            position falls outside `0..2**32-1`. The last reports
            `field == "sequence_no"` with that position as `index`.
    """
    _checks.fdb_version(fdb_version)
    _checks.sequence_no(first_sequence_no, field="first_sequence_no")
    _checks.envelope(stream_name, bridge_timestamp_ns)
    natives = _checks.natives(mutations, first_sequence_no=first_sequence_no)
    if not natives:
        # An empty batch carries no version, so a reader cannot tell which group
        # it belongs to.
        raise InputValueError(
            "mutations must yield at least one native mutation", field="mutations"
        )
    return _wire.record(
        stream_name,
        bridge_timestamp_ns,
        batch=mutations_pb2.FDBMutationBatch(
            mutations=_wire.mutation_messages(
                natives, fdb_version=fdb_version, first_sequence_no=first_sequence_no
            )
        ),
    )


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
    _checks.envelope(stream_name, bridge_timestamp_ns)
    timestamp = _wire.timestamp(bridge_timestamp_ns)
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream_name,
        bridge_timestamp=timestamp,
        version_end=mutations_pb2.VersionEnd(
            fdb_version=fdb_version,
            total_mutations=total_mutations,
            bridge_timestamp=timestamp,
        ),
    ).SerializeToString()
