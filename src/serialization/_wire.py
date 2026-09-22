"""Protobuf messages built from checked inputs.

Nothing here validates. Every argument must already have passed `_checks`:
protobuf swallows a `None` kwarg instead of raising.
"""

from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization.model import MutationType


def timestamp(bridge_timestamp_ns: int) -> Timestamp:
    """The exact `Timestamp` of integer ns since the Unix epoch."""
    seconds, nanos = divmod(bridge_timestamp_ns, 10**9)
    return Timestamp(seconds=seconds, nanos=nanos)


def record(stream_name: str, bridge_timestamp_ns: int, **body: Message) -> bytes:
    """An `FDBMutationRecord`'s bytes: the envelope plus the `body` oneof, if any."""
    # The constructor kwarg keeps a zero Timestamp present on the wire.
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream_name,
        bridge_timestamp=timestamp(bridge_timestamp_ns),
        **body,
    ).SerializeToString()


def mutation_message(
    type_code: int, param1: bytes, param2: bytes, fdb_version: int, sequence_no: int
) -> mutations_pb2.FDBMutation:
    """One mutation at version index `(fdb_version, sequence_no)`."""
    version_index = mutations_pb2.FDBVersionIndex(
        fdb_version=fdb_version, sequence_no=sequence_no
    )
    if type_code == MutationType.CLEAR_RANGE:
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


def mutation_messages(
    natives: list[tuple[int, bytes, bytes]],
    *,
    fdb_version: int,
    first_sequence_no: int = 0,
) -> list[mutations_pb2.FDBMutation]:
    """Mutation i at version index `(fdb_version, first_sequence_no + i)`."""
    return [
        mutation_message(*native, fdb_version, first_sequence_no + i)
        for i, native in enumerate(natives)
    ]
