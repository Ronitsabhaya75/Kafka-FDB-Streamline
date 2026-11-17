"""Native CDC mutations to Protobuf `FDBMutationRecord` bytes and back.

Public contract: [`docs/SERIALIZATION-SPEC.md`](../../docs/SERIALIZATION-SPEC.md).
"""

from fdb_kafka_bridge.serialization.deserializer import deserialize_record
from fdb_kafka_bridge.serialization.errors import (
    DecodeFailure,
    InputTypeError,
    InputValueError,
    RecordDecodeError,
    RecordTooLargeError,
    SerializationError,
)
from fdb_kafka_bridge.serialization.model import (
    CDCCheckpoint,
    FDBDirectoryContext,
    KafkaSinkIdentity,
    LeaderLease,
    Mutation,
    MutationBatch,
    MutationType,
    NativeMutation,
    Record,
    RecordBody,
    VersionEnd,
    VersionIndex,
    deduplication_identity,
    legacy_deduplication_identity,
)
from fdb_kafka_bridge.serialization.serializer import (
    serialize_batch,
    serialize_mutation,
    serialize_version_end,
)
from fdb_kafka_bridge.serialization.state import (
    deserialize_checkpoint,
    deserialize_leader_lease,
    serialize_checkpoint,
    serialize_leader_lease,
)
from fdb_kafka_bridge.serialization.version_group import serialize_version_group

__all__ = [
    "CDCCheckpoint",
    "DecodeFailure",
    "FDBDirectoryContext",
    "InputTypeError",
    "InputValueError",
    "KafkaSinkIdentity",
    "LeaderLease",
    "Mutation",
    "MutationBatch",
    "MutationType",
    "NativeMutation",
    "Record",
    "RecordBody",
    "RecordDecodeError",
    "RecordTooLargeError",
    "SerializationError",
    "VersionEnd",
    "VersionIndex",
    "deduplication_identity",
    "deserialize_checkpoint",
    "deserialize_leader_lease",
    "deserialize_record",
    "legacy_deduplication_identity",
    "serialize_batch",
    "serialize_checkpoint",
    "serialize_leader_lease",
    "serialize_mutation",
    "serialize_version_end",
    "serialize_version_group",
]
