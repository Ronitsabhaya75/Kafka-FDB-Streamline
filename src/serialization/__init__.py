"""Native CDC mutations to Protobuf `FDBMutationRecord` bytes and back."""

from src.serialization.deserializer import deserialize_record
from src.serialization.errors import (
    DecodeFailure,
    InputTypeError,
    InputValueError,
    RecordDecodeError,
    RecordTooLargeError,
    SerializationError,
)
from src.serialization.model import (
    DECLARED_TYPE_CODES,
    MAX_BRIDGE_TIMESTAMP_NS,
    MAX_SEQUENCE_NO,
    MAX_TOTAL_MUTATIONS,
    MAX_TYPE_CODE,
    MAX_VERSION,
    Mutation,
    MutationBatch,
    MutationType,
    NativeMutation,
    Record,
    RecordBody,
    VersionEnd,
    VersionIndex,
)
from src.serialization.serializer import (
    mutation_type_name,
    serialize_batch,
    serialize_mutation,
    serialize_version_end,
)
from src.serialization.version_group import serialize_version_group

__all__ = [
    "DecodeFailure",
    "InputTypeError",
    "InputValueError",
    "RecordDecodeError",
    "RecordTooLargeError",
    "SerializationError",
    "DECLARED_TYPE_CODES",
    "MAX_BRIDGE_TIMESTAMP_NS",
    "MAX_SEQUENCE_NO",
    "MAX_TOTAL_MUTATIONS",
    "MAX_TYPE_CODE",
    "MAX_VERSION",
    "Mutation",
    "MutationBatch",
    "MutationType",
    "NativeMutation",
    "Record",
    "RecordBody",
    "VersionEnd",
    "VersionIndex",
    "deserialize_record",
    "mutation_type_name",
    "serialize_batch",
    "serialize_mutation",
    "serialize_version_end",
    "serialize_version_group",
]
