from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FDBVersionIndex(_message.Message):
    __slots__ = ("fdb_version", "sequence_no")
    FDB_VERSION_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NO_FIELD_NUMBER: _ClassVar[int]
    fdb_version: int
    sequence_no: int
    def __init__(self, fdb_version: _Optional[int] = ..., sequence_no: _Optional[int] = ...) -> None: ...

class VersionEnd(_message.Message):
    __slots__ = ("fdb_version", "total_mutations", "bridge_timestamp")
    FDB_VERSION_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MUTATIONS_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    fdb_version: int
    total_mutations: int
    bridge_timestamp: _timestamp_pb2.Timestamp
    def __init__(self, fdb_version: _Optional[int] = ..., total_mutations: _Optional[int] = ..., bridge_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FDBSingleKeyMutation(_message.Message):
    __slots__ = ("key", "value", "mutation_type")
    class MutationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MUTATION_TYPE_SET_VALUE: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_CLEAR_RANGE: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_ADD: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_AND: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_OR: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_XOR: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_APPEND_IF_FITS: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_MAX: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_MIN: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_SET_VERSIONSTAMPED_KEY: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_BYTE_MIN: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_BYTE_MAX: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_MIN_V2: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_AND_V2: _ClassVar[FDBSingleKeyMutation.MutationType]
        MUTATION_TYPE_COMPARE_AND_CLEAR: _ClassVar[FDBSingleKeyMutation.MutationType]
    MUTATION_TYPE_SET_VALUE: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_CLEAR_RANGE: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_ADD: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_AND: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_OR: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_XOR: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_APPEND_IF_FITS: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_MAX: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_MIN: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_SET_VERSIONSTAMPED_KEY: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_BYTE_MIN: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_BYTE_MAX: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_MIN_V2: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_AND_V2: FDBSingleKeyMutation.MutationType
    MUTATION_TYPE_COMPARE_AND_CLEAR: FDBSingleKeyMutation.MutationType
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    mutation_type: FDBSingleKeyMutation.MutationType
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ..., mutation_type: _Optional[_Union[FDBSingleKeyMutation.MutationType, str]] = ...) -> None: ...

class FDBClearRange(_message.Message):
    __slots__ = ("begin_key", "end_key")
    BEGIN_KEY_FIELD_NUMBER: _ClassVar[int]
    END_KEY_FIELD_NUMBER: _ClassVar[int]
    begin_key: bytes
    end_key: bytes
    def __init__(self, begin_key: _Optional[bytes] = ..., end_key: _Optional[bytes] = ...) -> None: ...

class FDBMutation(_message.Message):
    __slots__ = ("version_index", "single_key_mutation", "clear_range")
    VERSION_INDEX_FIELD_NUMBER: _ClassVar[int]
    SINGLE_KEY_MUTATION_FIELD_NUMBER: _ClassVar[int]
    CLEAR_RANGE_FIELD_NUMBER: _ClassVar[int]
    version_index: FDBVersionIndex
    single_key_mutation: FDBSingleKeyMutation
    clear_range: FDBClearRange
    def __init__(self, version_index: _Optional[_Union[FDBVersionIndex, _Mapping]] = ..., single_key_mutation: _Optional[_Union[FDBSingleKeyMutation, _Mapping]] = ..., clear_range: _Optional[_Union[FDBClearRange, _Mapping]] = ...) -> None: ...

class FDBMutationBatch(_message.Message):
    __slots__ = ("mutations",)
    MUTATIONS_FIELD_NUMBER: _ClassVar[int]
    mutations: _containers.RepeatedCompositeFieldContainer[FDBMutation]
    def __init__(self, mutations: _Optional[_Iterable[_Union[FDBMutation, _Mapping]]] = ...) -> None: ...

class FDBMutationRecord(_message.Message):
    __slots__ = ("stream_name", "bridge_timestamp", "mutation", "version_end", "batch")
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    VERSION_END_FIELD_NUMBER: _ClassVar[int]
    BATCH_FIELD_NUMBER: _ClassVar[int]
    stream_name: str
    bridge_timestamp: _timestamp_pb2.Timestamp
    mutation: FDBMutation
    version_end: VersionEnd
    batch: FDBMutationBatch
    def __init__(self, stream_name: _Optional[str] = ..., bridge_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., mutation: _Optional[_Union[FDBMutation, _Mapping]] = ..., version_end: _Optional[_Union[VersionEnd, _Mapping]] = ..., batch: _Optional[_Union[FDBMutationBatch, _Mapping]] = ...) -> None: ...
