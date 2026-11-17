import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class KafkaSinkIdentity(_message.Message):
    __slots__ = ("cluster_id", "topic")
    CLUSTER_ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    cluster_id: str
    topic: str
    def __init__(self, cluster_id: _Optional[str] = ..., topic: _Optional[str] = ...) -> None: ...

class CDCCheckpoint(_message.Message):
    __slots__ = ("format_version", "source_id", "stream_name", "stream_id", "last_consumed_version", "sink", "updating_instance_id", "update_timestamp", "lease_epoch", "range_config_fingerprint")
    FORMAT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_CONSUMED_VERSION_FIELD_NUMBER: _ClassVar[int]
    SINK_FIELD_NUMBER: _ClassVar[int]
    UPDATING_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATE_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    LEASE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RANGE_CONFIG_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    format_version: int
    source_id: str
    stream_name: str
    stream_id: int
    last_consumed_version: int
    sink: KafkaSinkIdentity
    updating_instance_id: str
    update_timestamp: _timestamp_pb2.Timestamp
    lease_epoch: int
    range_config_fingerprint: bytes
    def __init__(self, format_version: _Optional[int] = ..., source_id: _Optional[str] = ..., stream_name: _Optional[str] = ..., stream_id: _Optional[int] = ..., last_consumed_version: _Optional[int] = ..., sink: _Optional[_Union[KafkaSinkIdentity, _Mapping]] = ..., updating_instance_id: _Optional[str] = ..., update_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., lease_epoch: _Optional[int] = ..., range_config_fingerprint: _Optional[bytes] = ...) -> None: ...

class LeaderLease(_message.Message):
    __slots__ = ("format_version", "source_id", "stream_name", "owner_instance_id", "lease_epoch", "heartbeat_sequence", "expiration_timestamp", "stream_id")
    FORMAT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    EXPIRATION_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    format_version: int
    source_id: str
    stream_name: str
    owner_instance_id: str
    lease_epoch: int
    heartbeat_sequence: int
    expiration_timestamp: _timestamp_pb2.Timestamp
    stream_id: int
    def __init__(self, format_version: _Optional[int] = ..., source_id: _Optional[str] = ..., stream_name: _Optional[str] = ..., owner_instance_id: _Optional[str] = ..., lease_epoch: _Optional[int] = ..., heartbeat_sequence: _Optional[int] = ..., expiration_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stream_id: _Optional[int] = ...) -> None: ...
