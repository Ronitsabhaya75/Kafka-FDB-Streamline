"""Shared testing utilities for Kafka producer."""

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2


def make_timestamp() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def build_set_mutation_record(
    stream: str,
    version: int,
    seq: int,
    key: bytes,
    value: bytes,
) -> mutations_pb2.FDBMutationRecord:
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream,
        bridge_timestamp=make_timestamp(),
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=version,
                sequence_no=seq,
            ),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=key,
                value=value,
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        ),
    )


def build_version_end_record(
    stream: str,
    version: int,
    total: int,
) -> mutations_pb2.FDBMutationRecord:
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream,
        bridge_timestamp=make_timestamp(),
        version_end=mutations_pb2.VersionEnd(
            fdb_version=version,
            total_mutations=total,
            bridge_timestamp=make_timestamp(),
        ),
    )


def build_batch_record(
    stream: str,
    version: int,
    count: int,
) -> mutations_pb2.FDBMutationRecord:
    mutations = [
        mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=version,
                sequence_no=i,
            ),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=f"batch:{i}".encode(),
                value=f"val:{i}".encode(),
                mutation_type=mutations_pb2.FDBSingleKeyMutation.MutationType.MUTATION_TYPE_SET_VALUE,
            ),
        )
        for i in range(count)
    ]
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream,
        bridge_timestamp=make_timestamp(),
        batch=mutations_pb2.FDBMutationBatch(mutations=mutations),
    )
