from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from hypothesis import given
from hypothesis import strategies as st

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization import (
    DecodeFailure,
    InputTypeError,
    Mutation,
    MutationBatch,
    Record,
    RecordBody,
    RecordDecodeError,
    VersionEnd,
    VersionIndex,
    deserialize_record,
    serialize_mutation,
)
from tests.serialization.doubles import CLEAR, ENV, SET, STREAM, TS, V
from tests.serialization.test_golden_bytes import GOLDEN

ENVELOPE_ONLY = mutations_pb2.FDBMutationRecord(
    stream_name=STREAM, bridge_timestamp=Timestamp(seconds=1_700_000_000, nanos=5)
).SerializeToString()


def wire_single(
    code: int, fdb_version: int, sequence_no: int
) -> mutations_pb2.FDBMutation:
    return mutations_pb2.FDBMutation(
        version_index=mutations_pb2.FDBVersionIndex(
            fdb_version=fdb_version, sequence_no=sequence_no
        ),
        single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
            key=b"k",
            value=b"v",
            mutation_type=code,  # type: ignore[arg-type]  # raw int, open enum
        ),
    )


def wire_set(fdb_version: int, sequence_no: int) -> mutations_pb2.FDBMutation:
    return wire_single(0, fdb_version, sequence_no)


def batch_record(*mutations: mutations_pb2.FDBMutation) -> bytes:
    return mutations_pb2.FDBMutationRecord(
        batch=mutations_pb2.FDBMutationBatch(mutations=mutations)
    ).SerializeToString()


@pytest.mark.parametrize("data", [None, "x", bytearray(b""), memoryview(b"")])
def test_non_bytes_data_is_an_input_type_error(data: Any) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.field == "data"
    assert excinfo.value.index is None


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"", id="empty"),
        pytest.param(
            mutations_pb2.FDBVersionIndex(
                fdb_version=5, sequence_no=1
            ).SerializeToString(),
            id="foreign-proto",
        ),
        pytest.param(ENVELOPE_ONLY, id="envelope-only"),
        pytest.param(ENVELOPE_ONLY + b"\x7a\x00", id="future-arm"),
    ],
)
def test_record_without_a_known_record_body_is_no_record_arm(data: bytes) -> None:
    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.NO_RECORD_ARM
    assert excinfo.value.index is None


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"\x0f\x01\x02\x03", id="invalid-wire-type"),
        pytest.param(b"\x0a\x02\xff\xfe", id="invalid-utf8-stream-name"),
    ],
)
def test_unparseable_bytes_are_malformed_wire(data: bytes) -> None:
    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.MALFORMED_WIRE
    assert excinfo.value.index is None
    assert excinfo.value.__cause__ is not None


def test_mutation_without_version_index_is_missing_version_index() -> None:
    data = mutations_pb2.FDBMutationRecord(
        mutation=mutations_pb2.FDBMutation(
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(key=b"k")
        )
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.MISSING_VERSION_INDEX
    assert excinfo.value.index is None


def test_mutation_without_an_inner_arm_is_no_mutation_arm() -> None:
    data = mutations_pb2.FDBMutationRecord(
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=V, sequence_no=0)
        )
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.NO_MUTATION_ARM
    assert excinfo.value.index is None


@pytest.mark.parametrize(
    ("faulty", "reason"),
    [
        pytest.param(
            mutations_pb2.FDBMutation(
                single_key_mutation=mutations_pb2.FDBSingleKeyMutation(key=b"k")
            ),
            DecodeFailure.MISSING_VERSION_INDEX,
            id="no-version-index",
        ),
        pytest.param(
            mutations_pb2.FDBMutation(
                version_index=mutations_pb2.FDBVersionIndex(
                    fdb_version=V, sequence_no=1
                )
            ),
            DecodeFailure.NO_MUTATION_ARM,
            id="no-inner-arm",
        ),
    ],
)
def test_per_mutation_fault_in_a_batch_reports_its_position(
    faulty: mutations_pb2.FDBMutation, reason: DecodeFailure
) -> None:
    data = batch_record(wire_set(V, 0), faulty, wire_set(V, 2))

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is reason
    assert excinfo.value.index == 1


@pytest.mark.parametrize("code", [300, -1])
def test_wire_type_code_outside_uint8_is_type_code_out_of_range(code: int) -> None:
    data = mutations_pb2.FDBMutationRecord(
        mutation=wire_single(code, V, 0)
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.TYPE_CODE_OUT_OF_RANGE
    assert excinfo.value.index is None


def test_version_index_beyond_int64_is_version_out_of_range() -> None:
    data = mutations_pb2.FDBMutationRecord(
        mutation=wire_set(2**63, 0)
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.VERSION_OUT_OF_RANGE
    assert excinfo.value.index is None


def test_range_fault_in_a_batch_reports_its_position() -> None:
    bad_type = batch_record(wire_set(V, 0), wire_single(300, V, 1))
    bad_version = batch_record(wire_set(2**63, 0), wire_set(2**63, 1))

    with pytest.raises(RecordDecodeError) as type_error:
        deserialize_record(bad_type)
    with pytest.raises(RecordDecodeError) as version_error:
        deserialize_record(bad_version)

    assert type_error.value.reason is DecodeFailure.TYPE_CODE_OUT_OF_RANGE
    assert type_error.value.index == 1
    assert version_error.value.reason is DecodeFailure.VERSION_OUT_OF_RANGE
    assert version_error.value.index == 0


def test_batch_with_zero_mutations_is_empty_batch() -> None:
    data = batch_record()

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.EMPTY_BATCH
    assert excinfo.value.index is None


@pytest.mark.parametrize(
    ("first", "second"),
    [
        pytest.param((V, 0), (V + 1, 1), id="two-versions"),
        pytest.param((V, 0), (V, 2), id="gap"),
        pytest.param((V, 1), (V, 0), id="descending"),
        pytest.param((V, 0), (V, 0), id="repeat"),
    ],
)
def test_batch_off_one_contiguous_run_is_inconsistent_batch(
    first: tuple[int, int], second: tuple[int, int]
) -> None:
    data = batch_record(wire_set(*first), wire_set(*second))

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.INCONSISTENT_BATCH
    assert excinfo.value.index == 1


def test_batch_may_start_at_any_position() -> None:
    data = batch_record(wire_set(V, 5), wire_set(V, 6), wire_set(V, 7))

    assert deserialize_record(data) == Record(
        "",
        None,
        MutationBatch(
            (
                Mutation(0, b"k", b"v", VersionIndex(V, 5)),
                Mutation(0, b"k", b"v", VersionIndex(V, 6)),
                Mutation(0, b"k", b"v", VersionIndex(V, 7)),
            )
        ),
    )


@pytest.mark.parametrize(
    ("message", "body"),
    [
        pytest.param(
            mutations_pb2.FDBMutationRecord(
                mutation=mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(),
                    single_key_mutation=mutations_pb2.FDBSingleKeyMutation(),
                )
            ),
            Mutation(0, b"", b"", VersionIndex(0, 0)),
            id="single-key-mutation",
        ),
        pytest.param(
            mutations_pb2.FDBMutationRecord(
                mutation=mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(),
                    clear_range=mutations_pb2.FDBClearRange(),
                )
            ),
            Mutation(1, b"", b"", VersionIndex(0, 0)),
            id="clear-range",
        ),
        pytest.param(
            mutations_pb2.FDBMutationRecord(version_end=mutations_pb2.VersionEnd()),
            VersionEnd(0, 0, None),
            id="version-end",
        ),
    ],
)
def test_all_defaults_arms_are_accepted(
    message: mutations_pb2.FDBMutationRecord, body: RecordBody
) -> None:
    assert deserialize_record(message.SerializeToString()) == Record("", None, body)


def test_non_canonical_clear_normalises_to_the_canonical_mutation() -> None:
    canonical = serialize_mutation(CLEAR, fdb_version=V, sequence_no=4, **ENV)
    non_canonical = mutations_pb2.FDBMutationRecord(
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(fdb_version=V, sequence_no=4),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=b"a",
                value=b"b\xff",
                mutation_type=1,  # type: ignore[arg-type]  # raw int, open enum
            ),
        )
    ).SerializeToString()

    body = deserialize_record(non_canonical).body

    assert isinstance(body, Mutation)
    assert body == deserialize_record(canonical).body
    assert type(body.type) is int
    reserialized = serialize_mutation(body, fdb_version=V, sequence_no=4, **ENV)
    assert reserialized == canonical
    reparsed = mutations_pb2.FDBMutationRecord.FromString(reserialized)
    assert reparsed.mutation.WhichOneof("mutation") == "clear_range"


@pytest.mark.parametrize(
    ("envelope", "stream_name", "bridge_timestamp_ns"),
    [
        pytest.param({"stream_name": STREAM}, STREAM, None, id="no-timestamp"),
        pytest.param(
            {"bridge_timestamp": Timestamp(seconds=7, nanos=9)},
            "",
            7_000_000_009,
            id="no-stream-name",
        ),
        pytest.param(
            {
                "stream_name": STREAM,
                "bridge_timestamp": Timestamp(seconds=253_402_300_800),
            },
            STREAM,
            253_402_300_800_000_000_000,
            id="timestamp-past-year-9999",
        ),
        pytest.param(
            {"stream_name": STREAM, "bridge_timestamp": Timestamp(seconds=-5)},
            STREAM,
            -5_000_000_000,
            id="negative-timestamp",
        ),
    ],
)
def test_envelope_is_returned_as_found_and_never_raises(
    envelope: dict[str, Any], stream_name: str, bridge_timestamp_ns: int | None
) -> None:
    data = mutations_pb2.FDBMutationRecord(
        mutation=wire_set(V, 0), **envelope
    ).SerializeToString()

    assert deserialize_record(data) == Record(
        stream_name, bridge_timestamp_ns, Mutation(0, b"k", b"v", VersionIndex(V, 0))
    )


def test_version_end_inner_timestamp_is_read_independently() -> None:
    data = mutations_pb2.FDBMutationRecord(
        stream_name=STREAM,
        bridge_timestamp=Timestamp(seconds=7, nanos=9),
        version_end=mutations_pb2.VersionEnd(
            fdb_version=V, total_mutations=2, bridge_timestamp=Timestamp(seconds=99)
        ),
    ).SerializeToString()

    assert deserialize_record(data) == Record(
        STREAM, 7_000_000_009, VersionEnd(V, 2, 99_000_000_000)
    )


@pytest.mark.parametrize(
    ("inner", "bridge_timestamp_ns"),
    [
        pytest.param({}, None, id="no-timestamp"),
        pytest.param(
            {"bridge_timestamp": Timestamp(seconds=253_402_300_800)},
            253_402_300_800_000_000_000,
            id="timestamp-past-year-9999",
        ),
        pytest.param(
            {"bridge_timestamp": Timestamp(seconds=-5)},
            -5_000_000_000,
            id="negative-timestamp",
        ),
    ],
)
def test_version_end_inner_timestamp_is_returned_as_found_and_never_raises(
    inner: dict[str, Any], bridge_timestamp_ns: int | None
) -> None:
    data = mutations_pb2.FDBMutationRecord(
        stream_name=STREAM,
        bridge_timestamp=Timestamp(seconds=7, nanos=9),
        version_end=mutations_pb2.VersionEnd(fdb_version=V, total_mutations=2, **inner),
    ).SerializeToString()

    assert deserialize_record(data) == Record(
        STREAM, 7_000_000_009, VersionEnd(V, 2, bridge_timestamp_ns)
    )


UNKNOWN_FIELD = b"\x98\x06\x01"  # field 99, varint 1


SET_RECORD = Record(STREAM, TS, Mutation(0, b"k", b"v", VersionIndex(V, 0)))


def test_unknown_record_level_field_is_ignored() -> None:
    data = serialize_mutation(SET, fdb_version=V, sequence_no=0, **ENV)

    assert deserialize_record(data + UNKNOWN_FIELD) == SET_RECORD


def test_unknown_nested_field_is_ignored() -> None:
    data = serialize_mutation(SET, fdb_version=V, sequence_no=0, **ENV)
    message = mutations_pb2.FDBMutationRecord.FromString(data)
    message.mutation.MergeFromString(UNKNOWN_FIELD)
    with_unknown = message.SerializeToString()

    assert with_unknown != data
    assert deserialize_record(with_unknown) == SET_RECORD


@pytest.mark.parametrize("golden", GOLDEN, ids=["G1", "G2", "G3", "G4"])
def test_every_proper_prefix_of_a_golden_record_is_undecodable(golden: bytes) -> None:
    for cut in range(len(golden)):
        with pytest.raises(RecordDecodeError):
            deserialize_record(golden[:cut])


@given(st.binary(max_size=256))
def test_arbitrary_bytes_give_a_record_or_a_record_decode_error(data: bytes) -> None:
    try:
        record = deserialize_record(data)
    except RecordDecodeError:
        return

    assert isinstance(record, Record)


def test_version_end_beyond_int64_is_version_out_of_range() -> None:
    data = mutations_pb2.FDBMutationRecord(
        version_end=mutations_pb2.VersionEnd(fdb_version=2**63, total_mutations=1)
    ).SerializeToString()

    with pytest.raises(RecordDecodeError) as excinfo:
        deserialize_record(data)

    assert excinfo.value.reason is DecodeFailure.VERSION_OUT_OF_RANGE
    assert excinfo.value.index is None
