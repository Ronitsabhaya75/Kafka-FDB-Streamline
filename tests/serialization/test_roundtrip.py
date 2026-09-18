import enum
from dataclasses import FrozenInstanceError, dataclass

import pytest
from hypothesis import given
from hypothesis import strategies as st

from src.serialization import (
    InputValueError,
    Mutation,
    MutationBatch,
    NativeMutation,
    Record,
    VersionEnd,
    VersionIndex,
    deserialize_record,
    serialize_batch,
    serialize_mutation,
    serialize_version_end,
)
from tests.serialization import doubles, strategies
from tests.serialization.doubles import ADD, CLEAR, ENV, SET, STREAM, TS, V


def test_set_value_mutation_round_trips() -> None:
    data = serialize_mutation(SET, fdb_version=V, sequence_no=0, **ENV)

    assert deserialize_record(data) == Record(
        STREAM, TS, Mutation(0, b"k", b"v", VersionIndex(V, 0))
    )


def test_clear_range_round_trips_with_synthesised_type_code() -> None:
    data = serialize_mutation(CLEAR, fdb_version=V, sequence_no=0, **ENV)

    assert deserialize_record(data) == Record(
        STREAM, TS, Mutation(1, b"a", b"b\xff", VersionIndex(V, 0))
    )


@pytest.mark.parametrize("code", [3, 4, 5, 10, 11, 21, 22, 23, 24, 255])
def test_undeclared_type_code_round_trips_as_plain_int(code: int) -> None:
    native = doubles.NativeMutation(code, b"k", b"v")

    record = deserialize_record(
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)
    )

    assert record == Record(STREAM, TS, Mutation(code, b"k", b"v", VersionIndex(V, 0)))
    assert isinstance(record.body, Mutation)
    assert type(record.body.type) is int


@pytest.mark.parametrize(
    ("fdb_version", "sequence_no"), [(0, 0), (1, 5), (2**63 - 1, 2**32 - 1)]
)
def test_version_index_round_trips(fdb_version: int, sequence_no: int) -> None:
    data = serialize_mutation(
        SET, fdb_version=fdb_version, sequence_no=sequence_no, **ENV
    )

    assert deserialize_record(data) == Record(
        STREAM, TS, Mutation(0, b"k", b"v", VersionIndex(fdb_version, sequence_no))
    )


@pytest.mark.parametrize(
    ("code", "param1", "param2"),
    [
        pytest.param(0, b"k", b"", id="set-empty-value"),
        pytest.param(0, b"", b"v", id="set-empty-key"),
        pytest.param(0, b"k" * 10_001, b"v", id="set-10001-byte-key"),
        pytest.param(0, b"k", b"v" * 100_001, id="set-100001-byte-value"),
        pytest.param(0, b"\xff\xff/x", b"v", id="set-system-key"),
        pytest.param(2, b"k", b"", id="add-0-byte-operand"),
        pytest.param(2, b"k", b"\x01", id="add-1-byte-operand"),
        pytest.param(2, b"k", b"\x01\x02\x03", id="add-3-byte-operand"),
        pytest.param(2, b"k", b"\x01" * 9, id="add-9-byte-operand"),
        pytest.param(20, b"k", b"", id="compare-and-clear-empty-operand"),
    ],
)
def test_opaque_params_round_trip_unchanged(
    code: int, param1: bytes, param2: bytes
) -> None:
    native = doubles.NativeMutation(code, param1, param2)

    data = serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert deserialize_record(data) == Record(
        STREAM, TS, Mutation(code, param1, param2, VersionIndex(V, 0))
    )


@pytest.mark.parametrize(
    ("begin", "end"),
    [
        pytest.param(b"a", b"a", id="begin-equals-end"),
        pytest.param(b"b", b"a", id="begin-after-end"),
        pytest.param(b"", b"z", id="empty-begin"),
        pytest.param(b"a", b"\xff", id="end-at-system-keyspace"),
        pytest.param(b"k", b"k\x00", id="single-key-clear"),
    ],
)
def test_clear_range_keys_round_trip_uninspected(begin: bytes, end: bytes) -> None:
    native = doubles.NativeMutation(1, begin, end)

    data = serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert deserialize_record(data) == Record(
        STREAM, TS, Mutation(1, begin, end, VersionIndex(V, 0))
    )


@dataclass(frozen=True)
class MutationWithExtras:
    type: int
    param1: bytes
    param2: bytes
    version: int = 99
    tenant: str = "acme"


class UpstreamTypeCode(enum.IntEnum):
    ADD = 2


@pytest.mark.parametrize(
    "native",
    [
        pytest.param(
            doubles.AttrOnlyMutation(2, ADD.param1, ADD.param2), id="attr-only"
        ),
        pytest.param(MutationWithExtras(2, ADD.param1, ADD.param2), id="extras"),
        pytest.param(
            doubles.NativeMutation(UpstreamTypeCode.ADD, ADD.param1, ADD.param2),
            id="int-enum-type-code",
        ),
    ],
)
def test_any_object_with_the_three_attributes_is_a_native_mutation(
    native: NativeMutation,
) -> None:
    record = deserialize_record(
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)
    )

    assert record == Record(
        STREAM,
        TS,
        Mutation(2, b"\x15\x01k\x00", b"\xff\x00v", VersionIndex(V, 0)),
    )
    assert isinstance(record.body, Mutation)
    assert type(record.body.type) is int


@pytest.mark.parametrize(
    "stream_name",
    [
        pytest.param("orders", id="plain"),
        pytest.param("   ", id="whitespace-only"),
        pytest.param("a\x00b", id="embedded-nul"),
        pytest.param("übër/注文", id="non-ascii"),
        pytest.param("n" * 10_000, id="10-kB"),
    ],
)
def test_stream_name_round_trips_exactly(stream_name: str) -> None:
    data = serialize_mutation(
        SET,
        fdb_version=V,
        sequence_no=0,
        stream_name=stream_name,
        bridge_timestamp_ns=TS,
    )

    assert deserialize_record(data) == Record(
        stream_name, TS, Mutation(0, b"k", b"v", VersionIndex(V, 0))
    )


@pytest.mark.parametrize("bridge_timestamp_ns", [0, 1, TS, 253_402_300_799_999_999_999])
def test_bridge_timestamp_round_trips_exactly(bridge_timestamp_ns: int) -> None:
    data = serialize_mutation(
        SET,
        fdb_version=V,
        sequence_no=0,
        stream_name=STREAM,
        bridge_timestamp_ns=bridge_timestamp_ns,
    )

    assert deserialize_record(data) == Record(
        STREAM, bridge_timestamp_ns, Mutation(0, b"k", b"v", VersionIndex(V, 0))
    )


def test_batch_assigns_version_indexes_in_input_order() -> None:
    data = serialize_batch([SET, ADD, CLEAR], fdb_version=V, **ENV)

    assert deserialize_record(data) == Record(
        STREAM,
        TS,
        MutationBatch(
            (
                Mutation(0, b"k", b"v", VersionIndex(V, 0)),
                Mutation(2, b"\x15\x01k\x00", b"\xff\x00v", VersionIndex(V, 1)),
                Mutation(1, b"a", b"b\xff", VersionIndex(V, 2)),
            )
        ),
    )


def test_batch_positions_start_at_first_sequence_no() -> None:
    data = serialize_batch([SET, ADD, CLEAR], fdb_version=V, first_sequence_no=7, **ENV)

    assert deserialize_record(data) == Record(
        STREAM,
        TS,
        MutationBatch(
            (
                Mutation(0, b"k", b"v", VersionIndex(V, 7)),
                Mutation(2, b"\x15\x01k\x00", b"\xff\x00v", VersionIndex(V, 8)),
                Mutation(1, b"a", b"b\xff", VersionIndex(V, 9)),
            )
        ),
    )


def test_one_element_batch_stays_a_batch() -> None:
    batch_record = deserialize_record(
        serialize_batch([ADD], fdb_version=V, first_sequence_no=3, **ENV)
    )
    mutation_record = deserialize_record(
        serialize_mutation(ADD, fdb_version=V, sequence_no=3, **ENV)
    )

    assert isinstance(batch_record.body, MutationBatch)
    assert batch_record != mutation_record
    assert batch_record.body.mutations[0] == mutation_record.body


def test_batch_applies_no_type_rules_of_its_own() -> None:
    natives = [
        SET,
        doubles.NativeMutation(6, b"legacy-and", b"\x0f"),
        CLEAR,
        doubles.NativeMutation(255, b"undeclared", b"\x00\x01"),
        doubles.NativeMutation(3, b"", b""),
        ADD,
    ]

    batch = deserialize_record(serialize_batch(natives, fdb_version=V, **ENV)).body

    assert isinstance(batch, MutationBatch)
    alone = tuple(
        deserialize_record(
            serialize_mutation(native, fdb_version=V, sequence_no=i, **ENV)
        ).body
        for i, native in enumerate(natives)
    )
    assert batch.mutations == alone


def test_deserialized_mutation_feeds_back_into_serialize_mutation() -> None:
    data = serialize_mutation(CLEAR, fdb_version=V, sequence_no=4, **ENV)
    record = deserialize_record(data)
    assert isinstance(record.body, Mutation)
    assert record.bridge_timestamp_ns is not None

    again = serialize_mutation(
        record.body,
        fdb_version=record.body.version_index.fdb_version,
        sequence_no=record.body.version_index.sequence_no,
        stream_name=record.stream_name,
        bridge_timestamp_ns=record.bridge_timestamp_ns,
    )

    assert again == data


def test_deserialized_batch_feeds_back_into_serialize_batch() -> None:
    data = serialize_batch([SET, ADD, CLEAR], fdb_version=V, first_sequence_no=7, **ENV)
    record = deserialize_record(data)
    assert isinstance(record.body, MutationBatch)
    assert record.bridge_timestamp_ns is not None
    first = record.body.mutations[0].version_index

    again = serialize_batch(
        record.body.mutations,
        fdb_version=first.fdb_version,
        first_sequence_no=first.sequence_no,
        stream_name=record.stream_name,
        bridge_timestamp_ns=record.bridge_timestamp_ns,
    )

    assert again == data


def test_deserialized_version_end_feeds_back_into_serialize_version_end() -> None:
    data = serialize_version_end(fdb_version=V, total_mutations=5, **ENV)
    record = deserialize_record(data)
    assert isinstance(record.body, VersionEnd)
    assert record.bridge_timestamp_ns is not None

    again = serialize_version_end(
        fdb_version=record.body.fdb_version,
        total_mutations=record.body.total_mutations,
        stream_name=record.stream_name,
        bridge_timestamp_ns=record.bridge_timestamp_ns,
    )

    assert again == data


@pytest.mark.parametrize(
    ("value", "equal", "attribute"),
    [
        pytest.param(
            Mutation(0, b"k", b"v", VersionIndex(V, 0)),
            Mutation(0, b"k", b"v", VersionIndex(V, 0)),
            "type",
            id="mutation",
        ),
        pytest.param(
            MutationBatch((Mutation(0, b"k", b"v", VersionIndex(V, 0)),)),
            MutationBatch((Mutation(0, b"k", b"v", VersionIndex(V, 0)),)),
            "mutations",
            id="batch",
        ),
        pytest.param(VersionEnd(V, 5), VersionEnd(V, 5), "fdb_version", id="end"),
        pytest.param(
            Record(STREAM, TS, VersionEnd(V, 5)),
            Record(STREAM, TS, VersionEnd(V, 5)),
            "stream_name",
            id="record",
        ),
    ],
)
def test_output_values_are_immutable_hashable_and_compared_field_wise(
    value: object, equal: object, attribute: str
) -> None:
    assert value == equal
    assert hash(value) == hash(equal)
    with pytest.raises(FrozenInstanceError):
        setattr(value, attribute, None)


def test_mutation_is_not_a_native_tuple() -> None:
    assert Mutation(0, b"k", b"v", VersionIndex(V, 0)) != (0, b"k", b"v")


def test_version_index_compares_as_a_tuple_in_stream_order() -> None:
    record = deserialize_record(
        serialize_mutation(SET, fdb_version=V, sequence_no=3, **ENV)
    )

    assert isinstance(record.body, Mutation)
    assert record.body.version_index == (V, 3)
    assert VersionIndex(V, 1) < VersionIndex(V + 1, 0)


def test_fdb_maximum_value_round_trips_through_serialize_mutation() -> None:
    value = bytes(range(250)) * 400
    native = doubles.NativeMutation(0, b"k", value)

    record = deserialize_record(
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)
    )

    assert len(value) == 100_000
    assert record == Record(STREAM, TS, Mutation(0, b"k", value, VersionIndex(V, 0)))


@pytest.mark.slow
def test_ten_megabyte_batch_has_no_size_cap_and_is_fully_materialised() -> None:
    natives = [
        doubles.NativeMutation(0, i.to_bytes(16, "big"), bytes(72))
        for i in range(100_000)
    ]

    data = serialize_batch(natives, fdb_version=V, **ENV)
    body = deserialize_record(data).body

    assert len(data) >= 10_000_000
    assert isinstance(body, MutationBatch)
    assert type(body.mutations) is tuple
    assert len(body.mutations) == 100_000
    assert body.mutations[0] == Mutation(
        0, (0).to_bytes(16, "big"), bytes(72), VersionIndex(V, 0)
    )
    assert body.mutations[-1] == Mutation(
        0, (99_999).to_bytes(16, "big"), bytes(72), VersionIndex(V, 99_999)
    )


@given(
    native=strategies.native_mutations,
    fdb_version=strategies.fdb_versions,
    sequence_no=strategies.sequence_nos,
    stream_name=strategies.stream_names,
    bridge_timestamp_ns=strategies.bridge_timestamps_ns,
)
def test_any_accepted_mutation_round_trips(
    native: doubles.NativeMutation,
    fdb_version: int,
    sequence_no: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> None:
    data = serialize_mutation(
        native,
        fdb_version=fdb_version,
        sequence_no=sequence_no,
        stream_name=stream_name,
        bridge_timestamp_ns=bridge_timestamp_ns,
    )

    assert deserialize_record(data) == Record(
        stream_name,
        bridge_timestamp_ns,
        Mutation(
            native.type,
            native.param1,
            native.param2,
            VersionIndex(fdb_version, sequence_no),
        ),
    )


@given(
    code=st.one_of(st.integers(max_value=-1), st.integers(min_value=256)),
    param1=strategies.keys,
    param2=strategies.values,
)
def test_type_code_outside_uint8_is_never_emitted(
    code: int, param1: bytes, param2: bytes
) -> None:
    native = doubles.NativeMutation(code, param1, param2)

    with pytest.raises(InputValueError) as alone:
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)
    with pytest.raises(InputValueError) as batched:
        serialize_batch([SET, native], fdb_version=V, **ENV)

    assert (alone.value.field, alone.value.index) == ("type", None)
    assert (batched.value.field, batched.value.index) == ("type", 1)


@pytest.mark.parametrize("total", [0, 1, 2**32 - 1])
def test_version_end_round_trips(total: int) -> None:
    data = serialize_version_end(fdb_version=V, total_mutations=total, **ENV)

    assert deserialize_record(data) == Record(STREAM, TS, VersionEnd(V, total))
