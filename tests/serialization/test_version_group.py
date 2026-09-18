from datetime import UTC, datetime
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from hypothesis import given
from hypothesis import strategies as st

from src.serialization import (
    InputTypeError,
    InputValueError,
    Mutation,
    MutationBatch,
    MutationType,
    Record,
    RecordTooLargeError,
    VersionEnd,
    VersionIndex,
    deserialize_record,
    serialize_batch,
    serialize_version_end,
    serialize_version_group,
)
from tests.serialization import doubles, strategies
from tests.serialization.doubles import ADD, CLEAR, ENV, SET, STREAM, TS, V


def test_group_within_budget_is_one_batch_then_the_version_end() -> None:
    records = serialize_version_group(
        [SET, ADD, CLEAR], fdb_version=V, max_record_bytes=1_000, **ENV
    )

    assert [deserialize_record(r) for r in records] == [
        Record(
            STREAM,
            TS,
            MutationBatch(
                (
                    Mutation(0, b"k", b"v", VersionIndex(V, 0)),
                    Mutation(2, b"\x15\x01k\x00", b"\xff\x00v", VersionIndex(V, 1)),
                    Mutation(1, b"a", b"b\xff", VersionIndex(V, 2)),
                )
            ),
        ),
        Record(STREAM, TS, VersionEnd(V, 3)),
    ]


def test_empty_group_is_exactly_the_version_end() -> None:
    records = serialize_version_group([], fdb_version=V, max_record_bytes=1_000, **ENV)

    assert [deserialize_record(r) for r in records] == [
        Record(STREAM, TS, VersionEnd(V, 0))
    ]


TEN = [doubles.NativeMutation(0, bytes([i]), bytes(20)) for i in range(10)]


def test_small_budget_slices_the_group_into_several_batch_records() -> None:
    records = serialize_version_group(TEN, fdb_version=V, max_record_bytes=150, **ENV)

    assert len(records) > 2
    assert all(len(r) <= 150 for r in records)
    seen: list[Mutation] = []
    for data in records[:-1]:
        body = deserialize_record(data).body
        assert isinstance(body, MutationBatch)
        assert body.mutations[0].version_index == (V, len(seen))
        seen.extend(body.mutations)
    assert seen == [
        Mutation(m.type, m.param1, m.param2, VersionIndex(V, i))
        for i, m in enumerate(TEN)
    ]
    assert deserialize_record(records[-1]) == Record(STREAM, TS, VersionEnd(V, 10))


def slice_bounds(records: list[bytes]) -> list[tuple[int, int]]:
    bounds: list[tuple[int, int]] = []
    start = 0
    for data in records[:-1]:
        body = deserialize_record(data).body
        assert isinstance(body, MutationBatch)
        bounds.append((start, start + len(body.mutations)))
        start += len(body.mutations)
    return bounds


def test_records_are_byte_identical_to_the_primitives_on_each_slice() -> None:
    records = serialize_version_group(TEN, fdb_version=V, max_record_bytes=150, **ENV)

    assert records[:-1] == [
        serialize_batch(TEN[a:b], fdb_version=V, first_sequence_no=a, **ENV)
        for a, b in slice_bounds(records)
    ]
    assert records[-1] == serialize_version_end(
        fdb_version=V, total_mutations=10, **ENV
    )


VARIED = [doubles.NativeMutation(0, bytes([i]), bytes(7 * i)) for i in range(12)]


@pytest.mark.parametrize("budget", range(130, 400, 9))
def test_no_slice_could_have_taken_the_next_mutation(budget: int) -> None:
    records = serialize_version_group(
        VARIED, fdb_version=V, max_record_bytes=budget, **ENV
    )

    bounds = slice_bounds(records)
    assert bounds[-1][1] == 12
    for a, b in bounds[:-1]:
        overfull = serialize_batch(
            VARIED[a : b + 1], fdb_version=V, first_sequence_no=a, **ENV
        )
        assert len(overfull) > budget


@pytest.mark.parametrize("group", [TEN, VARIED], ids=["ten", "varied"])
def test_budget_boundary_is_inclusive(group: list[doubles.NativeMutation]) -> None:
    exact = len(serialize_batch(group, fdb_version=V, **ENV))

    fits = serialize_version_group(group, fdb_version=V, max_record_bytes=exact, **ENV)
    one_less = serialize_version_group(
        group, fdb_version=V, max_record_bytes=exact - 1, **ENV
    )

    assert len(fits) == 2
    assert len(one_less) == 3


def test_equal_arguments_give_equal_records_and_a_generator_works() -> None:
    first = serialize_version_group(
        tuple(TEN), fdb_version=V, max_record_bytes=150, **ENV
    )
    second = serialize_version_group(
        tuple(TEN), fdb_version=V, max_record_bytes=150, **ENV
    )
    from_generator = serialize_version_group(
        (m for m in TEN), fdb_version=V, max_record_bytes=150, **ENV
    )

    assert first == second
    assert from_generator == first


def test_a_100_000_byte_value_fits_one_batch_record() -> None:
    big = doubles.NativeMutation(0, b"k", bytes(100_000))

    records = serialize_version_group(
        [big], fdb_version=V, max_record_bytes=1_000_000, **ENV
    )

    assert len(records) == 2
    assert deserialize_record(records[0]) == Record(
        STREAM,
        TS,
        MutationBatch((Mutation(0, b"k", bytes(100_000), VersionIndex(V, 0)),)),
    )


@pytest.mark.slow
def test_ten_megabyte_group_is_sliced_within_budget() -> None:
    ms = [
        doubles.NativeMutation(0, i.to_bytes(16, "big"), bytes(72))
        for i in range(100_000)
    ]

    records = serialize_version_group(
        ms, fdb_version=V, max_record_bytes=1_000_000, **ENV
    )

    assert len(records) - 1 >= 11
    assert all(len(r) <= 1_000_000 for r in records)
    assert sum(len(r) for r in records) >= 10_000_000
    seen: list[Mutation] = []
    for data in records[:-1]:
        body = deserialize_record(data).body
        assert isinstance(body, MutationBatch)
        seen.extend(body.mutations)
    assert len(seen) == 100_000
    # Plain bools: a pytest-diffed 100k-element comparison is unreadable and slow.
    params_match = [(m.type, m.param1, m.param2) for m in seen] == [
        (m.type, m.param1, m.param2) for m in ms
    ]
    assert params_match
    positions_match = [m.version_index for m in seen] == [
        (V, i) for i in range(100_000)
    ]
    assert positions_match
    assert seen[0].version_index == (V, 0)
    assert seen[-1].version_index == (V, 99_999)
    assert deserialize_record(records[-1]) == Record(STREAM, TS, VersionEnd(V, 100_000))


def test_unsplittable_mutation_raises_record_too_large() -> None:
    small = doubles.NativeMutation(0, b"k", b"v")
    big = doubles.NativeMutation(0, b"k", bytes(500))
    big_alone = len(serialize_batch([big], fdb_version=V, first_sequence_no=1, **ENV))

    with pytest.raises(RecordTooLargeError) as excinfo:
        serialize_version_group(
            [small, big, small], fdb_version=V, max_record_bytes=big_alone - 1, **ENV
        )

    assert excinfo.value.field == "max_record_bytes"
    assert excinfo.value.index == 1
    assert excinfo.value.record_bytes == big_alone
    assert excinfo.value.max_record_bytes == big_alone - 1
    assert excinfo.value.record_bytes > excinfo.value.max_record_bytes


def test_budget_below_the_version_end_raises_record_too_large() -> None:
    with pytest.raises(RecordTooLargeError) as excinfo:
        serialize_version_group([], fdb_version=V, max_record_bytes=1, **ENV)

    assert excinfo.value.field == "max_record_bytes"
    assert excinfo.value.index is None
    assert excinfo.value.record_bytes == len(
        serialize_version_end(fdb_version=V, total_mutations=0, **ENV)
    )
    assert excinfo.value.max_record_bytes == 1


@pytest.mark.parametrize("budget", [0, -1])
def test_budget_below_one_is_a_value_error_not_record_too_large(budget: int) -> None:
    with pytest.raises(InputValueError) as excinfo:
        serialize_version_group([SET], fdb_version=V, max_record_bytes=budget, **ENV)

    assert not isinstance(excinfo.value, RecordTooLargeError)
    assert excinfo.value.field == "max_record_bytes"
    assert excinfo.value.index is None


@pytest.mark.parametrize("budget", [True, 1.5, None])
def test_non_int_budget_is_a_type_error(budget: Any) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        serialize_version_group([SET], fdb_version=V, max_record_bytes=budget, **ENV)

    assert excinfo.value.field == "max_record_bytes"
    assert excinfo.value.index is None


@pytest.mark.parametrize("fdb_version", [-1, 2**63, 2**64])
def test_out_of_range_fdb_version_is_a_value_error(fdb_version: int) -> None:
    with pytest.raises(InputValueError) as excinfo:
        serialize_version_group(
            [SET], fdb_version=fdb_version, max_record_bytes=1_000, **ENV
        )

    assert excinfo.value.field == "fdb_version"


@pytest.mark.parametrize("fdb_version", [True, 1.0, "1", None, MutationType.ADD])
def test_non_int_fdb_version_is_a_type_error(fdb_version: Any) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        serialize_version_group(
            [SET], fdb_version=fdb_version, max_record_bytes=1_000, **ENV
        )

    assert excinfo.value.field == "fdb_version"


@pytest.mark.parametrize(
    ("stream_name", "error"),
    [
        ("", InputValueError),
        ("\ud800", InputValueError),
        (b"orders", InputTypeError),
        (bytearray(b"orders"), InputTypeError),
        (None, InputTypeError),
    ],
)
def test_bad_stream_name_is_rejected(
    stream_name: Any, error: type[InputTypeError] | type[InputValueError]
) -> None:
    with pytest.raises(error) as excinfo:
        serialize_version_group(
            [SET],
            fdb_version=V,
            max_record_bytes=1_000,
            stream_name=stream_name,
            bridge_timestamp_ns=TS,
        )

    assert excinfo.value.field == "stream_name"


@pytest.mark.parametrize(
    ("bridge_timestamp_ns", "error"),
    [
        (-1, InputValueError),
        (253_402_300_799_999_999_999 + 1, InputValueError),
        (True, InputTypeError),
        (1.5, InputTypeError),
        (datetime(2026, 1, 1, tzinfo=UTC), InputTypeError),
        (datetime(2026, 1, 1), InputTypeError),
        (Timestamp(), InputTypeError),
        (None, InputTypeError),
    ],
)
def test_bad_bridge_timestamp_is_rejected(
    bridge_timestamp_ns: Any, error: type[InputTypeError] | type[InputValueError]
) -> None:
    with pytest.raises(error) as excinfo:
        serialize_version_group(
            [SET],
            fdb_version=V,
            max_record_bytes=1_000,
            stream_name=STREAM,
            bridge_timestamp_ns=bridge_timestamp_ns,
        )

    assert excinfo.value.field == "bridge_timestamp_ns"


@pytest.mark.parametrize("mutations", [None, 5])
def test_non_iterable_mutations_is_a_type_error(mutations: Any) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        serialize_version_group(mutations, fdb_version=V, max_record_bytes=1_000, **ENV)

    assert excinfo.value.field == "mutations"
    assert excinfo.value.index is None


def test_invalid_mutation_late_in_the_group_raises_and_returns_nothing() -> None:
    group: list[Any] = [SET] * 100
    group[90] = doubles.NativeMutation(0, b"k", None)  # type: ignore[arg-type]
    records = None

    with pytest.raises(InputTypeError) as excinfo:
        # A budget that slices: the bad mutation must surface before any slice does.
        records = serialize_version_group(
            group, fdb_version=V, max_record_bytes=150, **ENV
        )

    assert records is None
    assert excinfo.value.field == "param2"
    assert excinfo.value.index == 90


@pytest.mark.parametrize(
    ("valid", "first_reported"),
    [
        ({}, "fdb_version"),
        ({"fdb_version": V}, "max_record_bytes"),
        ({"fdb_version": V, "max_record_bytes": 1_000}, "stream_name"),
        (
            {"fdb_version": V, "max_record_bytes": 1_000, "stream_name": STREAM},
            "bridge_timestamp_ns",
        ),
    ],
)
def test_scalars_are_checked_in_signature_order_before_mutations_are_touched(
    valid: dict[str, Any], first_reported: str
) -> None:
    all_invalid: dict[str, Any] = {
        "fdb_version": -1,
        "max_record_bytes": 0,
        "stream_name": "",
        "bridge_timestamp_ns": -1,
    }
    gen = (m for m in [SET, ADD])

    with pytest.raises(InputValueError) as excinfo:
        serialize_version_group(gen, **(all_invalid | valid))

    assert excinfo.value.field == first_reported
    assert next(gen) == SET


@given(
    group=strategies.groups,
    fdb_version=strategies.fdb_versions,
    stream_name=strategies.stream_names,
    bridge_timestamp_ns=strategies.bridge_timestamps_ns,
    headroom=st.integers(min_value=0, max_value=2_000),
)
def test_any_group_survives_slicing_under_any_sufficient_budget(
    group: list[doubles.NativeMutation],
    fdb_version: int,
    stream_name: str,
    bridge_timestamp_ns: int,
    headroom: int,
) -> None:
    env: dict[str, Any] = {
        "stream_name": stream_name,
        "bridge_timestamp_ns": bridge_timestamp_ns,
    }
    version_end = serialize_version_end(
        fdb_version=fdb_version, total_mutations=len(group), **env
    )
    one_element_records = [
        serialize_batch([m], fdb_version=fdb_version, first_sequence_no=i, **env)
        for i, m in enumerate(group)
    ]
    budget = max(len(r) for r in [version_end, *one_element_records]) + headroom

    records = serialize_version_group(
        group, fdb_version=fdb_version, max_record_bytes=budget, **env
    )

    assert all(len(r) <= budget for r in records)
    seen: list[Mutation] = []
    for data in records[:-1]:
        body = deserialize_record(data).body
        assert isinstance(body, MutationBatch)
        seen.extend(body.mutations)
    assert seen == [
        Mutation(m.type, m.param1, m.param2, VersionIndex(fdb_version, i))
        for i, m in enumerate(group)
    ]
    assert deserialize_record(records[-1]) == Record(
        stream_name, bridge_timestamp_ns, VersionEnd(fdb_version, len(group))
    )


def test_unrenderable_budget_is_still_a_value_error() -> None:
    with pytest.raises(InputValueError) as excinfo:
        serialize_version_group(
            [SET], fdb_version=V, max_record_bytes=-(10**5000), **ENV
        )

    assert excinfo.value.field == "max_record_bytes"
