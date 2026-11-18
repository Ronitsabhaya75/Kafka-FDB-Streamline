"""`_checks.natives` accepts and rejects exactly what `native()` does."""

import enum
from collections.abc import Iterable
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from fdb_kafka_bridge.serialization import _checks
from fdb_kafka_bridge.serialization.errors import InputTypeError, InputValueError
from tests.serialization import doubles, strategies


def _reference_natives(
    value: Iterable[Any], *, first_sequence_no: int = 0
) -> list[tuple[int, bytes, bytes]]:
    # The pre-fast-path body, kept as the oracle.
    checked: list[tuple[int, bytes, bytes]] = []
    for index, mutation in enumerate(_checks.mutations(value)):
        _checks.assigned_sequence_no(first_sequence_no + index, index=index)
        checked.append(_checks.native(mutation, index=index))
    return checked


class Code(enum.IntEnum):
    SET = 0
    CLEAR = 1


class Key(bytes):
    pass


class _NoParam2:
    type = 0
    param1 = b"k"


_codes = st.one_of(
    strategies.type_codes,
    st.integers(min_value=-(2**70), max_value=2**70),
    st.booleans(),
    st.sampled_from(list(Code)),
    st.none(),
    st.floats(allow_nan=False),
)
_params = st.one_of(
    strategies.keys,
    strategies.keys.map(Key),
    strategies.keys.map(bytearray),
    st.text(max_size=4),
    st.none(),
)
_mutations = st.one_of(
    strategies.native_mutations,
    st.builds(doubles.AttrOnlyMutation, _codes, _params, _params),
    st.just(_NoParam2()),
)


def _outcome(fn: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        result = fn(*args, **kwargs)
    except (InputTypeError, InputValueError) as error:
        return (type(error), error.field, error.index, str(error))
    return [(type(c), c, type(a), a, type(b), b) for c, a, b in result]


@given(st.lists(_mutations, max_size=12))
def test_natives_matches_reference_on_any_input(xs: list[Any]) -> None:
    assert _outcome(_checks.natives, xs) == _outcome(_reference_natives, xs)


@given(strategies.groups)
def test_natives_matches_reference_on_valid_groups(xs: list[Any]) -> None:
    assert _checks.natives(xs) == _reference_natives(xs)


@given(st.integers(min_value=-(2**33), max_value=2**33), st.integers(0, 3))
def test_natives_matches_reference_on_any_first_sequence_no(first: int, n: int) -> None:
    xs = [doubles.SET] * n
    assert _outcome(_checks.natives, xs, first_sequence_no=first) == _outcome(
        _reference_natives, xs, first_sequence_no=first
    )


def test_int_enum_type_code_is_accepted_as_plain_int() -> None:
    [(code, _, _)] = _checks.natives([doubles.AttrOnlyMutation(Code.CLEAR, b"a", b"b")])
    assert type(code) is int and code == 1


def test_bytes_subclass_params_are_accepted() -> None:
    [(_, key, value)] = _checks.natives(
        [doubles.NativeMutation(0, Key(b"k"), Key(b"v"))]
    )
    assert key == b"k" and value == b"v"


def test_true_type_code_is_still_rejected() -> None:
    with pytest.raises(InputTypeError) as info:
        _checks.natives([doubles.SET, doubles.AttrOnlyMutation(True, b"k", b"v")])
    assert (info.value.field, info.value.index) == ("type", 1)


class _CountingMutation:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    @property
    def type(self) -> int:
        self._log.append("type")
        return 0

    @property
    def param1(self) -> bytes:
        self._log.append("param1")
        return b"k"

    @property
    def param2(self) -> bytes:
        self._log.append("param2")
        return b"v"


def test_fast_path_reads_each_attribute_once_in_order() -> None:
    log: list[str] = []
    _checks.natives([_CountingMutation(log), _CountingMutation(log)])
    assert log == ["type", "param1", "param2"] * 2


def test_generator_input_still_works() -> None:
    assert _checks.natives(m for m in [doubles.SET, doubles.CLEAR]) == [
        (0, b"k", b"v"),
        (1, b"a", b"b\xff"),
    ]


def test_negative_first_sequence_no_is_rejected() -> None:
    with pytest.raises(InputValueError) as info:
        _checks.natives([doubles.SET], first_sequence_no=-1)
    assert info.value.field == "sequence_no"
