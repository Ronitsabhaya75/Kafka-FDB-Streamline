"""Input checks for the serialize functions.

Each raises `InputTypeError` or `InputValueError` with the offending `field`. Nothing
is coerced. Messages about `bytes` and `str` inputs give their Python type, never
their content.
"""

from collections.abc import Iterable, Iterator
from typing import Any

from src.serialization.errors import InputTypeError, InputValueError
from src.serialization.model import (
    _MAX_BRIDGE_TIMESTAMP_NS,
    _MAX_SEQUENCE_NO,
    _MAX_TOTAL_MUTATIONS,
    _MAX_TYPE_CODE,
    _MAX_VERSION,
    NativeMutation,
)


def at(index: int | None, *, label: str = "position") -> str:
    """`" at <label> <index>"` for an error message, or `""` when index is absent."""
    return "" if index is None else f" at {label} {index}"


def _rendered(value: int) -> str:
    # CPython raises a bare ValueError when asked to print an int past ~4300 digits.
    # That would replace the InputValueError being built.
    if value.bit_length() > 128:
        return f"an int of {value.bit_length()} bits"
    return str(value)


def _integer(
    value: int,
    *,
    field: str,
    maximum: int | None,
    minimum: int = 0,
    index: int | None = None,
) -> None:
    # Exact type. `isinstance` would let bool, IntEnum and numpy scalars through.
    if type(value) is not int:
        raise InputTypeError(
            f"{field}{at(index)} must be int, got {type(value).__name__}",
            field=field,
            index=index,
        )
    if value < minimum or (maximum is not None and value > maximum):
        domain = f">= {minimum}" if maximum is None else f"in {minimum}..{maximum}"
        raise InputValueError(
            f"{field}{at(index)} must be {domain}, got {_rendered(value)}",
            field=field,
            index=index,
        )


def fdb_version(value: int) -> None:
    """Require `0.._MAX_VERSION`."""
    _integer(value, field="fdb_version", maximum=_MAX_VERSION)


def sequence_no(value: int, *, field: str = "sequence_no") -> None:
    """Require `0.._MAX_SEQUENCE_NO`. `field` is the keyword name to report."""
    _integer(value, field=field, maximum=_MAX_SEQUENCE_NO)


def assigned_sequence_no(value: int, *, index: int) -> None:
    """Require a position assigned inside a batch to fit `0.._MAX_SEQUENCE_NO`."""
    _integer(value, field="sequence_no", maximum=_MAX_SEQUENCE_NO, index=index)


def total_mutations(value: int) -> None:
    """Require `0.._MAX_TOTAL_MUTATIONS`."""
    _integer(value, field="total_mutations", maximum=_MAX_TOTAL_MUTATIONS)


def max_record_bytes(value: int) -> None:
    """Require a budget of at least 1. There is no upper bound."""
    _integer(value, field="max_record_bytes", maximum=None, minimum=1)


def stream_name(value: str) -> None:
    """Require a non-empty `str` that encodes as UTF-8."""
    if not isinstance(value, str):
        raise InputTypeError(
            f"stream_name must be str, got {type(value).__name__}", field="stream_name"
        )
    if not value:
        raise InputValueError("stream_name must not be empty", field="stream_name")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        # `from None` because the codec error carries the whole name.
        raise InputValueError(
            f"stream_name (str of length {len(value)}) is not UTF-8-encodable",
            field="stream_name",
        ) from None


def bridge_timestamp_ns(value: int) -> None:
    """Require `0.._MAX_BRIDGE_TIMESTAMP_NS`."""
    _integer(value, field="bridge_timestamp_ns", maximum=_MAX_BRIDGE_TIMESTAMP_NS)


def envelope(name: str, timestamp_ns: int) -> None:
    """Check `stream_name`, then `bridge_timestamp_ns`."""
    stream_name(name)
    bridge_timestamp_ns(timestamp_ns)


def mutations(value: Iterable[NativeMutation]) -> Iterator[NativeMutation]:
    """Require an iterable and return its iterator, unread."""
    # Guard `iter()` only. A TypeError raised mid-iteration is the caller's
    # generator failing.
    try:
        return iter(value)
    except TypeError as error:
        raise InputTypeError(
            f"mutations must be iterable, got {type(value).__name__}", field="mutations"
        ) from error


def param(value: bytes, *, field: str, index: int | None = None) -> None:
    """Require `bytes` for `param1` or `param2`."""
    # Runs before protobuf sees the value. Protobuf swallows a `None` kwarg and
    # would write a record with an empty key.
    if not isinstance(value, bytes):
        raise InputTypeError(
            f"{field}{at(index)} must be bytes, got {type(value).__name__}",
            field=field,
            index=index,
        )


def type_code(value: int, *, index: int | None = None) -> int:
    """Require an `int` in 0..255 that is not a `bool`. Returns it as a plain `int`."""
    # Reject `bool` here. Older protobuf runtimes accept `True` as 1, which would
    # be a silent CLEAR_RANGE.
    if not isinstance(value, int) or isinstance(value, bool):
        raise InputTypeError(
            f"type{at(index)} must be int, got {type(value).__name__}",
            field="type",
            index=index,
        )
    code = int(value)
    if not 0 <= code <= _MAX_TYPE_CODE:
        raise InputValueError(
            f"type{at(index)} must be in 0..{_MAX_TYPE_CODE}, got {_rendered(code)}",
            field="type",
            index=index,
        )
    return code


def _attribute(mutation: NativeMutation, field: str, index: int | None) -> Any:
    try:
        return getattr(mutation, field)
    except AttributeError as error:
        raise InputTypeError(
            f"{type(mutation).__name__}{at(index)} is not a native mutation: "
            f"no {field!r} attribute",
            field=field,
            index=index,
        ) from error


def native(
    mutation: NativeMutation, *, index: int | None = None
) -> tuple[int, bytes, bytes]:
    """Require a native mutation. Returns its checked `(type, param1, param2)`."""
    # Read each attribute once, by name, in this order. The reads are the
    # conformance check.
    code = type_code(_attribute(mutation, "type", index), index=index)
    param1 = _attribute(mutation, "param1", index)
    param(param1, field="param1", index=index)
    param2 = _attribute(mutation, "param2", index)
    param(param2, field="param2", index=index)
    return code, param1, param2


def natives(
    value: Iterable[NativeMutation], *, first_sequence_no: int = 0
) -> list[tuple[int, bytes, bytes]]:
    """Require every mutation, and the position each is assigned, to be valid."""
    checked: list[tuple[int, bytes, bytes]] = []
    for index, mutation in enumerate(mutations(value)):
        # Check before the read. Past 2**32 mutations protobuf would raise its own
        # ValueError, and only once the whole group is in memory.
        assigned_sequence_no(first_sequence_no + index, index=index)
        checked.append(native(mutation, index=index))
    return checked
