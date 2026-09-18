"""Input checks shared by the serialize functions.

Each check raises `InputTypeError` or `InputValueError` naming the offending field.
Nothing is coerced: only `type_code` returns a value (the plain `int` of an `IntEnum`
member) and `mutations` the iterator it opened. Messages give Python type and length
for `bytes` / `str` inputs, never their content.
"""

from collections.abc import Iterable, Iterator

from src.serialization.errors import InputTypeError, InputValueError
from src.serialization.model import (
    MAX_BRIDGE_TIMESTAMP_NS,
    MAX_SEQUENCE_NO,
    MAX_TOTAL_MUTATIONS,
    MAX_TYPE_CODE,
    MAX_VERSION,
    NativeMutation,
)


def at(index: int | None) -> str:
    """Render a batch position for an error message; empty outside a batch.

    Args:
        index: Position in `mutations`, or `None`.

    Returns:
        `" at position <index>"`, or `""` when `index` is `None`.
    """
    return "" if index is None else f" at position {index}"


def _rendered(value: int) -> str:
    # CPython refuses to render ints past ~4300 digits with a bare ValueError, which
    # would escape in place of the InputValueError being built.
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
    # Exact type, not isinstance: bool, IntEnum and numpy scalars are all refused.
    # Only the type code has a reason to arrive as an IntEnum.
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
    """Require a commit version in `0..MAX_VERSION`.

    Args:
        value: The candidate `fdb_version`.

    Raises:
        InputTypeError: If `type(value) is not int`.
        InputValueError: If the version is negative or above `MAX_VERSION`.
    """
    _integer(value, field="fdb_version", maximum=MAX_VERSION)


def sequence_no(value: int, *, field: str = "sequence_no") -> None:
    """Require a position in `0..MAX_SEQUENCE_NO`.

    Args:
        value: The candidate `sequence_no` or `first_sequence_no`.
        field: Keyword name reported on failure.

    Raises:
        InputTypeError: If `type(value) is not int`.
        InputValueError: If the position is negative or above `MAX_SEQUENCE_NO`.
    """
    _integer(value, field=field, maximum=MAX_SEQUENCE_NO)


def assigned_sequence_no(value: int, *, index: int) -> None:
    """Require a position assigned inside a batch to fit `0..MAX_SEQUENCE_NO`.

    Args:
        value: The assigned position, `first_sequence_no + index`.
        index: Position in `mutations` of the mutation it was assigned to.

    Raises:
        InputValueError: If the assigned position is above `MAX_SEQUENCE_NO`.
    """
    _integer(value, field="sequence_no", maximum=MAX_SEQUENCE_NO, index=index)


def total_mutations(value: int) -> None:
    """Require a mutation count in `0..MAX_TOTAL_MUTATIONS`.

    Args:
        value: The candidate `total_mutations`.

    Raises:
        InputTypeError: If `type(value) is not int`.
        InputValueError: If the count is negative or above `MAX_TOTAL_MUTATIONS`.
    """
    _integer(value, field="total_mutations", maximum=MAX_TOTAL_MUTATIONS)


def max_record_bytes(value: int) -> None:
    """Require a record budget of at least one byte; there is no upper bound.

    Args:
        value: The candidate `max_record_bytes`.

    Raises:
        InputTypeError: If `type(value) is not int`.
        InputValueError: If the budget is below 1.
    """
    _integer(value, field="max_record_bytes", maximum=None, minimum=1)


def stream_name(value: str) -> None:
    """Require a non-empty, UTF-8-encodable stream name.

    Args:
        value: The candidate `stream_name`.

    Raises:
        InputTypeError: If the value is not a `str`.
        InputValueError: If the name is empty or not UTF-8-encodable.
    """
    if not isinstance(value, str):
        raise InputTypeError(
            f"stream_name must be str, got {type(value).__name__}", field="stream_name"
        )
    if not value:
        raise InputValueError("stream_name must not be empty", field="stream_name")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        # Raised `from None`: the codec error carries the whole name.
        raise InputValueError(
            f"stream_name (str of length {len(value)}) is not UTF-8-encodable",
            field="stream_name",
        ) from None


def bridge_timestamp_ns(value: int) -> None:
    """Require a bridge timestamp in `0..MAX_BRIDGE_TIMESTAMP_NS`.

    Args:
        value: The candidate `bridge_timestamp_ns`.

    Raises:
        InputTypeError: If `type(value) is not int`.
        InputValueError: If the timestamp is negative or past year 9999.
    """
    _integer(value, field="bridge_timestamp_ns", maximum=MAX_BRIDGE_TIMESTAMP_NS)


def mutations(value: Iterable[NativeMutation]) -> Iterator[NativeMutation]:
    """Require `mutations` to be iterable and start its single pass.

    Args:
        value: The candidate `mutations` argument.

    Returns:
        The iterator to consume; nothing has been read from it yet.

    Raises:
        InputTypeError: If the value is not iterable.
    """
    # Only `iter()` is guarded: a TypeError raised while iterating is the caller's
    # generator failing, not a non-iterable argument.
    try:
        return iter(value)
    except TypeError as error:
        raise InputTypeError(
            f"mutations must be iterable, got {type(value).__name__}", field="mutations"
        ) from error


def param(value: bytes, *, field: str, index: int | None = None) -> None:
    """Require a native mutation's `param1` / `param2` to be `bytes`.

    Args:
        value: The candidate parameter.
        field: `"param1"` or `"param2"`, reported on failure.
        index: Position in `mutations` reported on failure; `None` outside a batch.

    Raises:
        InputTypeError: If the value is not `bytes`.
    """
    # Must run before protobuf sees the value: a `None` constructor kwarg is
    # silently swallowed and would yield a record with an empty key.
    if not isinstance(value, bytes):
        raise InputTypeError(
            f"{field}{at(index)} must be bytes, got {type(value).__name__}",
            field=field,
            index=index,
        )


def type_code(value: int, *, field: str = "type", index: int | None = None) -> int:
    """Validate a type code and normalise it to a plain `int`.

    Args:
        value: The candidate type code.
        field: Name reported on failure.
        index: Position in `mutations` reported on failure; `None` outside a batch.

    Returns:
        The type code as a plain `int`.

    Raises:
        InputTypeError: If the code is a `bool` or not an `int`.
        InputValueError: If the code is outside 0..255.
    """
    # `bool` is rejected here, not left to protobuf: older runtimes accept `True`
    # as 1, which would be a silent CLEAR_RANGE.
    if not isinstance(value, int) or isinstance(value, bool):
        raise InputTypeError(
            f"{field}{at(index)} must be int, got {type(value).__name__}",
            field=field,
            index=index,
        )
    code = int(value)
    if not 0 <= code <= MAX_TYPE_CODE:
        raise InputValueError(
            f"{field}{at(index)} must be in 0..{MAX_TYPE_CODE}, got {_rendered(code)}",
            field=field,
            index=index,
        )
    return code
