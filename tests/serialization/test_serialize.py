import enum
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.serialization import (
    DECLARED_TYPE_CODES,
    MAX_BRIDGE_TIMESTAMP_NS,
    MAX_SEQUENCE_NO,
    MAX_TOTAL_MUTATIONS,
    MAX_TYPE_CODE,
    MAX_VERSION,
    InputTypeError,
    InputValueError,
    Mutation,
    MutationType,
    Record,
    RecordDecodeError,
    RecordTooLargeError,
    SerializationError,
    VersionIndex,
    deserialize_record,
    mutation_type_name,
    serialize_batch,
    serialize_mutation,
    serialize_version_end,
)
from tests.serialization import doubles
from tests.serialization.doubles import ADD, CLEAR, ENV, SET, STREAM, TS, V


def test_mutation_record_uses_the_real_wire_encoding() -> None:
    data = serialize_mutation(SET, fdb_version=V, sequence_no=0, **ENV)

    record = mutations_pb2.FDBMutationRecord.FromString(data)
    assert record.WhichOneof("record") == "mutation"
    assert record.mutation.WhichOneof("mutation") == "single_key_mutation"
    assert record.mutation.single_key_mutation.key == b"k"
    assert record.mutation.single_key_mutation.value == b"v"
    assert record.mutation.single_key_mutation.mutation_type == 0
    assert record.mutation.version_index.fdb_version == 2**40 + 7
    assert record.mutation.version_index.sequence_no == 0
    assert record.stream_name == "orders"
    assert record.bridge_timestamp.seconds == 1_700_000_000
    assert record.bridge_timestamp.nanos == 123_456_789


def test_clear_range_lands_in_clear_range_arm() -> None:
    data = serialize_mutation(CLEAR, fdb_version=V, sequence_no=0, **ENV)

    mutation = mutations_pb2.FDBMutationRecord.FromString(data).mutation
    assert mutation.WhichOneof("mutation") == "clear_range"
    assert mutation.clear_range.begin_key == b"a"
    assert mutation.clear_range.end_key == b"b\xff"


NON_CLEAR_DECLARED_TYPE_CODES = [0, 2, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20]


@pytest.mark.parametrize("code", NON_CLEAR_DECLARED_TYPE_CODES)
def test_declared_type_code_is_forwarded_verbatim(code: int) -> None:
    native = doubles.NativeMutation(code, b"k", b"v")

    data = serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    mutation = mutations_pb2.FDBMutationRecord.FromString(data).mutation
    assert mutation.WhichOneof("mutation") == "single_key_mutation"
    assert mutation.single_key_mutation.mutation_type == code
    assert deserialize_record(data) == Record(
        STREAM, TS, Mutation(code, b"k", b"v", VersionIndex(V, 0))
    )


def test_zero_version_index_is_present_on_the_wire() -> None:
    data = serialize_mutation(SET, fdb_version=0, sequence_no=0, **ENV)

    mutation = mutations_pb2.FDBMutationRecord.FromString(data).mutation
    assert mutation.HasField("version_index")


def test_zero_bridge_timestamp_is_present_on_the_wire() -> None:
    data = serialize_mutation(
        SET, fdb_version=V, sequence_no=0, stream_name=STREAM, bridge_timestamp_ns=0
    )

    assert mutations_pb2.FDBMutationRecord.FromString(data).HasField("bridge_timestamp")


@pytest.mark.parametrize(
    "call",
    [
        lambda: serialize_mutation(ADD, fdb_version=V, sequence_no=3, **ENV),
        lambda: serialize_batch([SET, ADD, CLEAR], fdb_version=V, **ENV),
        lambda: serialize_version_end(fdb_version=V, total_mutations=5, **ENV),
    ],
    ids=["mutation", "batch", "version_end"],
)
def test_equal_arguments_give_equal_bytes(call: Callable[[], bytes]) -> None:
    assert call() == call()


@pytest.mark.parametrize("code", [6, 13, 14, 15])
def test_legacy_and_dead_type_codes_are_forwarded_silently(
    code: int, recwarn: pytest.WarningsRecorder, caplog: pytest.LogCaptureFixture
) -> None:
    native = doubles.NativeMutation(code, b"k", b"v")

    with caplog.at_level(logging.DEBUG):
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)
        serialize_batch([native], fdb_version=V, **ENV)

    assert len(recwarn) == 0
    assert caplog.records == []


def test_batch_lands_in_batch_arm() -> None:
    data = serialize_batch([SET, ADD, CLEAR], fdb_version=V, **ENV)

    record = mutations_pb2.FDBMutationRecord.FromString(data)
    assert record.WhichOneof("record") == "batch"
    assert len(record.batch.mutations) == 3


def test_batch_from_a_generator_equals_batch_from_a_tuple() -> None:
    from_generator = serialize_batch(
        (m for m in (SET, ADD, CLEAR)), fdb_version=V, **ENV
    )

    assert from_generator == serialize_batch((SET, ADD, CLEAR), fdb_version=V, **ENV)


def test_version_end_sets_both_timestamps_equal() -> None:
    data = serialize_version_end(fdb_version=V, total_mutations=5, **ENV)

    record = mutations_pb2.FDBMutationRecord.FromString(data)
    assert record.WhichOneof("record") == "version_end"
    assert record.HasField("bridge_timestamp")
    assert record.version_end.HasField("bridge_timestamp")
    assert record.version_end.bridge_timestamp == record.bridge_timestamp
    assert (record.bridge_timestamp.seconds, record.bridge_timestamp.nanos) == divmod(
        TS, 10**9
    )


def test_mutation_type_mirrors_the_declared_type_codes() -> None:
    proto_enum = mutations_pb2.FDBSingleKeyMutation.MutationType

    assert {m.value for m in MutationType} == set(proto_enum.values())
    assert {m.value for m in MutationType} == {
        *(0, 1, 2, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    }
    assert {f"MUTATION_TYPE_{m.name}": m.value for m in MutationType} == dict(
        proto_enum.items()
    )
    assert DECLARED_TYPE_CODES == frozenset(MutationType)


def test_limits_have_the_native_and_proto_bounds() -> None:
    assert MAX_TYPE_CODE == 255
    assert MAX_VERSION == 9_223_372_036_854_775_807
    assert MAX_SEQUENCE_NO == 4_294_967_295
    assert MAX_TOTAL_MUTATIONS == 4_294_967_295
    assert MAX_BRIDGE_TIMESTAMP_NS == 253_402_300_799_999_999_999


@pytest.mark.parametrize(
    ("code", "name"),
    [
        (0, "SET_VALUE"),
        (20, "COMPARE_AND_CLEAR"),
        (3, "UNDECLARED_3"),
        (255, "UNDECLARED_255"),
    ],
)
def test_mutation_type_name_names_every_type_code(code: int, name: str) -> None:
    assert mutation_type_name(code) == name


def test_error_hierarchy() -> None:
    assert issubclass(InputTypeError, SerializationError)
    assert issubclass(InputTypeError, TypeError)
    assert issubclass(InputValueError, SerializationError)
    assert issubclass(InputValueError, ValueError)
    assert issubclass(RecordDecodeError, SerializationError)
    assert issubclass(RecordDecodeError, ValueError)
    assert issubclass(RecordTooLargeError, InputValueError)
    assert not issubclass(RecordDecodeError, DecodeError)


@pytest.mark.parametrize("code", [-1, 256, 2**31, 2**64])
def test_type_code_outside_uint8_is_a_value_error(code: int) -> None:
    native = doubles.NativeMutation(code, b"k", b"v")

    with pytest.raises(InputValueError) as excinfo:
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert excinfo.value.field == "type"
    assert excinfo.value.index is None


@pytest.mark.parametrize(
    ("code", "error"),
    [
        (-1, InputValueError),
        (256, InputValueError),
        (2**31, InputValueError),
        (2**64, InputValueError),
        (True, InputTypeError),
        (False, InputTypeError),
        (1.0, InputTypeError),
        ("1", InputTypeError),
        (None, InputTypeError),
    ],
)
def test_mutation_type_name_checks_its_code_like_a_type_code(
    code: Any, error: type[InputTypeError | InputValueError]
) -> None:
    with pytest.raises(error) as excinfo:
        mutation_type_name(code)

    assert excinfo.value.field == "code"
    assert excinfo.value.index is None


@pytest.mark.parametrize("field", ["param1", "param2"])
@pytest.mark.parametrize(
    "value",
    [None, bytearray(b"k"), memoryview(b"k"), "k", 5],
    ids=["None", "bytearray", "memoryview", "str", "int"],
)
def test_param_that_is_not_bytes_is_a_type_error(field: str, value: Any) -> None:
    native = doubles.NativeMutation(
        **{"type": 0, "param1": b"k", "param2": b"v"} | {field: value}
    )

    with pytest.raises(InputTypeError) as excinfo:
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert excinfo.value.field == field
    assert excinfo.value.index is None


@dataclass(frozen=True)
class MissingParam2:
    type: int
    param1: bytes


NON_CONFORMING_MUTATIONS = [
    pytest.param((0, b"k", b"v"), "type", id="bare-tuple"),
    pytest.param(None, "type", id="None"),
    pytest.param(MissingParam2(0, b"k"), "param2", id="missing-param2"),
]


@pytest.mark.parametrize(("native", "field"), NON_CONFORMING_MUTATIONS)
def test_object_without_the_three_attributes_is_not_a_native_mutation(
    native: Any, field: str
) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert excinfo.value.field == field
    assert excinfo.value.index is None
    assert isinstance(excinfo.value.__cause__, AttributeError)


@pytest.mark.parametrize(("native", "field"), NON_CONFORMING_MUTATIONS)
def test_batch_element_without_the_three_attributes_is_not_a_native_mutation(
    native: Any, field: str
) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        serialize_batch([native], fdb_version=V, **ENV)

    assert excinfo.value.field == field
    assert excinfo.value.index == 0
    assert isinstance(excinfo.value.__cause__, AttributeError)


class UpstreamVersion(enum.IntEnum):
    FIVE = 5


def _mutation_call(**overrides: Any) -> bytes:
    return serialize_mutation(
        SET, **{"fdb_version": V, "sequence_no": 0} | ENV | overrides
    )


def _batch_call(**overrides: Any) -> bytes:
    return serialize_batch([SET, ADD], **{"fdb_version": V} | ENV | overrides)


def _version_end_call(**overrides: Any) -> bytes:
    return serialize_version_end(
        **{"fdb_version": V, "total_mutations": 2} | ENV | overrides
    )


PRIMITIVES = [
    pytest.param(_mutation_call, id="serialize_mutation"),
    pytest.param(_batch_call, id="serialize_batch"),
    pytest.param(_version_end_call, id="serialize_version_end"),
]


@pytest.mark.parametrize("call", PRIMITIVES)
@pytest.mark.parametrize("fdb_version", [-1, 2**63, 2**64])
def test_fdb_version_outside_int64_is_a_value_error(
    call: Callable[..., bytes], fdb_version: int
) -> None:
    with pytest.raises(InputValueError) as excinfo:
        call(fdb_version=fdb_version)

    assert excinfo.value.field == "fdb_version"
    assert excinfo.value.index is None


@pytest.mark.parametrize("call", PRIMITIVES)
@pytest.mark.parametrize(
    "fdb_version",
    [True, 1.0, "1", None, UpstreamVersion.FIVE],
    ids=["bool", "float", "str", "None", "IntEnum"],
)
def test_fdb_version_that_is_not_exactly_int_is_a_type_error(
    call: Callable[..., bytes], fdb_version: Any
) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        call(fdb_version=fdb_version)

    assert excinfo.value.field == "fdb_version"
    assert excinfo.value.index is None


POSITION_REJECTIONS = [
    pytest.param(-1, InputValueError, id="negative"),
    pytest.param(2**32, InputValueError, id="above-uint32"),
    pytest.param(True, InputTypeError, id="bool"),
    pytest.param(None, InputTypeError, id="None"),
]


@pytest.mark.parametrize(("sequence_no", "error"), POSITION_REJECTIONS)
def test_sequence_no_outside_its_domain_is_rejected(
    sequence_no: Any, error: type[InputTypeError | InputValueError]
) -> None:
    with pytest.raises(error) as excinfo:
        serialize_mutation(SET, fdb_version=V, sequence_no=sequence_no, **ENV)

    assert excinfo.value.field == "sequence_no"
    assert excinfo.value.index is None


@pytest.mark.parametrize(("first_sequence_no", "error"), POSITION_REJECTIONS)
def test_first_sequence_no_outside_its_domain_is_rejected(
    first_sequence_no: Any, error: type[InputTypeError | InputValueError]
) -> None:
    with pytest.raises(error) as excinfo:
        serialize_batch(
            [SET], fdb_version=V, first_sequence_no=first_sequence_no, **ENV
        )

    assert excinfo.value.field == "first_sequence_no"
    assert excinfo.value.index is None


@pytest.mark.parametrize("call", PRIMITIVES)
@pytest.mark.parametrize(
    ("stream_name", "error"),
    [
        pytest.param("", InputValueError, id="empty"),
        pytest.param("\ud800", InputValueError, id="lone-surrogate"),
        pytest.param(b"orders", InputTypeError, id="bytes"),
        pytest.param(bytearray(b"orders"), InputTypeError, id="bytearray"),
        pytest.param(None, InputTypeError, id="None"),
    ],
)
def test_stream_name_outside_its_domain_is_rejected(
    call: Callable[..., bytes],
    stream_name: Any,
    error: type[InputTypeError | InputValueError],
) -> None:
    with pytest.raises(error) as excinfo:
        call(stream_name=stream_name)

    assert excinfo.value.field == "stream_name"
    assert excinfo.value.index is None


@pytest.mark.parametrize("call", PRIMITIVES)
@pytest.mark.parametrize(
    ("bridge_timestamp_ns", "error"),
    [
        pytest.param(-1, InputValueError, id="negative"),
        pytest.param(253_402_300_800_000_000_000, InputValueError, id="year-10000"),
        pytest.param(True, InputTypeError, id="bool"),
        pytest.param(1.5, InputTypeError, id="float"),
        pytest.param(datetime(2026, 1, 1, tzinfo=UTC), InputTypeError, id="aware"),
        pytest.param(datetime(2026, 1, 1), InputTypeError, id="naive"),
        pytest.param(Timestamp(), InputTypeError, id="Timestamp"),
        pytest.param(None, InputTypeError, id="None"),
    ],
)
def test_bridge_timestamp_outside_its_domain_is_rejected(
    call: Callable[..., bytes],
    bridge_timestamp_ns: Any,
    error: type[InputTypeError | InputValueError],
) -> None:
    with pytest.raises(error) as excinfo:
        call(bridge_timestamp_ns=bridge_timestamp_ns)

    assert excinfo.value.field == "bridge_timestamp_ns"
    assert excinfo.value.index is None


@pytest.mark.parametrize(("total_mutations", "error"), POSITION_REJECTIONS)
def test_total_mutations_outside_its_domain_is_rejected(
    total_mutations: Any, error: type[InputTypeError | InputValueError]
) -> None:
    with pytest.raises(error) as excinfo:
        serialize_version_end(fdb_version=V, total_mutations=total_mutations, **ENV)

    assert excinfo.value.field == "total_mutations"
    assert excinfo.value.index is None


@pytest.mark.parametrize(
    "mutations", [[], iter(())], ids=["empty-list", "exhausted-generator"]
)
def test_empty_batch_is_a_value_error(mutations: Any) -> None:
    with pytest.raises(InputValueError) as excinfo:
        serialize_batch(mutations, fdb_version=V, **ENV)

    assert excinfo.value.field == "mutations"
    assert excinfo.value.index is None


@pytest.mark.parametrize("mutations", [None, 5], ids=["None", "int"])
def test_batch_of_a_non_iterable_is_a_type_error(mutations: Any) -> None:
    with pytest.raises(InputTypeError) as excinfo:
        serialize_batch(mutations, fdb_version=V, **ENV)

    assert excinfo.value.field == "mutations"
    assert excinfo.value.index is None


def test_batch_of_bytes_fails_on_its_first_element_with_no_special_case() -> None:
    mutations: Any = b"abc"

    with pytest.raises(InputTypeError) as excinfo:
        serialize_batch(mutations, fdb_version=V, **ENV)

    assert excinfo.value.field == "type"
    assert excinfo.value.index == 0


@pytest.mark.parametrize(
    ("bad", "error", "field"),
    [
        pytest.param(
            doubles.NativeMutation(256, b"k", b"v"), InputValueError, "type", id="type"
        ),
        pytest.param(
            doubles.AttrOnlyMutation(0, b"k", bytearray(b"v")),  # type: ignore[arg-type]
            InputTypeError,
            "param2",
            id="param2",
        ),
    ],
)
def test_bad_mutation_in_a_batch_is_reported_at_its_position(
    bad: Any, error: type[InputTypeError | InputValueError], field: str
) -> None:
    with pytest.raises(error) as excinfo:
        serialize_batch([SET, bad, ADD], fdb_version=V, **ENV)

    assert excinfo.value.field == field
    assert excinfo.value.index == 1


def test_assigned_position_past_uint32_is_a_value_error_at_that_position() -> None:
    with pytest.raises(InputValueError) as excinfo:
        serialize_batch(
            [SET, ADD, CLEAR], fdb_version=V, first_sequence_no=2**32 - 2, **ENV
        )

    assert excinfo.value.field == "sequence_no"
    assert excinfo.value.index == 2


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"fdb_version": -1}, id="fdb_version"),
        pytest.param({"bridge_timestamp_ns": -1}, id="last-scalar"),
    ],
)
def test_invalid_scalar_leaves_the_generator_unconsumed(
    overrides: dict[str, Any],
) -> None:
    generator = (m for m in (SET, ADD, CLEAR))

    with pytest.raises(InputValueError):
        serialize_batch(generator, **{"fdb_version": V} | ENV | overrides)

    assert next(generator) == SET


SECRET_KEY = bytes.fromhex(
    "9f3c07e1a2b44d5688c1f0e39a7b2d14c5e6f708192a3b4c5d6e7f8091a2b3c4"
)


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(
            lambda native: serialize_mutation(
                native, fdb_version=V, sequence_no=0, **ENV
            ),
            id="serialize_mutation",
        ),
        pytest.param(
            lambda native: serialize_batch([SET, native], fdb_version=V, **ENV),
            id="serialize_batch",
        ),
    ],
)
def test_error_message_never_contains_key_content(
    call: Callable[[doubles.NativeMutation], bytes],
) -> None:
    with pytest.raises(InputValueError) as excinfo:
        call(doubles.NativeMutation(300, SECRET_KEY, SECRET_KEY))

    message = str(excinfo.value)
    assert "300" in message
    assert repr(SECRET_KEY) not in message
    assert repr(SECRET_KEY)[2:-1] not in message
    assert SECRET_KEY.hex() not in message
    assert SECRET_KEY.decode("latin-1") not in message


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(
            lambda: _mutation_call(stream_name="hunter2\ud800"), id="unencodable-name"
        ),
        pytest.param(lambda: _mutation_call(stream_name=b"hunter2"), id="bytes-name"),
        pytest.param(
            lambda: serialize_mutation(
                doubles.NativeMutation(0, "hunter2", b"v"),  # type: ignore[arg-type]
                fdb_version=V,
                sequence_no=0,
                **ENV,
            ),
            id="str-param1",
        ),
    ],
)
def test_error_message_never_contains_name_or_param_content(
    call: Callable[[], bytes],
) -> None:
    with pytest.raises(SerializationError) as excinfo:
        call()

    assert "hunter2" not in str(excinfo.value)


@pytest.mark.parametrize("code", [True, False, 1.0, "1", None])
def test_type_code_that_is_not_an_int_is_a_type_error(code: Any) -> None:
    native = doubles.NativeMutation(code, b"k", b"v")

    with pytest.raises(InputTypeError) as excinfo:
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert excinfo.value.field == "type"
    assert excinfo.value.index is None


# CPython refuses to render ints past ~4300 digits; an error message must not be
# where that surfaces.
UNRENDERABLE = 10**5000


def test_unrenderable_fdb_version_is_still_a_value_error() -> None:
    with pytest.raises(InputValueError) as excinfo:
        serialize_version_end(fdb_version=UNRENDERABLE, total_mutations=0, **ENV)

    assert excinfo.value.field == "fdb_version"


def test_unrenderable_type_code_is_still_a_value_error() -> None:
    native = doubles.NativeMutation(-UNRENDERABLE, b"k", b"v")

    with pytest.raises(InputValueError) as excinfo:
        serialize_mutation(native, fdb_version=V, sequence_no=0, **ENV)

    assert excinfo.value.field == "type"


def test_unrenderable_code_is_still_a_value_error_for_mutation_type_name() -> None:
    with pytest.raises(InputValueError) as excinfo:
        mutation_type_name(UNRENDERABLE)

    assert excinfo.value.field == "code"
