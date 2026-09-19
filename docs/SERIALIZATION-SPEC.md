# Serialization module spec

Contract for the `src/serialization/` package: native CDC mutations → Protobuf
`FDBMutationRecord` bytes and back. The build order is the test list in
[§12](#12-ordered-test-list). Trello:
[Implement Binary Serializer & Deserializer Module](https://trello.com/c/amcMAYnn).

- These terms are used exactly: native mutation, type code,
  declared / undeclared type code, version group, batch, consume reply, version index, mutation,
  record, record body, round trip, version end, bridge timestamp, stream name, undecodable record.
  "Arm" names a proto `oneof` case only; the domain word is record body.
- [`mutations.proto`](../protobuf/proto/fdbkafka/cdc/v1/mutations.proto) is taken as-is. Its gaps
  are worked around here and listed in [§14](#14-schema-follow-ups).

## 1. Scope

In: four serialize functions, one deserialize function, the input `Protocol`, the output values,
the error taxonomy, and their tests.

Out: snapshot arms (`FDBSnapshotRow`, `SnapshotChunkEnd`, `SnapshotEnd`); Kafka record key,
partition and topic layout; Schema Registry framing; producing records; idle-watermark policy;
throughput benchmarks; how the bridge image installs the runtime and exposes `protobuf/gen`.

## 2. Principles

1. **The type code is the only semantic discriminator; the proto arm is an encoding detail.** The
   serializer forwards native mutations as-is
   ([deep-dive §5](CDC-DEEP-DIVE.md#5-mapping-onto-the-kafka-proposal)): no rewrite, no collapse of
   atomics, no operand validation, no logging.
2. **The serializer's rejection set is disjoint from anything CDC can deliver.** With real CDC
   input and a start-up-validated stream name, no serializer error can fire. A rejecting
   serializer stalls the stream, and a stalled stream is a cluster-wide write outage (L3).
3. **On read, strict for identity, tolerant for metadata.** The deserializer rejects only what
   makes the identity-bearing parts of a record (record body, version index, type code, batch
   shape) uninterpretable or non-re-serializable. Envelope fields (stream name, bridge timestamp)
   never fail deserialization.
4. **Pure functions.** No state, no clock, no I/O, no logging. Equal arguments give equal bytes.
   Inputs are never mutated or retained.
5. **All-or-nothing.** Every call returns its complete result or raises; there is no partial
   output.
6. **The output is the serializer's inputs, as immutable values.** What went in through the
   keywords comes out as attributes under the same names. One carve-out: a mutation's
   `fdb_version` and `sequence_no` come out together as `version_index`, because the pair is the
   dedup identity and is compared as one value. No generated code crosses the public API.
7. **Nothing is left to protobuf to decide.** Every value is checked before protobuf sees it; no
   protobuf-raised exception defines behaviour or escapes.

## 3. Package

Imported as `src.serialization`; product code imports generated code as
`from fdbkafka.cdc.v1 import mutations_pb2` and never touches `sys.path`
(`PYTHONPATH=protobuf/gen` at runtime, pytest `pythonpath` in tests). Runtime dependency:
`protobuf>=7.34,<8`.

Everything below is exported from the package root and listed in `__all__`. Only these names are
contract; the file split is a suggestion (`model.py` — Protocol, enum, constants, output values;
`errors.py`; `serializer.py`; `version_group.py`; `deserializer.py`; `__init__.py` re-exports).

```python
__all__ = [
    # input
    "NativeMutation", "MutationType", "DECLARED_TYPE_CODES", "mutation_type_name",
    # limits
    "MAX_TYPE_CODE", "MAX_VERSION", "MAX_SEQUENCE_NO", "MAX_TOTAL_MUTATIONS",
    "MAX_BRIDGE_TIMESTAMP_NS",
    # functions
    "serialize_mutation", "serialize_batch", "serialize_version_end",
    "serialize_version_group", "deserialize_record",
    # output values
    "VersionIndex", "Mutation", "MutationBatch", "VersionEnd", "RecordBody", "Record",
    # errors
    "SerializationError", "InputTypeError", "InputValueError", "RecordTooLargeError",
    "DecodeFailure", "RecordDecodeError",
]
```

## 4. API surface

### 4.1 Input

```python
class NativeMutation(Protocol):
    """One native mutation as FDB CDC delivers it; upstream `CdcMutation` conforms."""

    @property
    def type(self) -> int: ...
    @property
    def param1(self) -> bytes: ...
    @property
    def param2(self) -> bytes: ...
```

- Not `@runtime_checkable`. Conformance is established by reading the three attributes, by name,
  once each, in the order `type`, `param1`, `param2`. Never indexed, unpacked or `isinstance`-ed,
  so `NamedTuple`s, frozen dataclasses and objects with extra attributes all work; extras never
  reach the wire.
- A bare tuple `(0, b"k", b"v")` is not a native mutation (no `.type`) and is rejected.
- The package ships no concrete input class. `CdcVersionedMutations` / `CdcConsumeResult`-shaped
  objects are not accepted: the call is `serialize_version_group(g.mutations, fdb_version=g.version, …)`.

```python
class MutationType(enum.IntEnum):
    """The 16 declared type codes, mirroring upstream `CdcMutationType`."""

    SET_VALUE = 0
    CLEAR_RANGE = 1
    ADD = 2
    AND = 6
    OR = 7
    XOR = 8
    APPEND_IF_FITS = 9
    MAX = 12
    MIN = 13
    SET_VERSIONSTAMPED_KEY = 14
    SET_VERSIONSTAMPED_VALUE = 15
    BYTE_MIN = 16
    BYTE_MAX = 17
    MIN_V2 = 18
    AND_V2 = 19
    COMPARE_AND_CLEAR = 20


DECLARED_TYPE_CODES: Final[frozenset[int]] = frozenset(MutationType)

MAX_TYPE_CODE: Final = 255                  # native uint8_t
MAX_VERSION: Final = 2**63 - 1              # native int64_t commit version
MAX_SEQUENCE_NO: Final = 2**32 - 1          # proto uint32
MAX_TOTAL_MUTATIONS: Final = 2**32 - 1      # proto uint32
MAX_BRIDGE_TIMESTAMP_NS: Final = 253_402_300_799_999_999_999  # 9999-12-31T23:59:59.999999999Z


def mutation_type_name(code: int) -> str:
    """`"SET_VALUE"` for a declared type code, `"UNDECLARED_<code>"` for any other in 0..255."""
```

`MutationType` is an input, test and display convenience only. It is never used for a lookup on
the read path, and the deserializer never returns it. `mutation_type_name` validates `code`
exactly like a type code ([§6.2](#62-integer-rule)) with `field == "code"`; it never raises for
0..255.

### 4.2 Serialize functions

The first positional argument is the native thing; **everything else is keyword-only and
required** (except `first_sequence_no`). `fdb_version` is the commit version under that one name
everywhere — keywords, output attributes, error `field`s — matching the proto field, as every
other keyword does. Upstream's `CdcVersionedMutations.version` is passed as
`fdb_version=g.version`.

```python
def serialize_mutation(
    mutation: NativeMutation,
    *,
    fdb_version: int,
    sequence_no: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> bytes: ...


def serialize_batch(
    mutations: Iterable[NativeMutation],
    *,
    fdb_version: int,
    first_sequence_no: int = 0,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> bytes: ...


def serialize_version_end(
    *,
    fdb_version: int,
    total_mutations: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> bytes: ...


def serialize_version_group(
    mutations: Iterable[NativeMutation],
    *,
    fdb_version: int,
    max_record_bytes: int,
    stream_name: str,
    bridge_timestamp_ns: int,
) -> list[bytes]: ...
```

Three primitives — one per record body, each returning the bytes of exactly one record, with no
size cap — and one composite built from them.

**`serialize_mutation`** → one record whose body is the mutation at version index
`(fdb_version, sequence_no)`. The caller owns the position; `sequence_no > 0` with no predecessor is
legal. Used when a topic wants one Kafka record per mutation (loop over
`enumerate(group.mutations)`, close with `serialize_version_end(total_mutations=n)`).

**`serialize_batch`** → one record whose body is a batch. The i-th mutation yielded (0-based) gets
version index `(fdb_version, first_sequence_no + i)`. `mutations` is consumed exactly once, in order;
generators are legal. Zero mutations is an error (an empty batch carries no version, so a reader
could attribute it to nothing). A one-element batch stays a batch: the record body follows the
function called, never the mutation count.
Precondition the serializer cannot verify: `mutations` is a contiguous, unfiltered, native-order
run of one version group and `first_sequence_no` is the native index of its first element.
Filtering before serializing silently corrupts the dedup identity.

**`serialize_version_end`** → one record whose body is the version end `(version,
total_mutations)`. `total_mutations` is always set by the caller, counts native mutations (not
records) of the whole version group however it was sliced, and `0` means "no mutations at this
version" — an empty version group and an idle watermark are deliberately the same bytes. Both
proto timestamp fields (`FDBMutationRecord.bridge_timestamp`, `VersionEnd.bridge_timestamp`) are
set, to the same `bridge_timestamp_ns`.

**`serialize_version_group`** → the whole version group as its ordered record sequence: zero or
more batch records covering version indexes `(fdb_version, 0) … (fdb_version, n-1)` in order, then exactly
one version-end record with `total_mutations == n`, counted by the module. `records[-1]` is always
the version end; an empty group returns exactly `[version_end(fdb_version, 0)]`. All records share the
call's envelope.

- **Budget.** `max_record_bytes` bounds `len()` of each returned record value and nothing else.
  Kafka key, headers, record-batch and Schema Registry framing are the caller's allowance to
  subtract. No default: the module does not know the broker config.
- **Slicing.** Greedy, in native order: a slice is closed only when appending the next mutation
  would push its record over budget. Deterministic.
- **Equivalence.** The k-th batch record is byte-identical to `serialize_batch(ms[a:b],
  fdb_version=fdb_version, first_sequence_no=a, …)` for its slice `[a, b)`; slices are disjoint, ordered,
  and cover `[0, n)`. The last record is byte-identical to
  `serialize_version_end(fdb_version=fdb_version, total_mutations=n, …)`.
- **Unsplittable.** A mutation whose one-element batch record alone exceeds the budget, or a
  budget below the version-end record's size, raises `RecordTooLargeError`. A mutation is never
  split.
- **Completeness** is signalled by the version end alone; there is no last-slice flag or slice
  counter.
- Precondition (unverifiable): `mutations` is the *whole* group. A slice belongs in
  `serialize_batch`; passing one here emits a wrong `total_mutations`.
- Returns a `list`, not an iterator, so an invalid mutation late in the group raises before the
  bridge has produced a partial group. Cost: a ≤ 10 MB group held twice.

Non-normative sizing note: FDB caps keys at 10,000 B and values at 100,000 B
([deep-dive §5.1](CDC-DEEP-DIVE.md#51-kafka-record-key-partitioning-and-atomicity-)), so a budget
≥ 128 KiB plus the stream name's length can never hit `RecordTooLargeError` on real CDC data. The
bridge should refuse a smaller configured budget at start-up. The module itself accepts any
budget ≥ 1; tests need small ones.

Envelope construction: the proto `Timestamp` is built from `bridge_timestamp_ns` exactly
(`seconds, nanos = divmod(ns, 10**9)`) and passed as a **constructor kwarg**, never
attribute-assigned, so `bridge_timestamp_ns == 0` stays present on the wire (T11; if a
`Timestamp(0, 0)` kwarg ever loses presence, `record.bridge_timestamp.SetInParent()` restores
it). The version index is
likewise always constructed, so `(0, 0)` stays present.

### 4.3 Output values and `deserialize_record`

```python
class VersionIndex(NamedTuple):
    """Identity of a mutation on the wire; orders lexicographically in stream order."""

    fdb_version: int
    sequence_no: int


@dataclass(frozen=True, slots=True)
class Mutation:
    """A native mutation tagged with its version index. Satisfies `NativeMutation`."""

    type: int  # raw type code 0..255, always plain `int`
    param1: bytes
    param2: bytes
    version_index: VersionIndex


@dataclass(frozen=True, slots=True)
class MutationBatch:
    """The mutations of one batch record, in wire order."""

    mutations: tuple[Mutation, ...]


@dataclass(frozen=True, slots=True)
class VersionEnd:
    """A version end: the group at `fdb_version` is complete and held `total_mutations`."""

    fdb_version: int
    total_mutations: int


type RecordBody = Mutation | MutationBatch | VersionEnd


@dataclass(frozen=True, slots=True)
class Record:
    """One deserialized record: envelope fields plus exactly one record body."""

    stream_name: str  # as on the wire; may be "" from a foreign producer
    bridge_timestamp_ns: int | None  # None iff absent on the wire
    body: RecordBody


def deserialize_record(data: bytes) -> Record:
    """Parse one Kafka record value. Returns a complete `Record` or raises."""
```

- Values are immutable, hashable, compared field-wise, and validate nothing in `__post_init__`.
  Only instances returned by `deserialize_record` carry the guarantees below.
- `Mutation` is deliberately not a tuple: it never equals, unpacks as, or is confused with a
  native 3-tuple. `VersionIndex` deliberately is one: `m.version_index <= last_applied` and
  `out.version_index == (v, s)` work as written.
- `MutationBatch` has no `fdb_version` of its own; every `Mutation` carries its version index.
- Always fully materialised; there is no streaming form. A Kafka tombstone (`value is None`) is
  the caller's to filter.
- A mutation record and a one-element batch record of the same mutation deserialize to **unequal**
  `Record`s whose contained `Mutation`s are equal. For a reader they mean the same thing
  ([§9](#9-wire-contract-for-one-version-group)).
- Dispatch is structural `match`; there is no tag enum, no per-body function, no flattening
  helper:

```python
match deserialize_record(value).body:
    case Mutation() as m: ...
    case MutationBatch(mutations=ms): ...
    case VersionEnd(fdb_version=v, total_mutations=n): ...
```

**Guarantees on every returned `Record`.** `body` and `version_index` are never `None`;
`type(m.type) is int` and `0 <= m.type <= 255`; every version is in `0..MAX_VERSION`; a batch is
non-empty, single-version, with contiguous ascending positions. Hence **every returned record body
is re-serializable**: `Mutation` feeds `serialize_mutation`, `MutationBatch.mutations` feeds
`serialize_batch` (with `fdb_version` / `first_sequence_no` from `mutations[0].version_index`),
`VersionEnd` feeds `serialize_version_end`. The guarantee does not extend to the envelope: `""`,
`None` or an out-of-range timestamp from a foreign producer are returned as-is and are not legal
serializer inputs.

### 4.4 Wire mapping

| Native / keyword | Wire |
|---|---|
| `type == 1` | `FDBMutation.clear_range`: `begin_key = param1`, `end_key = param2` |
| any other `type` in 0..255 | `FDBMutation.single_key_mutation`: `key = param1`, `value = param2`, `mutation_type = type` (raw int through the open enum) |
| `fdb_version`, `sequence_no` / `first_sequence_no + i` | `FDBMutation.version_index` (`fdb_version`, `sequence_no`), always present |
| `fdb_version`, `total_mutations` | `VersionEnd.fdb_version`, `VersionEnd.total_mutations` |
| `stream_name` | `FDBMutationRecord.stream_name` |
| `bridge_timestamp_ns` | `FDBMutationRecord.bridge_timestamp`, and `VersionEnd.bridge_timestamp` in a version-end record |

| Wire | Output |
|---|---|
| `record.mutation` | `body = Mutation(…)` |
| `record.batch` | `body = MutationBatch(tuple(Mutation(…) for each, wire order))` |
| `record.version_end` | `body = VersionEnd(fdb_version=fdb_version, total_mutations=total_mutations)` |
| `FDBMutation.version_index` | `VersionIndex(fdb_version, sequence_no)` |
| `single_key_mutation` | `type = mutation_type`, `param1 = key`, `param2 = value` |
| `clear_range` | `type = 1`, `param1 = begin_key`, `param2 = end_key` (type synthesised; the arm has no type field) |
| `FDBMutationRecord.bridge_timestamp` | `seconds * 10**9 + nanos` computed from the two fields directly — no `Timestamp` helper, no range check; `None` iff `HasField` is false |
| `VersionEnd.bridge_timestamp`, unknown fields | dropped, never read or cross-checked |

The serializer never emits `single_key_mutation` with `mutation_type == 1`. The deserializer
accepts that non-canonical form and normalises it: both clear encodings yield equal `Mutation`s,
so bytes → values → bytes is byte-identical only for canonical input.

## 5. Type matrix

| Native `type` | Serializer | Lands in | Deserializer returns |
|---|---|---|---|
| 1 `CLEAR_RANGE` | accept | `clear_range` | `1` |
| 0, 2, 7, 8, 9, 12, 16–20 (declared) | accept verbatim | `single_key_mutation` | same int |
| 6 `AND`, 13 `MIN` (legacy API < 510) | accept verbatim, silently; never rewritten to `_V2` | `single_key_mutation` | same int |
| 14, 15 `SET_VERSIONSTAMPED_*` (dead on the wire today) | accept verbatim, silently; never rewritten to `SET_VALUE` | `single_key_mutation` | same int |
| undeclared 3–5, 10, 11, 21–23, 24–255 | accept verbatim, silently | `single_key_mutation` | same int |
| `IntEnum` member in 0..255 | accept, normalised with `int(t)` | as its value | plain `int` |
| `int` outside 0..255 | `InputValueError` | — | — |
| `bool`, `float`, `str`, `None`, `__index__`-only objects | `InputTypeError` | — | — |
| wire `mutation_type` outside 0..255 | (cannot be written) | — | `RecordDecodeError(TYPE_CODE_OUT_OF_RANGE)` |
| wire `single_key_mutation` with `mutation_type == 1` | (never written) | — | `1`, equal to the canonical form |
| wire `single_key_mutation` with `mutation_type` absent | — | — | `0` (`SET_VALUE`); undetectable, see [§14](#14-schema-follow-ups) item 4 |

There is no strict mode for type codes and no per-type parameter rule: `param2 == b""` for
`SET_VALUE`, any operand width for `ADD` / `MIN` / `MAX` / `*_V2`, any `COMPARE_AND_CLEAR`
operand, and the versionstamp offset suffix are all forwarded uninspected. Type handling is
identical inside a batch and alone.

## 6. Serializer validation

### 6.1 Accepted domains

| Input | `InputTypeError` | `InputValueError` | Accepted |
|---|---|---|---|
| mutation object | bare tuple, `None`, any object missing one of the three attributes | — | anything exposing `type`, `param1`, `param2` |
| `mutations` | not iterable | zero mutations, in `serialize_batch` only | any iterable, consumed once; empty is legal in `serialize_version_group` |
| `type` | `bool`, any non-`int` | outside 0..255 | `int` / `IntEnum` in 0..255 |
| `param1`, `param2` | not `isinstance(p, bytes)`: `bytearray`, `memoryview`, `str`, `int`, `None` | — | any `bytes`, any length, any content (`b""`, `\xff`-prefixed, > FDB limits) |
| `fdb_version` | `type(x) is not int` | `< 0`, `> MAX_VERSION` | `0..2**63-1` (`0` is legal) |
| `sequence_no`, `first_sequence_no` | `type(x) is not int` | `< 0`, `> MAX_SEQUENCE_NO`; any assigned `first_sequence_no + i > MAX_SEQUENCE_NO` | `0..2**32-1` |
| `total_mutations` | `type(x) is not int` | `< 0`, `> MAX_TOTAL_MUTATIONS` | `0..2**32-1` |
| `max_record_bytes` | `type(x) is not int` | `< 1`; unsplittable → `RecordTooLargeError` | `>= 1` |
| `stream_name` | not `str` (`bytes`, `bytearray`, `None`) | `""`; not UTF-8-encodable (lone surrogate) | any other `str`, byte-exact: no strip, fold, normalisation or length cap; whitespace-only and embedded NUL are fine |
| `bridge_timestamp_ns` | `type(x) is not int` (`datetime`, `Timestamp`, `float`, `bool`, `None`) | `< 0`, `> MAX_BRIDGE_TIMESTAMP_NS` | `0..MAX_BRIDGE_TIMESTAMP_NS` |

Not checked, by decision: FDB key/value size limits, system keys, empty keys/values/operands,
clear-range key order (`begin == end`, `begin > end`, empty `begin`, `end == b"\xff"`,
`(k, k + b"\x00")` all forwarded unchanged), declared-only type codes, operand widths, record size
on the primitives. Nothing is ever coerced (`bytes(5)` is five NULs).

### 6.2 Integer rule

One rule with one exception:

- **Every integer keyword** (`fdb_version`, `sequence_no`, `first_sequence_no`, `total_mutations`,
  `max_record_bytes`, `bridge_timestamp_ns`): `type(x) is int`. Rejects `bool` on every runtime,
  plus `float`, `str`, `None`, `IntEnum`, numpy scalars.
- **The type code only** (`mutation.type`, and `code` of `mutation_type_name`):
  `isinstance(t, int) and not isinstance(t, bool)`, normalised with `int(t)`. Upstream's
  `CdcMutationType` is an `IntEnum` and callers will naturally pass it; no other field has that
  use case. `bool` must be rejected by this check, not protobuf's (`True` would otherwise be a
  silent `CLEAR_RANGE` on older runtimes).

### 6.3 Check order

Keyword scalars in signature order ([§4.2](#42-serialize-functions)), then mutations in iteration
order, each as `type`, `param1`, `param2`; for each value, type check before range check. Scalars
are checked before `mutations` is touched, so an invalid `fdb_version` leaves a generator unconsumed.
The empty-batch check runs after iteration, so an exhausted generator is caught like `[]`. All
checks finish before any proto message is built (protobuf silently swallows `None` constructor
kwargs), and serializer code has no `except` around protobuf calls. `AttributeError` from the
three attribute reads is re-raised as `InputTypeError` `from` the original.

Protobuf's 2 GiB `EncodeError` is unreachable under CDC's 10 MB reply budget and is neither
wrapped nor tested.

## 7. Deserializer checks

`data` must be `bytes` (`isinstance`); `bytearray`, `memoryview`, `str`, `None` →
`InputTypeError(field="data")`. Then, each failure a `RecordDecodeError` with the `reason` shown:

| Condition | `reason` | `index` |
|---|---|---|
| protobuf cannot parse: corrupt, truncated, invalid UTF-8 `stream_name` | `MALFORMED_WIRE`, original exception as `__cause__` | `None` |
| `WhichOneof("record") is None`: `b""`, truncation at a field boundary, foreign proto bytes, an arm added by a newer schema | `NO_RECORD_ARM` | `None` |
| a mutation without `HasField("version_index")` | `MISSING_VERSION_INDEX` | batch position, else `None` |
| a version index or version end with `fdb_version > MAX_VERSION` | `VERSION_OUT_OF_RANGE` | batch position, else `None` |
| a mutation with `WhichOneof("mutation") is None` | `NO_MUTATION_ARM` | batch position, else `None` |
| `mutation_type` outside 0..255 | `TYPE_CODE_OUT_OF_RANGE` | batch position, else `None` |
| `batch` arm with zero mutations | `EMPTY_BATCH` | `None` |
| batch with mixed `fdb_version`, or `sequence_no[i] != sequence_no[0] + i` | `INCONSISTENT_BATCH` | first offending position |

- Per-mutation checks run on every batch element before the batch-consistency check, so an
  element with no version index is `MISSING_VERSION_INDEX`, never read as `(0, 0)`. Order among
  the other checks is unspecified; tests craft single-fault input.
- Parsing: exactly one `ParseFromString` call wrapped in `except (DecodeError, ValueError)`
  (pure-python raises `UnicodeDecodeError` where upb raises `DecodeError`). Product code does not
  assert the upb backend. `DecodeError` never escapes.
- **Accepted, not errors:** an arm that is present but all-defaults (empty `single_key_mutation` =
  `SET_VALUE(b"", b"")`; empty `clear_range` = `CLEAR_RANGE(b"", b"")`; `(0, 0)` version index;
  empty `version_end` = `VersionEnd(0, 0)`); the non-canonical clear encoding; undeclared type
  codes; a batch starting at any position; unknown fields at any depth (ignored, dropped, never
  preserved); duplicate arms (protobuf's last-wins).
- **Envelope never fails:** `stream_name == ""` is returned as `""`; a missing timestamp is
  `None`; an out-of-range or negative timestamp is returned as the integer it encodes. The inner
  `VersionEnd.bridge_timestamp` is ignored.
- **Not verified** (stateless, one record at a time): that a group's first slice starts at 0,
  continuity across records, `total_mutations` agreeing with anything, `fdb_version` monotonicity.
  These are the reader's ([§9](#9-wire-contract-for-one-version-group)).
- **Whole defence against foreign bytes** is the arm / version-index presence checks. A foreign
  proto message that populates field 3, 4 or 5 with a well-formed submessage is undetectable; type
  identity belongs to the topic and Schema Registry framing.
- A **new `record` arm** is an unknown field to an old reader and surfaces as `NO_RECORD_ARM`:
  readers must be upgraded before a new arm is produced.

## 8. Error taxonomy

```python
class SerializationError(Exception):
    """Base of every error this package raises deliberately."""


class InputTypeError(SerializationError, TypeError):
    """An argument or native-mutation attribute has the wrong Python type."""

    field: str
    index: int | None


class InputValueError(SerializationError, ValueError):
    """An argument or native-mutation attribute is outside its accepted domain."""

    field: str
    index: int | None


class RecordTooLargeError(InputValueError):
    """One mutation, or the version end, cannot fit `max_record_bytes`."""

    record_bytes: int
    max_record_bytes: int


class DecodeFailure(enum.Enum):
    MALFORMED_WIRE = "malformed_wire"
    NO_RECORD_ARM = "no_record_arm"
    NO_MUTATION_ARM = "no_mutation_arm"
    MISSING_VERSION_INDEX = "missing_version_index"
    TYPE_CODE_OUT_OF_RANGE = "type_code_out_of_range"
    VERSION_OUT_OF_RANGE = "version_out_of_range"
    EMPTY_BATCH = "empty_batch"
    INCONSISTENT_BATCH = "inconsistent_batch"  # mixed versions or non-contiguous sequence_no


class RecordDecodeError(SerializationError, ValueError):
    """Bytes are not a record this package can interpret."""

    reason: DecodeFailure
    index: int | None
```

**Raises contract.** Serialize functions raise only `InputTypeError` / `InputValueError`
(`RecordTooLargeError` from `serialize_version_group` only). `deserialize_record` raises only
`InputTypeError` / `RecordDecodeError`. `mutation_type_name` raises only `InputTypeError` /
`InputValueError`. `RecordDecodeError` is not a `google.protobuf.message.DecodeError`.

**Context.**

- `field` is the offending parameter or attribute name as spelled in the public signature or
  `NativeMutation`: `"fdb_version"`, `"sequence_no"`, `"first_sequence_no"`, `"total_mutations"`,
  `"max_record_bytes"`, `"stream_name"`, `"bridge_timestamp_ns"`, `"mutations"`, `"type"`,
  `"param1"`, `"param2"`, `"data"`, `"code"`.
  - Batch position overflow: `field == "sequence_no"`, `index` = the overflowing position.
  - Non-conforming mutation object: `field` = the first attribute that could not be read (`"type"`
    for a bare tuple or `None`; `"param2"` for an object missing only that), `__cause__` is the
    `AttributeError`.
  - Non-iterable or empty `mutations`: `field == "mutations"`, `index is None`.
  - `RecordTooLargeError`: `field == "max_record_bytes"`, `index` = position of the unsplittable
    mutation (`None` when the version end is what does not fit), `record_bytes` = size of the
    record that did not fit.
- `index` is the 0-based position in `mutations` (serialize) or in the wire batch (deserialize);
  `None` outside a batch.
- Messages name field, position, offending `int` values and the version index where known. For
  `bytes` / `str` inputs they state Python type and length only — never raw key, value or name
  content.

**Handling.**

- `InputTypeError`, `InputValueError`, `RecordTooLargeError` are programmer or configuration
  errors. The bridge must not catch-and-skip (dropping one mutation breaks the version group):
  fail loudly, do not acknowledge, redeliver after the fix.
- `RecordDecodeError` marks an undecodable record — bad data, the only error a reader is expected
  to route around (skip or dead-letter, plus alarm); `reason` is the metric label. The bridge
  never deserializes on its produce path, so it never sees one.
- Strict only: no `strict=` / `validate=` flag, no unchecked twin. Every surviving check is
  mandatory.

## 9. Wire contract for one version group

Reader guidance; the module guarantees this shape within one `serialize_version_group` call and
enforces nothing across calls.

- For version V of a stream, a partition carries mutation-bearing records (mutation or batch
  records, freely mixed) whose version indexes run `(V, 0) … (V, n-1)` gap-free and ascending
  across records, then `VersionEnd(V, n)` as the group's last record.
- A mutation record ≡ a one-element batch with that version index. Readers must accept both.
- `VersionEnd(V, n)` means: every mutation of this stream with commit version ≤ V has been emitted
  ahead of this record; `n` of them are at exactly V. `n == 0` is "nothing at V, advance to V".
- Absent redelivery, `fdb_version` on successive version ends is strictly increasing. A repeat or
  regression, or `(V, 0)` reappearing before the version end, signals redelivery: discard the
  partial buffer ([deep-dive §5.1](CDC-DEEP-DIVE.md#51-kafka-record-key-partitioning-and-atomicity-)).
- The bridge timestamp is not identity. A redelivered group gets new timestamps and different
  bytes; nothing may dedup, hash or compare on serialized bytes or on the timestamp. Identity is
  the version index — and, because the schema has no `stream_id`
  ([§14](#14-schema-follow-ups) item 1), the stream's identity must travel outside the record
  value for now (Kafka producer module).

Bridge notes (non-normative): call `time.time_ns()` once per serializer call; decode
`CdcStreamInfo.name` as UTF-8 once at start-up and refuse a non-UTF-8 or empty name there; for an
empty consume reply that advanced the cursor emit
`serialize_version_end(fdb_version=result.last_consumed_version, total_mutations=0, …)`; never emit
for a fresh cursor (`last_consumed_version == -1` is rejected as an invalid version); bind the
stream name with `functools.partial` if wanted.

## 10. Edge-case catalogue

Accepted inputs that look suspicious, and rejected inputs that look harmless. Tests are
[§12](#12-ordered-test-list) ids.

| Case | Outcome | Test |
|---|---|---|
| `SET_VALUE` with `param2 == b""`; `b""` key; `b""` atomic operand | round-trips unchanged | T07 |
| 10,001 B key, 100,001 B value, `b"\xff\xff/x"` key | round-trips unchanged (no FDB limits, no system-key case) | T07 |
| `ADD` with 0-, 1-, 3-, 9-byte operands | round-trips unchanged | T07 |
| clear `(a, a)`, `(b, a)`, `(b"", z)`, `(a, \xff)`, `(k, k + \x00)` | round-trips unchanged | T08 |
| type codes 6, 13, 14, 15 | forwarded verbatim; no warning, no log record | T04, T13 |
| undeclared type codes incl. 255 | forwarded verbatim; `mutation_type_name` does not raise | T05, T32 |
| `IntEnum` type code | accepted; comes back plain `int` | T09 |
| `True` as type code, `True` as `fdb_version` | `InputTypeError` on every runtime | T34, T37 |
| `IntEnum` member as `fdb_version` | `InputTypeError` (integer rule's exception covers the type code only) | T37 |
| `None` as `param1` / `param2` | `InputTypeError` — must not yield a record with an empty key | T35 |
| bare tuple, `None`, object missing `param2` as a mutation | `InputTypeError`, `__cause__` is `AttributeError` | T36 |
| `serialize_batch(b"abc", …)` | `InputTypeError`, `field == "type"`, `index == 0` (elements have no `.type`; no special case) | T39 |
| `fdb_version == 0`; version index `(0, 0)` | legal; present on the wire | T06 |
| `fdb_version == -1` (fresh cursor), `2**63` | `InputValueError` | T37 |
| `sequence_no > 0` with no predecessor | legal | T06 |
| `first_sequence_no = 2**32 - 2` with three mutations | `InputValueError`, `field == "sequence_no"`, `index == 2`, nothing returned | T41 |
| empty `serialize_batch` (list or exhausted generator) | `InputValueError`, `field == "mutations"` | T39 |
| empty `serialize_version_group` | exactly `[version_end(V, 0)]` | T22 |
| one-element batch | stays a batch; its record ≠ the mutation record, contained `Mutation`s equal | T16 |
| `bridge_timestamp_ns == 0` | round-trips as `0`, not `None` | T11 |
| `stream_name` `"   "`, `"a\x00b"`, `"übër/注文"`, 10 kB | round-trip exactly | T10 |
| `stream_name` `""`, `"\ud800"`, `b"orders"` | value, value, type error | T43 |
| `total_mutations == 0` | legal: empty group or idle watermark | T19 |
| mutation larger than `max_record_bytes`; budget below the version-end record | `RecordTooLargeError`, nothing returned | T47 |
| invalid mutation at position 90 of 100 in a group | raises, nothing returned | T48 |
| `b""`, foreign proto bytes, envelope-only record, future arm | `NO_RECORD_ARM` | T51 |
| every proper prefix of a golden record | `RecordDecodeError` (no arm at the three field boundaries, malformed elsewhere) | T64 |
| wire batch positions `5, 6, 7` | accepted | T59 |
| wire batch `0, 2` / `1, 0` / `0, 0` / two versions | `INCONSISTENT_BATCH`, `index == 1` | T58 |
| all-defaults arms | accepted as `SET_VALUE(b"", b"")` at `(0, 0)` / `VersionEnd(0, 0)` | T60 |
| non-canonical clear on the wire | normalised; re-serializes to the `clear_range` arm | T61 |
| missing / out-of-range / negative envelope timestamp, unset `stream_name`, differing inner timestamp | `None` / the raw integer / `""` / envelope's value; never raises | T62 |
| unknown fields, record level and nested | ignored; record equals the one without them | T63 |

## 11. Test tooling and layout

- **Style:** plain pytest functions + `parametrize`, full type hints (`-> None`); no `unittest`
  classes here. `protobuf/tests/` keeps its own `unittest` style.
- **Rule:** every test in `tests/serialization/` exercises public names of `src.serialization`
  (`test_environment.py` excepted). `mutations_pb2` may be touched for exactly two things:
  parsing serializer output to assert what the deserializer cannot show (arm used, raw field
  values), and crafting adversarial deserializer input. Nothing `protobuf/tests/` already proves
  (field numbers, oneof exclusion, protobuf's own round trip, unknown-field preservation, thread
  safety, compactness) is re-asserted.
- **Backend:** upb only; `test_environment.py` asserts `api_implementation.Type() == "upb"` and
  runtime `>= 7.34, < 8` (internal API, tests only).
- **Dependencies:** `pyproject.toml` `dependencies = ["protobuf>=7.34,<8"]`.
  `requirements-dev.txt` adds exact pins `protobuf==7.36.2` (comment: must satisfy the pyproject
  range), `pytest==9.1.1`, `hypothesis==6.168.0`.
- **Config:**

```toml
[tool.ruff]
src = [".", "src", "tests", "protobuf/gen"]

[tool.pytest.ini_options]
minversion = "9.0"
testpaths = ["tests", "protobuf/tests"]
pythonpath = [".", "protobuf/gen"]
markers = ["slow: builds a ~10 MB version group; deselect with -m 'not slow'"]
addopts = [
  "-ra",
  "--strict-markers",
  "--strict-config",
  # Manual smoke script: imports fdb and needs a live cluster.
  "--ignore=tests/test_cdc_e2e.py",
]
```

- **Layout:**

```
tests/
├── __init__.py
├── conftest.py                 # Hypothesis profiles only
└── serialization/
    ├── __init__.py
    ├── doubles.py              # NativeMutation, VersionGroup, AttrOnlyMutation
    ├── strategies.py           # Hypothesis strategies shared by P1–P4
    ├── test_environment.py     # upb backend, runtime version range
    ├── test_serialize.py       # native → bytes, type matrix, serializer-side errors
    ├── test_version_group.py   # serialize_version_group: slicing, budget, its errors
    ├── test_deserialize.py     # bytes → values, adversarial input
    ├── test_roundtrip.py       # example round trips, feed-back, large inputs, P1–P4
    └── test_golden_bytes.py    # four change detectors
```

- **Doubles** (never imported by product code; tests import the module qualified,
  `doubles.NativeMutation`, because the name shadows the Protocol): `NativeMutation(NamedTuple)`
  (`type`, `param1`, `param2`), `VersionGroup(NamedTuple)` (`version`, `mutations`), and
  `AttrOnlyMutation`, a frozen slotted dataclass with the same three attributes that is not a
  tuple. `fdb` is never imported by tests.
- **Hypothesis** (dev-only) sweeps; it is never the only cover of a named case. A property failure
  is fixed by adding the shrunk case as an explicit parametrized test. Profiles in
  `tests/conftest.py`: `dev` (`deadline=None`) and `ci` (`deadline=None`, `derandomize=True`,
  `max_examples=200`, `print_blob=True`), selected by `HYPOTHESIS_PROFILE`, default `dev`.
  Strategies stay small: keys `st.binary(max_size=64)`, values `max_size=256`, groups ≤ 20.
- **CI / make:** `.github/workflows/tests.yml` (`name: Tests`), shaped like `lint.yml`
  (`pull_request` + `workflow_dispatch`, `contents: read`, concurrency-cancel, Python 3.12, pip
  cache keyed on `requirements-dev.txt`), runs `pip install -r requirements-dev.txt` then
  `HYPOTHESIS_PROFILE=ci pytest`. `make test` runs `$(VENV)/bin/pytest`. `.hypothesis/` and
  `.pytest_cache/` are gitignored. `docs/conventions.md` lists `make test` under Local setup and
  `Tests` as a required PR check.
- **Budget:** whole suite < 10 s locally without the `slow` tests. One custom marker, `slow`
  (T66, T67); `slow` tests run by default, in CI and in `make test`.

## 12. Ordered test list

One behaviour per entry, through the public API, ordered so each forces the next slice of
implementation. "Pins" marks a test expected to pass on arrival: it is still written, because it
fixes a decision the implementation could otherwise drift from.

Shared fixtures:

```python
STREAM = "orders"
TS = 1_700_000_000_123_456_789
V = 2**40 + 7
ENV = {"stream_name": STREAM, "bridge_timestamp_ns": TS}
SET = doubles.NativeMutation(0, b"k", b"v")
ADD = doubles.NativeMutation(2, b"\x15\x01k\x00", b"\xff\x00v")
CLEAR = doubles.NativeMutation(1, b"a", b"b\xff")
```

Round-trip assertions always build the expected `Record` from the very arguments passed in and
compare with `==`.

### Phase 0 — tooling

| # | Behaviour | Forces |
|---|---|---|
| E1 | `api_implementation.Type() == "upb"` | test tooling |
| E2 | runtime version `>= 7.34, < 8` | pinned runtime installed |

### Phase 1 — tracer bullet and the single mutation (`test_roundtrip.py`, `test_serialize.py`)

| # | Behaviour | Forces |
|---|---|---|
| **T01** | **Tracer.** `deserialize_record(serialize_mutation(SET, fdb_version=V, sequence_no=0, **ENV)) == Record(STREAM, TS, Mutation(0, b"k", b"v", VersionIndex(V, 0)))` | package, generated-code import, `NativeMutation`, `serialize_mutation`, `deserialize_record`, `Record` / `Mutation` / `VersionIndex` |
| T02 | T01's bytes parsed with `mutations_pb2`: `WhichOneof("record") == "mutation"`, `WhichOneof("mutation") == "single_key_mutation"`, `key` / `value` / `mutation_type` / `fdb_version` / `sequence_no` / `stream_name` / timestamp `(seconds, nanos)` as given | the real `FDBMutationRecord` encoding (T01 alone admits any symmetric codec) |
| T03 | `CLEAR` → `clear_range` arm with `begin_key` / `end_key`; round-trips to `Mutation(1, b"a", b"b\xff", …)` | arm routing on code 1; synthesised `type = 1` on read |
| T04 | parametrized over the 15 non-clear declared codes: `single_key_mutation`, `mutation_type == code`, round trip equal. 6, 13, 14, 15 come back as 6, 13, 14, 15 | generic type-code forwarding; no rewrite |
| T05 | undeclared 3, 4, 5, 10, 11, 21, 22, 23, 24, 255 round-trip with the same int; `type(out.type) is int` | pins: no raising enum lookup anywhere |
| T06 | version indexes `(0, 0)`, `(1, 5)`, `(2**63 - 1, 2**32 - 1)` round-trip; `sequence_no = 5` needs no predecessor | version index always constructed (presence of `(0, 0)`) |
| T07 | opaque params round-trip unchanged: `SET_VALUE` with `b""` value, with `b""` key, with a 10,001 B key, a 100,001 B value, key `b"\xff\xff/x"`; `ADD` with 0-, 1-, 3-, 9-byte operands; `COMPARE_AND_CLEAR` with `b""` operand | pins: no limits, no operand rules |
| T08 | clear-range pairs `(b"a", b"a")`, `(b"b", b"a")`, `(b"", b"z")`, `(b"a", b"\xff")`, `(k, k + b"\x00")` round-trip unchanged | pins: no key inspection |
| T09 | `AttrOnlyMutation`, an object with extra attributes, and an `IntEnum` type each produce the same `Record` as the plain double; `type(out.type) is int` | attribute-only reads; `int(t)` normalisation |
| T10 | `stream_name` `"orders"`, `"   "`, `"a\x00b"`, `"übër/注文"`, a 10 kB name round-trip exactly | pins: byte-exact forwarding |
| T11 | `bridge_timestamp_ns` `0`, `1`, `TS`, `MAX_BRIDGE_TIMESTAMP_NS` round-trip exactly; `0` comes back `0`, not `None` | exact ns conversion; timestamp presence via constructor kwarg |
| T12 | two calls with equal arguments return equal bytes | pins: purity |
| T13 | serializing 6, 13, 14, 15 leaves `recwarn` and `caplog` empty | pins: silence |

### Phase 2 — batch

| # | Behaviour | Forces |
|---|---|---|
| T14 | `serialize_batch([SET, ADD, CLEAR], fdb_version=V, **ENV)` → `batch` arm; `MutationBatch` of 3 with version indexes `(V, 0), (V, 1), (V, 2)` in input order | `serialize_batch`, `MutationBatch`, position assignment |
| T15 | `first_sequence_no=7` → `(V, 7), (V, 8), (V, 9)` | offset |
| T16 | one-element batch: `batch` arm; record `!=` the `serialize_mutation` record of the same mutation and version index; `batch.mutations[0] == mutation_record.body` | pins: body follows the function called |
| T17 | a generator gives the same bytes as the equivalent tuple | single-pass consumption |
| T18 | a batch mixing declared, undeclared and clear type codes round-trips element-wise equal to each mutation serialized alone | pins: no batch-level type rules |

### Phase 3 — version end

| # | Behaviour | Forces |
|---|---|---|
| T19 | `serialize_version_end(fdb_version=V, total_mutations=n, **ENV)` round-trips to `Record(STREAM, TS, VersionEnd(V, n))` for `n` in `0`, `1`, `2**32 - 1` | `serialize_version_end`, `VersionEnd` |
| T20 | parsed with `mutations_pb2`: `version_end` arm; both timestamp fields present and equal | inner timestamp written |

### Phase 4 — version group

| # | Behaviour | Forces |
|---|---|---|
| T21 | group fits the budget → exactly `[batch, version_end]`; indexes `(V, 0..n-1)`; `total_mutations == n`; both records carry `TS` | `serialize_version_group` |
| T22 | empty group → exactly one record, `VersionEnd(V, 0)` | empty-group branch |
| T23 | small budget → several batch records; every `len(r) <= max_record_bytes`; deserialized slices concatenate to the input in order; each slice's first position equals the running count; `records[-1]` is the version end with the full count | greedy slicing |
| T24 | each batch record `== serialize_batch(ms[a:b], fdb_version=V, first_sequence_no=a, **ENV)`; last `== serialize_version_end(fdb_version=V, total_mutations=n, **ENV)` | exact size accounting |
| T25 | for each adjacent pair of slices, `serialize_batch` of slice k plus the first mutation of slice k+1 is `> max_record_bytes` | maximal packing |
| T26 | budget `== len(serialize_batch(ms, …))` → one batch record; one byte less → two | boundary is `<=` |
| T27 | two calls give equal lists; a generator gives the same list as a tuple | pins: determinism, single pass |

### Phase 5 — output values and feed-back

| # | Behaviour | Forces |
|---|---|---|
| T28 | feed-back, one per record body: re-serializing the deserialized output with its own attributes as keywords reproduces the original bytes (`Mutation` passed positionally to `serialize_mutation`; `batch.mutations` to `serialize_batch` with `fdb_version` / `first_sequence_no` from `mutations[0].version_index`; `VersionEnd` to `serialize_version_end`) | pins: `Mutation` satisfies `NativeMutation`; keyword = attribute names |
| T29 | values are hashable, `==` field-wise, attribute assignment raises `FrozenInstanceError`; `Mutation(0, b"k", b"v", …) != (0, b"k", b"v")` | frozen slotted dataclasses |
| T30 | `out.version_index == (V, 3)`; `VersionIndex(V, 1) < VersionIndex(V + 1, 0)` | `NamedTuple` |
| T31 | drift guard: `{m.value for m in MutationType}` equals the proto enum's value set and the literal 16 codes of [deep-dive §2.2](CDC-DEEP-DIVE.md#22-value-types-); names match modulo `MUTATION_TYPE_`; `DECLARED_TYPE_CODES == frozenset(MutationType)` | `MutationType` |
| T32 | `mutation_type_name(0) == "SET_VALUE"`, `(20) == "COMPARE_AND_CLEAR"`, `(3) == "UNDECLARED_3"`, `(255) == "UNDECLARED_255"`; the five `MAX_*` constants have the §4.1 values | helper, constants |

### Phase 6 — golden bytes (`test_golden_bytes.py`)

Change detectors, not contract. Module docstring: *these assert byte stability upstream does not
guarantee; a failure after a protobuf/gencode upgrade means review the diff, then re-bless; a
failure at any other time is a real regression.* The literals are the bytes of the equivalent
hand-built `mutations_pb2` messages. No golden for `serialize_version_group` (covered by T24).

| # | Call | Expected hex |
|---|---|---|
| G1 | `serialize_mutation(ADD, fdb_version=V, sequence_no=3, **ENV)` | `0a066f7264657273120b0880e2cfaa0610959aef3a1a1a0a09088780808080201003120d0a0415016b001203ff00761802` |
| G2 | `serialize_mutation(CLEAR, fdb_version=V, sequence_no=4, **ENV)` | `0a066f7264657273120b0880e2cfaa0610959aef3a1a140a090887808080802010041a070a0161120262ff` |
| G3 | `serialize_version_end(fdb_version=V, total_mutations=5, **ENV)` | `0a066f7264657273120b0880e2cfaa0610959aef3a22160887808080802010051a0b0880e2cfaa0610959aef3a` |
| G4 | `serialize_batch([ADD, CLEAR], fdb_version=V, first_sequence_no=3, **ENV)` | `0a066f7264657273120b0880e2cfaa0610959aef3a2a320a1a0a09088780808080201003120d0a0415016b001203ff007618020a140a090887808080802010041a070a0161120262ff` |

### Phase 7 — serializer rejections (`test_serialize.py`)

Each asserts the class, `field`, and `index` where stated.

| # | Behaviour | Forces |
|---|---|---|
| T33 | hierarchy: `InputTypeError` is a `SerializationError` and a `TypeError`; `InputValueError` and `RecordDecodeError` are `SerializationError` and `ValueError`; `RecordTooLargeError` is an `InputValueError`; `RecordDecodeError` is not a protobuf `DecodeError` | `errors.py` |
| T34 | type code `-1`, `256`, `2**31`, `2**64` → `InputValueError`; `True`, `False`, `1.0`, `"1"`, `None` → `InputTypeError`; `field == "type"`. Same split for `mutation_type_name` with `field == "code"` | own type-code check |
| T35 | `param1` / `param2` × `None`, `bytearray`, `memoryview`, `"k"`, `5` → `InputTypeError` with the right `field` | `bytes`-only check before protobuf |
| T36 | `(0, b"k", b"v")`, `None`, an object missing `param2` → `InputTypeError`; `__cause__` is an `AttributeError`; `field` is `"type"`, `"type"`, `"param2"`. Both `serialize_mutation` and `serialize_batch` | wrapped attribute reads |
| T37 | `fdb_version` `-1`, `2**63`, `2**64` → `InputValueError`; `True`, `1.0`, `"1"`, `None`, an `IntEnum` member → `InputTypeError`; `field == "fdb_version"`. Parametrized over all four serialize functions | shared version check |
| T38 | `sequence_no` and `first_sequence_no`: `-1`, `2**32` → value; `True`, `None` → type | position check |
| T39 | `serialize_batch([], …)` and `(iter(()), …)` → `InputValueError`, `field == "mutations"`; `serialize_batch(None, …)` → `InputTypeError`, `field == "mutations"`; `serialize_batch(b"abc", …)` → `InputTypeError`, `field == "type"`, `index == 0`. Non-iterable also for `serialize_version_group` | iteration guards |
| T40 | a bad mutation at batch position 1 → `index == 1`; nothing returned | position reporting; all-or-nothing |
| T41 | `first_sequence_no = 2**32 - 2` with three mutations → `InputValueError`, `field == "sequence_no"`, `index == 2` | overflow check on assigned positions |
| T42 | invalid `fdb_version` plus a generator → raises and the generator is untouched (`next(gen)` still yields the first mutation) | scalars before iteration |
| T43 | `stream_name` `""`, `"\ud800"` → `InputValueError`; `b"orders"`, `bytearray(b"orders")`, `None` → `InputTypeError` | own `str` + strict-encode check |
| T44 | `bridge_timestamp_ns` `-1`, `MAX_BRIDGE_TIMESTAMP_NS + 1` → value; `True`, `1.5`, aware and naive `datetime`, `Timestamp()`, `None` → type | timestamp check |
| T45 | `total_mutations` `-1`, `2**32` → value; `True`, `None` → type | count check |
| T46 | `max_record_bytes` `0`, `-1` → `InputValueError` (not `RecordTooLargeError`); `True`, `1.5`, `None` → type | budget check |
| T47 | group `[small, big, small]` with the budget below `big`'s one-element record → `RecordTooLargeError`, `field == "max_record_bytes"`, `index == 1`, `record_bytes == len(serialize_batch([big], fdb_version=V, first_sequence_no=1, **ENV))`, `record_bytes > max_record_bytes`. Empty group with `max_record_bytes=1` → `index is None`, `record_bytes == len(serialize_version_end(fdb_version=V, total_mutations=0, **ENV))` | unsplittable cases |
| T48 | an invalid mutation at position 90 of 100 in `serialize_version_group` → raises, nothing returned | validation before output |
| T49 | a bad `type` alongside a 32-byte random key: neither the key's raw bytes nor its `repr` appear in `str(exc)` | message hygiene |

### Phase 8 — deserializer, adversarial input (`test_deserialize.py`)

Inputs hand-built with `mutations_pb2`; each asserts `reason` and `index`.

| # | Behaviour | Forces |
|---|---|---|
| T50 | `deserialize_record` of `None`, `"x"`, `bytearray(b"")`, `memoryview(b"")` → `InputTypeError`, `field == "data"` | input check |
| T51 | `b""`; `FDBVersionIndex(fdb_version=5, sequence_no=1)` bytes; a record with only envelope fields; envelope + `b"\x7a\x00"` (unknown length-delimited field 15, a "future arm") → `NO_RECORD_ARM` | `WhichOneof("record")` check |
| T52 | `b"\x0f\x01\x02\x03"` (invalid wire type); `b"\x0a\x02\xff\xfe"` (invalid UTF-8 stream name) → `MALFORMED_WIRE`, `__cause__` not `None` | wrapped parse |
| T53 | `mutation` arm without `version_index` → `MISSING_VERSION_INDEX`; without an inner arm → `NO_MUTATION_ARM`; `index is None` | presence checks |
| T54 | the same two faults at batch position 1 → `index == 1` | per-element checks with position |
| T55 | `mutation_type` `300` and `-1` → `TYPE_CODE_OUT_OF_RANGE` | 0..255 bound on read |
| T56 | `fdb_version = 2**63` in a version index and in a version end → `VERSION_OUT_OF_RANGE` | version bound on read |
| T57 | `batch` arm with zero mutations → `EMPTY_BATCH` | emptiness check |
| T58 | batches with two versions; positions `0, 2`; `1, 0`; `0, 0` → `INCONSISTENT_BATCH`, `index == 1` | consistency check |
| T59 | positions `5, 6, 7` at one version → accepted | pins: a slice may start anywhere |
| T60 | all-defaults arms: empty `single_key_mutation` with empty `version_index` → `Record("", None, Mutation(0, b"", b"", VersionIndex(0, 0)))`; empty `clear_range` → type `1`; empty `version_end` → `VersionEnd(0, 0)` | pins: presence, not content, is checked |
| T61 | `single_key_mutation` with `mutation_type = 1` deserializes equal to the canonical record's `Mutation`; re-serializing it yields the `clear_range` arm | pins: normalisation |
| T62 | tolerant envelope, none raise: no `bridge_timestamp` → `None`; version end whose inner timestamp differs → the envelope's value; `stream_name` unset → `""`; `Timestamp(seconds=253_402_300_800)` → `253_402_300_800_000_000_000`; `Timestamp(seconds=-5)` → `-5_000_000_000` | direct `seconds * 10**9 + nanos`, no helper |
| T63 | serializer output + `b"\x98\x06\x01"` → equal record; a record whose `mutation` submessage had `MergeFromString(b"\x98\x06\x01")` applied → equal record | pins: unknown fields ignored |
| T64 | every proper prefix of G1–G4 → `RecordDecodeError` (any reason) | pins: no partial parse escapes |

### Phase 9 — large inputs (`test_roundtrip.py`, always on; T66 and T67 marked `slow`)

Group fixture: `ms = [doubles.NativeMutation(0, i.to_bytes(16, "big"), bytes(72)) for i in
range(100_000)]` (≈ 109 B per mutation on the wire, ≈ 10.9 MB in all). Compare via plain `bool`s,
never a pytest-diffed 100 k-element assert.

| # | Behaviour | Forces |
|---|---|---|
| T65 | a 100,000 B value round-trips through `serialize_mutation`, and fits one batch record under `max_record_bytes=1_000_000` via `serialize_version_group` | — |
| T66 | `serialize_version_group(ms, fdb_version=V, max_record_bytes=1_000_000, **ENV)` → ≥ 11 batch records, all within budget, total `>= 10_000_000` bytes; deserialized slices concatenate to the input; first / last version indexes `(V, 0)` / `(V, 99_999)`; version end counts 100,000 | slicing at scale |
| T67 | `serialize_batch(ms, fdb_version=V, **ENV)` (one ≈ 10 MB record) deserializes to a `tuple` of 100,000 with correct first / last version index | pins: the primitives have no size cap; full materialisation |

### Phase 10 — properties (`test_roundtrip.py`, `test_deserialize.py`)

| # | Property |
|---|---|
| P1 | any type code 0..255, any `bytes` params, any in-range version index, any accepted envelope → `deserialize_record(serialize_mutation(…))` equals the expected `Record` |
| P2 | any group (≤ 20) through `serialize_version_group` with a budget drawn from `max(largest one-element batch record, the version-end record)` upward: order, count and version indexes preserved; last record is the version end with the count; every record within budget |
| P3 | `deserialize_record(st.binary(max_size=256))` returns a `Record` or raises `RecordDecodeError` — nothing else escapes |
| P4 | any `int` outside 0..255 as type code → `InputValueError`; never emitted |

## 13. Non-goals

Documented, deliberately untested or unsupported:

- Verifying that `mutations` is an unfiltered, contiguous, native-order run (or the whole group);
  caller-supplied `total_mutations` agreeing with what was emitted; `fdb_version` or timestamp
  monotonicity across calls.
- FDB key/value limits; clear-range ordering; operand-width or any semantic validation; warning on
  legacy, dead or undeclared type codes; detecting an absent `mutation_type` field.
- A size cap on the primitives; protobuf's 2 GiB `EncodeError`; splitting a single mutation; a
  default or Kafka-side allowance for `max_record_bytes`; a per-mutation-layout group helper.
- Cross-record checks in the deserializer; detecting foreign proto messages that populate a record
  arm; duplicate-arm detection; distinguishing a newer-schema arm from garbage; any verification of
  `VersionEnd.bridge_timestamp`.
- bytes → values → bytes identity for foreign or non-canonical input; preserving or reporting
  unknown fields; a raw-proto return or escape hatch (readers needing one call
  `mutations_pb2.FDBMutationRecord.FromString` themselves); streaming / lazy read; a
  `serialize_record(Record)` inverse; an `UnknownBody` variant; validating hand-constructed output
  values; exposing which clear encoding or inner timestamp was on the wire.
- Accepting `CdcVersionedMutations` / `CdcConsumeResult` objects, bare tuples, a serializer object,
  an injected clock, `stream_id` (until the schema has the field), `bytes` stream names, `datetime`
  timestamps.
- Order among single-value checks beyond §6.3 / §7; exact message text; the pure-python half of
  the parse `except`; pure-python or `cpp` backend runs; a protobuf version matrix; coverage
  gating; mypy / static Protocol or exhaustiveness checks; Python's own keyword-only enforcement;
  an import-inspection test for "no clock" (T11 + T12 cover it through the API); backward-compat
  tests or a second `.proto` (v1 is the first schema); benchmarks; anything needing `fdb`, Kafka
  or Docker.

## 14. Schema follow-ups

Proposals for `mutations.proto`, ordered by value. None is applied; the module works around
each.

1. **Add `uint64 stream_id = 6` to `FDBMutationRecord`.** Reader identity is
   `(stream_id, version, sequence_no)` and a re-registered name is a different stream
   (deep-dive §1.5, §5.1); a reader cannot tell them apart. Additive, wire-compatible; the
   serializer would take it as a keyword-only `stream_id`.
2. **`FDBMutationBatch` permits states the contract forbids and repeats the version per
   mutation.** Minimum: replace "Monotonically ordered mutations within this batch" with the
   invariants (never empty, one `fdb_version`, `sequence_no` ascending and contiguous). Preferred,
   additive: hoist `uint64 fdb_version = 2` and `uint32 first_sequence_no = 3` onto the batch —
   saves ~9 B per mutation and makes mixed-version batches unrepresentable.
3. **Duplicate clear encoding.** `MUTATION_TYPE_CLEAR_RANGE = 1` inside `FDBSingleKeyMutation`
   duplicates the `clear_range` arm. Minimum: a comment that code 1 MUST use the arm and readers
   SHOULD treat the other form as equivalent. Preferred: `reserved 1` /
   `reserved "MUTATION_TYPE_CLEAR_RANGE"` (the open enum still parses old data).
4. **No presence on `mutation_type`.** `SET_VALUE = 0` is the proto3 default, so an absent field
   reads as a SET. An `UNSPECIFIED = 0` sentinel would break the 1:1 mapping to FDB codes; propose
   `optional MutationType mutation_type = 3` (same wire format, `HasField` works). Check
   `buf breaking` accepts the cardinality change.
5. **`VersionEnd.bridge_timestamp` duplicates the envelope's.** Delete it (`reserved 3`) or give it
   a distinct documented meaning. Until then both are written equal and the inner one is ignored
   on read.
6. **`VersionEnd` comments.** `total_mutations` says "Optional": reword to "always set by the
   bridge; 0 = no mutations at this version" (`optional uint32` only if the team wants a real
   "not reported" state; this spec does not). State that the version end is the last record of its
   version group and that the count spans every record of the group.
7. **`fdb_version` is `uint64`, native is `int64`.** Keep `uint64` (switching trips
   `buf breaking` `FIELD_SAME_TYPE` and would make `-1` encodable); comment both fields: "FDB
   commit version; always ≤ 2^63 − 1 (native `int64_t`); readers MAY reject larger."
8. **`record` oneof comments.** New arms are invisible to old readers (parsed as unknown fields →
   no arm set): readers must be upgraded before a new arm is produced — relevant to the snapshot
   arms. On `batch`: a group may arrive as several batch records, and `mutation` ≡ a one-element
   batch for readers.
9. **`stream_name`: keep `string`, document the constraint.** Native names are `bytes`; the bridge
   refuses non-UTF-8 names at start-up. With `stream_id` added the name is a label. The field
   comment ("Directory or CDC stream identifier") should stop calling it an identifier.
10. **`FDBSingleKeyMutation` naming.** It also carries undeclared type codes of unknown arity; a
    comment ("any non-clear type code; undeclared codes are forwarded raw") is enough.

Gencode: `protobuf/gen` is generated at 6.30.2, which protobuf 8 will not load. Bump the
`buf.gen.yaml` plugin and regenerate before raising the runtime ceiling.
