# Serialization module spec

Contract for `src/serialization/`: native FDB CDC mutations → Protobuf `FDBMutationRecord`
bytes, and back. Trello:
[Implement Binary Serializer & Deserializer Module](https://trello.com/c/amcMAYnn). Schema:
[`mutations.proto`](../protobuf/proto/fdbkafka/cdc/v1/mutations.proto), used as-is.

**In scope:** the serialize/deserialize functions, their input and output types, errors.
**Out of scope:** snapshot arms, Kafka key/partition/topic layout, Schema Registry framing,
producing, idle-watermark policy.

## 1. Rules

- The serializer forwards native mutations unchanged: no rewriting, merging or validation of
  operands, keys or type-code semantics
  ([deep-dive §5](CDC-DEEP-DIVE.md#5-mapping-onto-the-kafka-proposal)). With real CDC input and
  a valid stream name, no serializer error can fire.
- Pure: no state, clock, I/O or logging. Equal arguments give equal bytes.
- All-or-nothing: every call returns its full result or raises.
- The deserializer is strict on identity (record body, version index, type code, batch shape)
  and tolerant on metadata (`stream_name` and both bridge timestamps), which never fails a read.

## 2. Public API

Import from `src.serialization`; everything in its `__all__` is contract. Private helpers live
in `_`-modules. Runtime dependency: `protobuf>=7.34,<8` (the committed gencode is 6.30.2,
which protobuf 8 will not load); generated code is imported as
`from fdbkafka.cdc.v1 import mutations_pb2` (`protobuf/gen` on the path).

### 2.1 Input

`NativeMutation` is a `Protocol` with read-only `type: int`, `param1: bytes`, `param2: bytes`.
Upstream `CdcMutation` conforms. Attributes are read by name, once each; the object is never
indexed or unpacked, so a bare tuple is rejected.

`MutationType` is an `IntEnum` of the 16 declared type codes (mirrors upstream
`CdcMutationType`). It is a convenience for callers; neither path looks a type code up in it.

Ranges (inclusive; the limits are not exported):

| Value | Range |
|---|---|
| type code | 0..255 |
| `fdb_version` | 0..2**63-1 |
| `sequence_no`, `total_mutations` | 0..2**32-1 |
| `bridge_timestamp_ns` | 0..9999-12-31T23:59:59.999999999Z |

### 2.2 Serialize

All arguments after the first are keyword-only and required, except `first_sequence_no`
(default `0`). `bridge_timestamp_ns` is integer nanoseconds since the Unix epoch.

```python
serialize_mutation(mutation, *, fdb_version, sequence_no, stream_name, bridge_timestamp_ns) -> bytes
serialize_batch(mutations, *, fdb_version, first_sequence_no=0, stream_name, bridge_timestamp_ns) -> bytes
serialize_version_end(*, fdb_version, total_mutations, stream_name, bridge_timestamp_ns) -> bytes
serialize_version_group(mutations, *, fdb_version, max_record_bytes, stream_name, bridge_timestamp_ns) -> list[bytes]
```

- **`serialize_mutation`**: one mutation record at version index `(fdb_version, sequence_no)`.
- **`serialize_batch`**: one batch record; element `i` gets `(fdb_version, first_sequence_no + i)`.
  `mutations` is consumed once (generators are fine). Empty → `InputValueError`. A one-element
  batch stays a batch.
  *Caller precondition (unchecked):* `mutations` is a contiguous, unfiltered, native-order run of
  one version group and `first_sequence_no` is the native position of its first element.
  Filtering corrupts the dedup identity.
- **`serialize_version_end`**: one version-end record. `total_mutations` counts native mutations in
  the whole group, however it was sliced; `0` means none at this version (an empty group and an
  idle watermark are the same record).
- **`serialize_version_group`**: the whole group as batch records covering `(fdb_version, 0) …
  (fdb_version, n-1)` in order, then exactly one version end with `total_mutations == n`. An
  empty group returns only the version end. *Caller precondition:* `mutations` is the whole
  group.
  - `max_record_bytes` bounds `len()` of each returned value only; Kafka key, headers and
    framing are the caller's to subtract.
  - Slicing is greedy in native order; each batch record is byte-identical to
    `serialize_batch` of its slice.
  - A mutation is never split. If one mutation's one-element batch record, or the version-end
    record, exceeds the budget → `RecordTooLargeError`.
  - Returns a list, so an invalid mutation anywhere raises before any record is produced.

The primitives have no size cap. Sizing: FDB caps keys at 10,000 B and values at 100,000 B, so
a budget of 128 KiB plus the stream name's length never raises on real CDC data.

### 2.3 Deserialize

```python
deserialize_record(data: bytes) -> Record
```

Output values are frozen, slotted dataclasses (compared field-wise, hashable):

| Type | Fields |
|---|---|
| `Record` | `stream_name: str`, `bridge_timestamp_ns: int \| None`, `body: RecordBody` |
| `RecordBody` | `Mutation \| MutationBatch \| VersionEnd` |
| `Mutation` | `type: int`, `param1: bytes`, `param2: bytes`, `version_index: VersionIndex` |
| `MutationBatch` | `mutations: tuple[Mutation, ...]` |
| `VersionEnd` | `fdb_version: int`, `total_mutations: int`, `bridge_timestamp_ns: int \| None` |
| `VersionIndex` | `NamedTuple(fdb_version, sequence_no)`; tuple order is stream order |

Guarantees on every returned `Record`: `type` is a plain `int` in 0..255; versions are in
0..2**63-1; a batch is non-empty, single-version, with contiguous ascending `sequence_no`. So
every body can be re-serialized (`Mutation` satisfies `NativeMutation`).
Metadata carries no guarantee: `stream_name` may be `""`, and `Record.bridge_timestamp_ns` and
`VersionEnd.bridge_timestamp_ns` may each be `None` (absent) or outside the serializer's range.
They are read independently of each other.

## 3. Wire format

| Native / keyword | `FDBMutationRecord` |
|---|---|
| `type == 1` | `FDBMutation.clear_range` (`begin_key = param1`, `end_key = param2`) |
| any other `type` in 0..255, declared or not | `FDBMutation.single_key_mutation` (`key = param1`, `value = param2`, `mutation_type = type`, raw int) |
| `fdb_version`, sequence position | `FDBMutation.version_index`, always present (incl. `(0, 0)`) |
| `fdb_version`, `total_mutations` | `version_end` arm |
| `stream_name` | `stream_name` |
| `bridge_timestamp_ns` | `bridge_timestamp`, always present (incl. `0`); on a version end also `VersionEnd.bridge_timestamp`, same value |

Record body: `serialize_mutation` → `mutation` arm, `serialize_batch` / group slices → `batch`
arm, version end → `version_end` arm.

On read: `clear_range` yields `type = 1`. A `single_key_mutation` with `mutation_type == 1` (never
written) is accepted and yields the same `Mutation` as `clear_range`. `VersionEnd.bridge_timestamp`
surfaces as `VersionEnd.bridge_timestamp_ns`. Unknown fields are ignored. An absent
`mutation_type` reads as `0` (`SET_VALUE`) and cannot be detected.

## 4. Serializer input rules

| Input | `InputTypeError` | `InputValueError` |
|---|---|---|
| mutation | missing `type`, `param1` or `param2` (e.g. bare tuple, `None`); `__cause__` is the `AttributeError` | — |
| `mutations` | not iterable | empty (`serialize_batch` only) |
| `type` | not `int`, or `bool` | outside 0..255 |
| `param1`, `param2` | not `bytes` (incl. `bytearray`, `memoryview`, `None`) | — |
| `stream_name` | not `str` | `""`, or not UTF-8-encodable |
| other integer keywords | `type(x) is not int` (rejects `bool`, `IntEnum`, `float`) | outside its range in §2.1; `max_record_bytes < 1`; an assigned `sequence_no` past 2**32-1 |

- The type code alone accepts `IntEnum` (upstream `CdcMutationType`), normalized to plain `int`.
- Not checked: FDB key/value size limits, system keys, empty keys/values, clear-range order,
  operand widths, legacy/undeclared type codes. All are forwarded as-is.
- Keyword checks run before `mutations` is iterated, so a bad keyword leaves a generator
  unconsumed.

## 5. Errors

```
SerializationError(Exception)
├── InputTypeError(SerializationError, TypeError)      .field, .index
├── InputValueError(SerializationError, ValueError)    .field, .index
│   └── RecordTooLargeError                            .record_bytes, .max_record_bytes
└── RecordDecodeError(SerializationError, ValueError)  .reason: DecodeFailure, .index
```

- `field` is the parameter or attribute name as spelled in the public API (`"data"` for
  `deserialize_record`). `index` is the 0-based position in `mutations` or the wire batch;
  `None` outside a batch. `RecordTooLargeError.field` is `"max_record_bytes"`, with
  `index = None` when the version end is what does not fit.
- Serialize functions raise only `InputTypeError` / `InputValueError` (and
  `RecordTooLargeError` from `serialize_version_group`). `deserialize_record` raises only
  `InputTypeError` (non-`bytes` data) / `RecordDecodeError`. No protobuf exception escapes
  (protobuf's 2 GiB `EncodeError` is unreachable under CDC's 10 MB reply budget).
- Messages never contain key, value or stream-name content: `bytes` and `str` inputs are
  described by their Python type.

**Decode failures** (`RecordDecodeError.reason`, its `.value` is a metric label):

| `DecodeFailure` | Condition |
|---|---|
| `MALFORMED_WIRE` | protobuf cannot parse (corrupt, truncated, invalid UTF-8 `stream_name`); cause chained |
| `NO_RECORD_ARM` | no `record` arm set: `b""`, foreign bytes, or an arm from a newer schema |
| `NO_MUTATION_ARM` | mutation with no `mutation` arm set |
| `MISSING_VERSION_INDEX` | mutation without `version_index` |
| `VERSION_OUT_OF_RANGE` | `fdb_version > 2**63-1` (the wire field is `uint64`, a native version `int64`) |
| `TYPE_CODE_OUT_OF_RANGE` | `mutation_type` outside 0..255 |
| `EMPTY_BATCH` | `batch` arm with no mutations |
| `INCONSISTENT_BATCH` | mixed `fdb_version`, or `sequence_no[i] != sequence_no[0] + i`; `index` = first offender |

Arms that are present but all-default (e.g. empty `version_end` → `VersionEnd(0, 0, None)`) are
accepted: presence is checked, not content.

**Handling.** Input errors are programmer or configuration bugs: the bridge must fail, not skip
(dropping one mutation breaks its version group), and must not acknowledge. `RecordDecodeError`
is the only error a reader should route around (skip or dead-letter, and alarm).

## 6. Reader contract for one version group

The module guarantees this shape within one `serialize_version_group` call; it enforces nothing
across calls, and `deserialize_record` checks one record at a time.

- For version V, a partition carries mutation and/or batch records whose version indexes run
  `(V, 0) … (V, n-1)` gap-free and ascending, then `VersionEnd(V, n)` last.
- A mutation record and a one-element batch record of the same mutation mean the same thing.
  Readers must accept both. (They deserialize to unequal `Record`s with equal `Mutation`s.)
- `VersionEnd(V, n)`: every mutation at versions ≤ V has been emitted; `n` of them are at V.
  `n == 0` means "nothing at V, advance".
- Without redelivery, successive version ends strictly increase. A repeat, a regression, or
  `(V, 0)` reappearing before the version end means redelivery: discard the partial group
  ([deep-dive §5.1](CDC-DEEP-DIVE.md#51-kafka-record-key-partitioning-and-atomicity-)).
- Identity is the version index. Never dedup on bytes or on either bridge timestamp; a
  redelivered group gets new timestamps. The record value names its stream only by
  `stream_name`, so any stronger stream identity must travel outside it.
