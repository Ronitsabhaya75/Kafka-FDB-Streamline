# CDC Consumer Module Spec

Contract for `src/fdb_kafka_bridge/cdc/`: FoundationDB native CDC client initialization, stream
lifecycle, multi-range registration, Directory Layer resolution, bounded record polling, and
safe acknowledgement.

Trello: [Basic FDB Client & Mutation Stream Listener](https://trello.com/c/0Wn8zbEk) (CDC Engine).
Related architecture: [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md).
Schema: [`mutations.proto`](../protobuf/proto/fdbkafka/cdc/v1/mutations.proto).

## 1. Scope & Rules

- **FoundationDB API version:** Strictly gated on API version 800 (`fdb.api_version(800)`).
  Selecting an API version < 800 raises `CDCNotSupportedError`.
- **Subspace range alignment:** Stream registration covers `[rawPrefix, strinc(rawPrefix))`,
  ensuring bare prefix and escaped suffix keys are strictly included
  ([CDC deep dive §2.5](CDC-DEEP-DIVE.md#25-registering-a-directorys-range-)).
- **Directory streams:** a Directory Layer tree is one stream over the union of each
  directory's own prefix range (§2.5, §2.6), resolved once at startup. Later directory
  changes are not followed; see
  [`DIRECTORY-CDC-RECONCILIATION.md`](DIRECTORY-CDC-RECONCILIATION.md) for the
  unimplemented reconciliation design.
- **Ordering & Lifecycle:**
  `consume()` → durably process/checkpoint → `acknowledge()`.
  Acknowledgement is cumulative, takes no arguments, and advances the cluster retention
  watermark to `last_consumed_version + 1`. A listener rejects another consume until a
  non-empty `poll_records()` result is acknowledged. There is no automatic acknowledgement:
  the listener cannot know whether Kafka publication and checkpoint persistence have
  completed. A serialization failure permanently blocks acknowledgement on that listener;
  close it and resume from the last durable cursor.
- **Protobuf Fidelity:** Raw FDB mutations are mapped directly into Protobuf types
  (`FDBSingleKeyMutation`, `FDBClearRange`, `FDBMutation`, `FDBMutationBatch`, `FDBMutationRecord`).
  `poll_records()` uses `fdb_kafka_bridge.serialization.serialize_version_group()` to split each
  version group into records no larger than `max_record_bytes`, then emits its
  `VersionEnd`. A reply watermark beyond the final version group is represented by an
  additional zero-mutation `VersionEnd`, unless idle coalescing withholds it (below).
  Every returned record must be durably processed before ack.
- **Idle acknowledgement cadence:** an idle reply carries no version groups, only an
  advanced watermark. By default each one returns a single zero-mutation `VersionEnd` that
  must be acked before the next poll (one ack per ~5 s idle reply). Setting
  `idle_ack_max_replies` (N) and/or `idle_ack_max_seconds` (T) enables cumulative-ack
  coalescing ([CDC deep dive §1.3](CDC-DEEP-DIVE.md#13-acknowledgement-semantics-),
  [§2.7](CDC-DEEP-DIVE.md#27-threads-blocking-and-the-network-thread-)): an idle reply
  below threshold returns `[]` and needs no ack. Once N idle replies (this one included)
  accumulate since the last ack, or T seconds pass since the last ack (or start/resume),
  the reply flushes as one `VersionEnd` at the latest watermark, acked as usual; with both
  set, whichever is reached first flushes. T is soft: it is checked only when a reply
  arrives, since `consume()` long-polls, so a T at or below the native idle reply cadence
  flushes every idle reply; set it well above. Data replies are unchanged and their ack
  subsumes skipped idle progress; ack and start/resume reset both bounds. Skipped progress
  un-acked at close is re-consumed as an idle reply after resume. Downstream sees
  `VersionEnd` heartbeats less often. After a `[]`, `get_position()` is past the last ack:
  checkpoint a position only for a reply you then acknowledge, never after a `[]`.
  Idle coalescing is configured via `idle_ack_max_replies` and `idle_ack_max_seconds`
  on `FDBMutationListener`, and exposed daemon-wide via `BridgeConfig.cdc` and the bridge CLI.
- **Resume Safety:** Resume waits until a fresh database read version reaches the cursor's
  `last_consumed_version`, then re-acknowledges the resumed cursor before polling. This
  restores the native consumer's retention watermark after process restart.

## 2. Public API

Import from `fdb_kafka_bridge.cdc`; everything in its `__all__` is contract.

### 2.1 Client & Connection (`src/fdb_kafka_bridge/cdc/client.py`)

```python
init_fdb(api_version: int = 800) -> None
```
Initializes the FDB C-bindings API version idempotently. Raises `CDCNotSupportedError`
if `fdb` is not installed or `api_version < 800`.

```python
class FDBClient:
    def __init__(self, cluster_file: str | None = None, api_version: int = 800) -> None: ...
    def open(self) -> Any: ...
    def close(self) -> None: ...
    def register_cdc_stream(
        self, name: bytes | str, begin_key: bytes | None = None, end_key: bytes | None = None,
        *, ranges: Iterable[tuple[bytes, bytes]] | None = None,
    ) -> int: ...
    def ensure_cdc_stream(self, name: bytes | str, ranges: Iterable[tuple[bytes, bytes]]) -> int: ...
    def get_cdc_stream(self, name: bytes | str) -> Any | None: ...
    def create_cdc_consumer(self, name: bytes | str) -> Any: ...
    def resume_cdc_consumer(self, cursor: Any) -> Any: ...
    def remove_cdc_stream(self, name: bytes | str) -> None: ...
    def list_cdc_streams(self) -> list[Any]: ...
    def get_read_version(self) -> int: ...

multi_range_cdc_unsupported_reason() -> str | None
```

`register_cdc_stream(name, begin_key, end_key)` is unchanged. `ranges=` registers one
multi-range stream (§2.6); it cannot be combined with `begin_key`/`end_key`
(`TypeError`). `ensure_cdc_stream` is the safe multi-range entry point: it validates,
checks capability and compares an existing stream before its single native call.

### 2.2 Mutation Stream Listener (`src/fdb_kafka_bridge/cdc/listener.py`)

```python
class FDBMutationListener:
    def __init__(
        self,
        client: FDBClient,
        stream_name: str | bytes,
        *,
        key_range: tuple[bytes, bytes] | None = None,
        key_ranges: Iterable[tuple[bytes, bytes]] | None = None,
        subspace: Any | None = None,
        cursor: Any | None = None,
        max_record_bytes: int | None = None,
        resume_timeout_seconds: float = 10.0,
        resume_poll_interval_seconds: float = 0.05,
        idle_ack_max_replies: int | None = None,
        idle_ack_max_seconds: float | None = None,
    ) -> None: ...

    def start(self) -> None: ...
    def close(self) -> None: ...
    def poll_records(self, bridge_timestamp_ns: int | None = None) -> list[mutations_pb2.FDBMutationRecord]: ...
    def acknowledge(self) -> None: ...
    def get_position(self) -> Any: ...
    def flush_idle_progress(self, bridge_timestamp_ns: int | None = None) -> list[mutations_pb2.FDBMutationRecord]: ...

    key_ranges: tuple[tuple[bytes, bytes], ...] | None  # validated, sorted
    stream_id: int | None            # set by start() for key_ranges
    awaiting_ack: bool               # a delivered reply needs acknowledge()
    delivery_failed: bool            # last reply failed to serialize; resume to clear
    has_pending_idle_progress: bool  # withheld idle progress awaits flush
```

Supply at most one of `key_range`, `key_ranges` or `subspace` to register a stream when
the listener starts; supplying more than one raises `ValueError`. Omit all three to
consume a stream registered elsewhere. `key_ranges` is validated in full in the
constructor (§2.6), before any native call; `start()` registers it through
`ensure_cdc_stream` and, when resuming, refuses (`CDCStreamMismatchError`) a cursor whose
`stream_id` is not the registered stream's, before `resume_cdc_consumer` or the restoring
re-acknowledgement. `flush_idle_progress()` turns withheld idle progress into one
`VersionEnd` to deliver and acknowledge (used by graceful shutdown); it returns `[]`
when nothing is pending.
Leave both `idle_ack_*` bounds `None` for per-reply idle acks; set either to coalesce
idle replies (§1). N must be an `int` >= 1 and T a finite `int` or `float` > 0 (`bool`
rejected); wrong types raise `TypeError`, bad values `ValueError`.

`max_record_bytes` bounds each record **value** only. `None` (the default) derives it from
the stream name's UTF-8 bytes, which the bridge uses as the Kafka key:
`max_record_bytes_for_key(key)` = librdkafka's default `message.max.bytes` (1,000,000)
minus the key minus the 36-byte per-message overhead, so a full-size record fits the
producer's limit. The helper lives in `fdb_kafka_bridge.record_size` (re-exported by
`fdb_kafka_bridge.kafka`), so the CDC layer never imports the Kafka client. A caller that
produces under a different key (e.g. `BridgePipeline(partition_key=...)`) must pass
`max_record_bytes=max_record_bytes_for_key(key)` itself.

The stream name also sits in every record's envelope, so it counts twice. Whether derived
or explicit, the budget must hold the stream's largest possible `VersionEnd`
(`min_record_cap(stream_name)`: the name plus about 50 bytes); otherwise every watermark
would fail identically on each replay, so the constructor raises `ValueError`. With the
derived default this caps the stream name at roughly 500 KB. An explicit value is
otherwise used as given; the caller owns the full budget, see
[Record size](KAFKA-PRODUCER-SPEC.md#record-size).

### 2.3 Prefix Utilities (`src/fdb_kafka_bridge/cdc/listener.py`)

- `strinc(key: bytes) -> bytes`: Computes the lexicographically adjacent prefix upper bound.
- `subspace_to_key_range(subspace: Any) -> tuple[bytes, bytes]`: Extracts `(prefix, strinc(prefix))`.

### 2.4 Errors (`src/fdb_kafka_bridge/cdc/errors.py`)

- `CDCError`: Base class for CDC exceptions.
- `CDCNotSupportedError`: FDB bindings missing, API version < 800, or native CDC
  unavailable in the loaded binding.
- `CDCConsumerClosedError`: Operation attempted on closed consumer handle.
- `CDCInvalidCursorError`: Invalid resume cursor coordinates.
- `CDCInvalidRangeError`: Invalid key range or range collection supplied.
- `CDCStreamMismatchError` (also a `ValueError`): an existing stream covers a different
  canonical range union, or a cursor names a different stream id. Never repaired
  automatically.
- `DirectoryResolutionError` (also a `ValueError`): a directory tree could not be
  resolved. Subclasses `DirectoryNotFoundError` (missing root) and
  `DirectoryPartitionError` (a partition in the tree).
- `CDCConsumeError`: Base for a failed native `consume`/`acknowledge`. Carries the
  native FoundationDB `code` on the surface, not behind `__cause__`.
  - `CDCRetryableError`: `server_overloaded` (1211). Retry with bounded backoff;
    if it repeats at one commit version it is the L1 poison pill — escalate, do
    not spin.
  - `CDCTerminalError`: `transaction_too_old` (1007) or `client_invalid_operation`
    (2000). Do not retry; 1007 means rebuild, and the narrow ≤5 s retry-once case
    for 2000 is the caller's to implement from `code`.

Only these three CDC error codes are classified ([CDC deep dive §3](CDC-DEEP-DIVE.md#errors-we-have-to-map-),
whose standard `is_retryable` predicate is wrong about two of them). Any other native
`FDBError` propagates unwrapped so callers keep normal FDB retry and diagnostics. A
non-native consume/ack failure is wrapped as `CDCError` with the original as its cause.

### 2.5 Directory Layer resolver (`src/fdb_kafka_bridge/cdc/directory.py`)

```python
resolver = DirectoryCDCResolver(db, ("my_application",), recursive=True)
resolved = resolver.resolve()            # ResolvedDirectoryTree
listener = FDBMutationListener(client, "my-application-cdc", key_ranges=resolved.key_ranges)
resolved.path_for_key(raw_key)           # ("my_application", "users") or None
resolved.fingerprint                     # "v1:sha256:<64 hex>"
```

`DirectoryCDCResolver(db, root_path, *, recursive=False, directory_layer=None,
max_attempts=5)` reads an **existing** tree and needs no checkpoint store; the durable-checkpoint
requirement applies only to the `Bridge` daemon (BRIDGE-PIPELINE-SPEC "Directory mode"); `resolve()` returns a frozen
`ResolvedDirectoryTree` (`root_path`, `recursive`, `ranges`, `key_ranges`, `paths`,
`fingerprint`, `lookup`, `path_for_key`). Each `DirectoryRange` carries the full
logical `path` and raw `prefix`, and covers `[prefix, strinc(prefix))`.

- **Paths.** A non-empty `tuple` of `str`, each UTF-8-encodable; `""` is a valid
  component (the Directory Layer accepts it). A plain string, a list, `bytes`
  components and the empty tuple are rejected (`TypeError`/`ValueError`). The empty
  tuple names the Directory Layer root, whose contents are the whole key space,
  including directory metadata. An existing directory with no keys and no children is
  valid and resolves to its own prefix range; a missing root raises
  `DirectoryNotFoundError`. Nothing is ever created.
- **Discovery.** Non-recursive mode resolves only the root. Recursive mode adds every
  existing descendant via the Directory Layer's `exists`/`open`/`list`, iteratively, so
  depth is bounded by the 1,024-range limit rather than the Python stack. Child
  prefixes are independent of the parent's and are never assumed contiguous.
- **Snapshot.** All reads use one transaction, so the tree is read at one read
  version; the transaction is never committed. A retryable FDB error, including
  `transaction_too_old` when a large tree outlasts the transaction time limit, restarts
  discovery via `on_error`, up to `max_attempts`, then raises
  `DirectoryResolutionError`. A partial tree is never returned. Discovery runs once at
  startup; it is not continuous.
- **Partitions** are rejected (`DirectoryPartitionError`), as root or as a descendant
  in recursive mode, before any registration. A partition's prefix range also holds
  its own node metadata (`prefix + b"\xfe"`), so registering it would capture keys
  that belong to no directory. A partition below a non-recursive root is not read.
- **Ranges.** Prefixes must be non-empty bytes below `b"\xff"`. Ranges are sorted by
  raw bytes with their paths attached, and duplicate or overlapping ranges are
  rejected rather than merged; adjacent ranges are accepted. The result is never
  collapsed into one enclosing interval.
- **Lookup.** `lookup(key)` returns the one `DirectoryRange` whose own range holds
  `key`, or `None`. Ranges are disjoint, so there is at most one match, and it is the
  most specific directory: a key under a child's prefix is never inside the parent's.
  Boundaries are half-open. A `CLEAR_RANGE` can span several directories (native CDC
  clips clears to each registered range, not to directories), so its begin key alone
  does not identify one directory. Emitted record payloads are unchanged: lookup is a
  consumer-side helper, not a new wire field.
- **Fingerprint** (`directory_fingerprint`): `"v1:sha256:"` plus the SHA-256 of UTF-8
  JSON with sorted keys, no whitespace and ASCII escaping:
  `{"format":1,"kind":"fdb-kafka-bridge/directory-cdc","ranges":[{"begin":<hex>,"end":<hex>,"path":[...]}...],"recursive":<bool>,"root_path":[...]}`,
  ranges ordered by raw begin bytes. It changes when the root path, recursive mode, any
  resolved path, or any raw boundary changes, and never depends on discovery order.

### 2.6 Multi-range registration (`src/fdb_kafka_bridge/cdc/ranges.py`)

Verified contract, FoundationDB **8.0.0** (the `Dockerfile` `FDB_VERSION` and the
`foundationdb` pin): `fdb_database_register_cdc_stream(db, name, name_length,
FDBKeyRange const* ranges, int range_count)` registers one stream over the union of
1 to 1,024 non-empty half-open ranges in normal user key space. The native client merges
overlapping, duplicate and adjacent inputs, and `list_cdc_streams()` reports the
canonical union as `CdcStreamInfo.ranges`. A stream's range set is immutable:
re-registering the same name and equivalent union is idempotent, and a different union
is rejected. All ranges share one cursor and one acknowledgement position. The Python
binding exposes this as `Database.register_cdc_stream(name, begin_key=None,
end_key=None, *, ranges=None, split_points=None)`. Sources: `api-c.rst` "CDC",
`design/cdc.md` "Registration and removal semantics", `fdb/impl.py` at tag `8.0.0`.

- `validate_key_ranges` (stricter than native): bytes pairs, `begin < end`,
  `end <= b"\xff"`, 1 to 1,024 ranges, and no duplicates or overlaps; adjacent is fine.
  The result is sorted.
- `canonicalize_key_ranges` merges adjacent ranges as the native listing does. An
  existing stream must equal this form.
- **Capability check.** `multi_range_cdc_unsupported_reason()` passes only if an API
  version is selected and the binding's `Database.register_cdc_stream` has a
  keyword-only `ranges` parameter and `fdb.CdcKeyRange` exists. That binding surface
  ships with the five-argument native symbol, so nothing native is probed. If the check
  fails, registration raises `CDCNotSupportedError` naming the missing piece, before any
  stream exists.
- `ensure_cdc_stream(name, ranges)` runs, in order: validate the ranges, check
  capability, compare them with the listed stream (if one exists), then make one native
  registration call. A mismatch raises `CDCStreamMismatchError`; the stream is never
  removed or re-registered to "repair" it.

## 3. Testing

Unit tests run without FoundationDB. The live-cluster integration tests are skipped unless
`FDB_CDC_INTEGRATION=1` is set in an environment containing the API-800 Python bindings,
CDC-enabled client library, and a reachable CDC-enabled cluster. CI's
`test-cdc-mutations` job sets it inside the `fdb-cdc:ci` image. Locally:

```bash
docker build -t fdb-cdc:ci .
docker run --rm -v "$PWD:/workspace" -w /workspace -e FDB_CDC_INTEGRATION=1 fdb-cdc:ci \
  bash -c 'pip3 install --break-system-packages -r requirements-dev.txt &&
           PYTHONPATH=/workspace:/workspace/src:/workspace/protobuf/gen \
           python3 -m pytest tests/cdc/ -v'
```

`tests/cdc/test_directory_integration.py` builds an isolated Directory Layer under a
per-run prefix. It places an unrelated directory between watched ones, registers one
multi-range stream, checks that the expected keys arrive and the unrelated ones do not,
checks the path mapping, resumes from a fingerprinted cursor, and confirms that a grown
tree is refused by both the fingerprint and the native immutable-range check. It removes
only its own stream and prefix. With the flag set, missing multi-range support fails
the test; it is never a skip.
