# CDC Consumer Module Spec

Contract for `src/cdc/`: FoundationDB native CDC client initialization, stream
lifecycle, bounded record polling, and safe acknowledgement.

Trello: [Basic FDB Client & Mutation Stream Listener](https://trello.com/c/0Wn8zbEk) (CDC Engine).
Related architecture: [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md).
Schema: [`mutations.proto`](../protobuf/proto/fdbkafka/cdc/v1/mutations.proto).

## 1. Scope & Rules

- **FoundationDB API version:** Strictly gated on API version 800 (`fdb.api_version(800)`).
  Selecting an API version < 800 raises `CDCNotSupportedError`.
- **Subspace range alignment:** Stream registration covers `[rawPrefix, strinc(rawPrefix))`,
  ensuring bare prefix and escaped suffix keys are strictly included
  ([CDC deep dive §2.5](CDC-DEEP-DIVE.md#25-registering-a-directorys-range-)).
- **Ordering & Lifecycle:**
  `consume()` → durably process/checkpoint → `acknowledge()`.
  Acknowledgement is cumulative, takes no arguments, and advances the cluster retention
  watermark to `last_consumed_version + 1`. A listener rejects another consume until a
  non-empty delivered reply is acknowledged. There is no automatic acknowledgement:
  the listener cannot know whether Kafka publication and checkpoint persistence have
  completed. A serialization failure permanently blocks acknowledgement on that listener;
  close it and resume from the last durable cursor.
- **Protobuf Fidelity:** Raw FDB mutations are mapped directly into Protobuf types
  (`FDBSingleKeyMutation`, `FDBClearRange`, `FDBMutation`, `FDBMutationBatch`, `FDBMutationRecord`).
  `poll_records()` uses `src.serialization.serialize_version_group()` to split each
  version group into records no larger than `max_record_bytes`, then emits its
  `VersionEnd`. A reply watermark beyond the final version group is represented by an
  additional zero-mutation `VersionEnd`. Every returned record, including an idle
  watermark record, belongs to the native reply and must be durably processed before ack.
- **Idle acknowledgement cadence:** ack-per-reply is deliberate. Any non-empty reply,
  including one that carries only an idle watermark `VersionEnd`, requires an ack before
  the next poll. At the ~5 s idle cadence this is one ack per idle reply, which keeps the
  ordering rule uniform.   The cumulative-ack coalescing option in
  [CDC deep dive §2.7](CDC-DEEP-DIVE.md#27-threads-blocking-and-the-network-thread-)
  (consume again without acking, then checkpoint and ack once every N replies or T
  seconds) is tracked as a follow-up, not implemented here.
- **Resume Safety:** Resume waits until a fresh database read version reaches the cursor's
  `last_consumed_version`, then re-acknowledges the resumed cursor before polling. This
  restores the native consumer's retention watermark after process restart.

## 2. Public API

Import from `src.cdc`; everything in its `__all__` is contract.

### 2.1 Client & Connection (`src/cdc/client.py`)

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
    def register_cdc_stream(self, name: bytes | str, begin_key: bytes, end_key: bytes) -> int: ...
    def create_cdc_consumer(self, name: bytes | str) -> Any: ...
    def resume_cdc_consumer(self, cursor: Any) -> Any: ...
    def remove_cdc_stream(self, name: bytes | str) -> None: ...
    def list_cdc_streams(self) -> list[Any]: ...
    def get_read_version(self) -> int: ...
```

### 2.2 Mutation Stream Listener (`src/cdc/listener.py`)

```python
class FDBMutationListener:
    def __init__(
        self,
        client: FDBClient,
        stream_name: str | bytes,
        *,
        key_range: tuple[bytes, bytes] | None = None,
        subspace: Any | None = None,
        cursor: Any | None = None,
        max_record_bytes: int = 1_000_000,
        resume_timeout_seconds: float = 10.0,
        resume_poll_interval_seconds: float = 0.05,
    ) -> None: ...

    def start(self) -> None: ...
    def close(self) -> None: ...
    def poll_records(self, bridge_timestamp_ns: int | None = None) -> list[mutations_pb2.FDBMutationRecord]: ...
    def acknowledge(self) -> None: ...
    def get_position(self) -> Any: ...
```

Supply exactly one of `key_range` or `subspace` to register a stream when the listener
starts. Omit both to consume a stream registered elsewhere. Supplying both is invalid.

### 2.3 Prefix Utilities (`src/cdc/listener.py`)

- `strinc(key: bytes) -> bytes`: Computes the lexicographically adjacent prefix upper bound.
- `subspace_to_key_range(subspace: Any) -> tuple[bytes, bytes]`: Extracts `(prefix, strinc(prefix))`.

### 2.4 Errors (`src/cdc/errors.py`)

- `CDCError`: Base class for CDC exceptions.
- `CDCNotSupportedError`: FDB bindings missing, API version < 800, or native CDC
  unavailable in the loaded binding.
- `CDCConsumerClosedError`: Operation attempted on closed consumer handle.
- `CDCInvalidCursorError`: Invalid resume cursor coordinates.
- `CDCInvalidRangeError`: Invalid key range supplied.
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

## 3. Testing

Unit tests run without FoundationDB. The live-cluster integration test is skipped unless
`FDB_CDC_INTEGRATION=1` is set in an environment containing the API-800 Python bindings,
CDC-enabled client library, and a reachable CDC-enabled cluster.
