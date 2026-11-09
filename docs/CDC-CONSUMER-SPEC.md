# CDC Consumer Module Spec

Contract for `src/fdb_kafka_bridge/cdc/`: FoundationDB native CDC client initialization, stream
lifecycle, bounded record polling, and safe acknowledgement.

Trello: [Basic FDB Client & Mutation Stream Listener](https://trello.com/c/0Wn8zbEk) (CDC Engine).
Related architecture: [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md) (research; this spec is normative for
this repo's behaviour).
Schema: [`mutations.proto`](../protobuf/proto/fdbkafka/cdc/v1/mutations.proto).

## 1. Scope & Rules

- **FoundationDB API version:** Gated on API version 800 or later (default `fdb.api_version(800)`).
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
  completed. A serialization failure permanently blocks both acknowledgement and every
  later `poll_records()` on that listener; close it and resume a new listener from the last
  durable cursor.
- **Protobuf Fidelity:** Raw FDB mutations are mapped directly into Protobuf types
  (`FDBSingleKeyMutation`, `FDBClearRange`, `FDBMutation`, `FDBMutationBatch`, `FDBMutationRecord`).
  `poll_records()` uses `fdb_kafka_bridge.serialization.serialize_version_group()` to split each
  version group into records no larger than `max_record_bytes`, then emits its
  `VersionEnd`. A reply watermark beyond the final version group is represented by an
  additional zero-mutation `VersionEnd`, which is held to the same `max_record_bytes` limit
  (`RecordTooLargeError` otherwise). Every returned record, including an idle
  watermark record, belongs to the native reply and must be durably processed before ack.
- **Idle acknowledgement cadence:** ack-per-reply is deliberate. Any non-empty reply,
  including one that carries only an idle watermark `VersionEnd`, requires an ack before
  the next poll, so an idle stream costs one ack per watermark-advancing reply. How often an
  idle stream yields such replies is UNVERIFIED: the 5 s figure is the server-side lease
  (`CDC_PROXY_CONSUME_POLL_TIMEOUT`), which the client renews rather than returning
  ([CDC deep dive §2.7](CDC-DEEP-DIVE.md#27-threads-blocking-and-the-network-thread-)). The
  cumulative-ack coalescing option in that section — only replies carrying **no mutations** may
  be coalesced: consume again without acking, then checkpoint and ack at most every few seconds
  or when a reply carries data — is tracked as a follow-up, not implemented here.
- **Resume Safety:** Resume waits until a fresh database read version reaches the cursor's
  `last_consumed_version`, then re-acknowledges the resumed cursor before polling. This
  restores the native consumer's retention watermark after process restart. If either step
  fails, the partially resumed consumer is closed before the error propagates.

## 2. Public API

Import from `fdb_kafka_bridge.cdc`; everything in its `__all__` is contract.

### 2.1 Client & Connection (`src/fdb_kafka_bridge/cdc/client.py`)

```python
DEFAULT_CDC_API_VERSION: Final[int] = 800   # default selected by init_fdb / FDBClient
MINIMUM_CDC_API_VERSION: Final[int] = 800   # lowest version init_fdb accepts

init_fdb(api_version: int = DEFAULT_CDC_API_VERSION) -> None
```
Selects the FDB API version; a repeat call with the same version is a no-op. Raises
`CDCNotSupportedError` if `api_version < 800` (checked first) or `fdb` is not installed, and
`RuntimeError` if FDB was already initialized with a different version.

```python
class FDBClient:
    cluster_file: str | None
    api_version: int

    def __init__(self, cluster_file: str | None = None, api_version: int = 800) -> None: ...
    def open(self) -> Any: ...            # init_fdb + fdb.open, cached; raises as init_fdb
    @property
    def db(self) -> Any: ...              # open() on first access
    def close(self) -> None: ...          # drops the cached handle only; frees no native resource
    def __enter__(self) -> Self: ...      # open()
    def __exit__(self, *exc) -> None: ... # close()
    def register_cdc_stream(self, name: bytes | str, begin_key: bytes, end_key: bytes) -> int: ...
    def create_cdc_consumer(self, name: bytes | str) -> Any: ...
    def resume_cdc_consumer(self, cursor: Any) -> Any: ...
    def remove_cdc_stream(self, name: bytes | str) -> None: ...
    def list_cdc_streams(self) -> list[Any]: ...
    def get_read_version(self) -> int: ...
```

Stream methods translate a binding `RuntimeError` that reports missing native CDC into
`CDCNotSupportedError` and re-raise any other `RuntimeError` or native `FDBError` unchanged.
`register_cdc_stream` raises `CDCInvalidRangeError` and `resume_cdc_consumer` raises
`CDCInvalidCursorError` before any native call.

### 2.2 Mutation Stream Listener (`src/fdb_kafka_bridge/cdc/listener.py`)

```python
class FDBMutationListener:
    client: FDBClient
    raw_stream_name: bytes        # stream name as passed to FDB
    stream_name: str              # UTF-8 decode (errors="replace"), written into records

    def __init__(
        self,
        client: FDBClient,
        stream_name: str | bytes,
        *,
        key_range: tuple[bytes, bytes] | None = None,
        subspace: Any | None = None,
        cursor: Any | None = None,
        max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,  # 1_000_000
        resume_timeout_seconds: float = 10.0,
        resume_poll_interval_seconds: float = 0.05,
    ) -> None: ...

    def start(self) -> None: ...
    def close(self) -> None: ...
    def poll_records(self, bridge_timestamp_ns: int | None = None) -> list[mutations_pb2.FDBMutationRecord]: ...
    def acknowledge(self) -> None: ...
    def get_position(self) -> Any: ...

    @property
    def consumer(self) -> Any: ...       # native CdcConsumer; starts the listener on first access
    @property
    def awaiting_ack(self) -> bool: ...  # a non-empty reply was delivered and not yet acked
    @property
    def delivery_failed(self) -> bool: ...  # a reply failed to serialize; sticky

    def __enter__(self) -> Self: ...     # start()
    def __exit__(self, *exc) -> None: ...  # close()
```

Supply exactly one of `key_range` or `subspace` to register a stream when the listener
starts. Omit both to consume a stream registered elsewhere. Supplying both is invalid.

`get_position()` returns the native cursor for the last **consumed** reply, which may be ahead
of the last acknowledged one. That includes a reply that failed to serialize and was never
returned. Persist it only after the records it covers are durable, and never while
`delivery_failed` is set: resuming from it would skip that reply.

`close()` closes the native consumer only: it never acknowledges and never removes the stream.
Only `acknowledge()` and the resume re-ack acknowledge.

`poll_records()` and `consumer` start the listener lazily. `consumer` raises whatever `start()`
raises, unchanged. `poll_records()` starts inside its consume failure handling, so a `start()`
failure is classified like a native consume failure (§2.4): `CDCRetryableError` /
`CDCTerminalError` keep their class, a raw `FDBError` with code 1007, 1211 or 2000 becomes
`CDCTerminalError` / `CDCRetryableError`, any other raw `FDBError` propagates unchanged, and
anything else (`CDCInvalidCursorError`, `CDCNotSupportedError`, `CDCInvalidRangeError`,
`RuntimeError`, …) is wrapped as `CDCError` with the original as `__cause__`. Call `start()`
first to get its exceptions unwrapped.

| Method | Raises |
|---|---|
| `start()` | `CDCConsumerClosedError` (closed listener); `CDCInvalidRangeError`; `CDCInvalidCursorError` (malformed cursor, or read-version wait timed out); `CDCNotSupportedError`; `RuntimeError` (API-version conflict, or any binding `RuntimeError` not about CDC support); `CDCRetryableError` / `CDCTerminalError` / `CDCError` from the resume re-ack, classified per §2.4; raw native `FDBError` from open, registration, consumer creation, the resume read-version wait, or any other re-ack code. |
| `poll_records()` | `CDCConsumerClosedError` (closed, including closed while the consume was in flight); `CDCError` (reply awaiting ack, prior serialization failure, or wrapped non-native consume failure); `CDCRetryableError` / `CDCTerminalError`; raw native `FDBError` for any other code; `SerializationError` subclasses including `RecordTooLargeError`; a first-call `start()` failure, classified as above. |
| `acknowledge()` | `CDCConsumerClosedError` (closed, including closed while the ack was in flight); `CDCError` (nothing awaiting ack, prior serialization failure, or wrapped non-native ack failure); `CDCRetryableError` / `CDCTerminalError`; raw native `FDBError` for any other code. |
| `get_position()` | `CDCConsumerClosedError` (closed listener); starts the listener lazily, so whatever `start()` raises, unchanged. |
| `close()` | Whatever the native `close()` raises. |

### 2.3 Prefix Utilities (`src/fdb_kafka_bridge/cdc/listener.py`)

- `strinc(key: bytes) -> bytes`: Computes the lexicographically adjacent prefix upper bound.
  Raises `ValueError` for an empty or all-`0xFF` key.
- `subspace_to_key_range(subspace: Any) -> tuple[bytes, bytes]`: Extracts `(prefix, strinc(prefix))`.

### 2.4 Errors (`src/fdb_kafka_bridge/cdc/errors.py`)

This table is the single source for what each CDC error means and whether it may be retried.
Other specs link to these anchors rather than restate them.

| Error | Meaning | Retry? |
|---|---|---|
| <a id="err-cdc-error"></a>`CDCError` | Base class. Raised directly for listener state violations (poll while a reply awaits ack, ack with nothing pending, any poll/ack after a serialization failure) and for a non-native consume/ack failure, wrapped with the original as `__cause__`. | State violations: no, they are caller bugs or need a resume. Wrapped failures: unknown; treat as terminal for the listener. |
| <a id="err-cdc-not-supported"></a>`CDCNotSupportedError` | The local client cannot provide native CDC: API version < 800, `fdb` bindings missing, or a binding `RuntimeError` reporting no native CDC. A cluster without `ENABLE_NATIVE_CDC` is **not** detected; registration there fails as a raw `FDBError`. | No. Configuration error. |
| <a id="err-cdc-closed"></a>`CDCConsumerClosedError` | Operation on a closed listener, including a consume or ack that fails because `close()` ran while it was in flight. | No. Create a new listener. |
| <a id="err-cdc-invalid-cursor"></a>`CDCInvalidCursorError` | Resume cursor lacks `stream_id`/`last_consumed_version` or has out-of-range values (permanent), **or** the resume read-version wait timed out before reaching the cursor version (possibly transient: the cursor may be ahead of the read version only briefly). | Malformed: no. Timeout: a fresh resume from the same cursor may succeed; bound the attempts. |
| <a id="err-cdc-invalid-range"></a>`CDCInvalidRangeError` | Registration range is not bytes, empty, or reversed. | No. |
| <a id="err-cdc-consume"></a>`CDCConsumeError` | Base for a classified native consume/ack failure. Carries the native FDB `code` on the surface, not behind `__cause__`; always set when this package raises it. | Per subclass. |
| <a id="err-cdc-retryable"></a>`CDCRetryableError` | `server_overloaded` (1211): reply-budget or buffer-limit trip. | Yes, in place, with bounded backoff. A failed ack leaves the reply awaiting ack, so `acknowledge()` may be retried. If it repeats at one commit version it is the L1 poison pill: escalate, do not spin. |
| <a id="err-cdc-terminal"></a>`CDCTerminalError` | `transaction_too_old` (1007): the cursor is behind the acked watermark (a bridge bug) or the data it needs was popped (a retention-invariant violation). `client_invalid_operation` (2000): concurrent consumer, unproven cursor, or a predecessor's poll still holding the proxy lease. | No. 1007: alert and rebuild. 2000: terminal, apart from the narrow ≤5 s retry-once-past-the-lease case, which the caller implements from `code`. |
| <a id="err-cdc-raw-fdb"></a>raw `fdb.FDBError` | Any other native consume/ack code, re-raised unchanged so callers keep normal FDB retry and diagnostics. | Per the code's own FDB semantics. |

Only these three CDC error codes are classified ([CDC deep dive §3](CDC-DEEP-DIVE.md#errors-we-have-to-map-),
whose standard `is_retryable` predicate is wrong about two of them).

## 3. Testing

Unit tests run without FoundationDB. The live-cluster integration test is skipped unless
`FDB_CDC_INTEGRATION=1` is set in an environment containing the API-800 Python bindings,
CDC-enabled client library, and a reachable CDC-enabled cluster.
