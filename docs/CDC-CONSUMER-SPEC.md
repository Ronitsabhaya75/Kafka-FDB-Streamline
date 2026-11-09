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
  additional zero-mutation `VersionEnd`. Every returned record, including an idle
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
  restores the native consumer's retention watermark after process restart.

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
        max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,  # 999_000
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
    def delivery_failed(self) -> bool: ...  # a reply failed to serialize; sticky (INV-CDC-3)

    def __enter__(self) -> Self: ...     # start()
    def __exit__(self, *exc) -> None: ...  # close()
```

Supply exactly one of `key_range` or `subspace` to register a stream when the listener
starts. Omit both to consume a stream registered elsewhere. Supplying both is invalid.

`get_position()` returns the native cursor for the last **delivered** reply, which may be ahead
of the last acknowledged one. Persist it only after the records it covers are durable.

`poll_records()` and `consumer` start the listener lazily, so their first call raises whatever
`start()` raises, unwrapped (INV-CDC-14).

| Method | Raises |
|---|---|
| `start()` | `CDCConsumerClosedError` (closed listener); `CDCInvalidRangeError`; `CDCInvalidCursorError` (malformed cursor, or read-version wait timed out); `CDCNotSupportedError`; `RuntimeError` (API-version conflict); `CDCRetryableError` / `CDCTerminalError` / `CDCError` from the resume re-ack, classified per §2.4; raw native `FDBError` from open, registration, consumer creation, or any other re-ack code. |
| `poll_records()` | `CDCConsumerClosedError` (closed, including closed while the consume was in flight); `CDCError` (reply awaiting ack, prior serialization failure, or wrapped non-native consume failure); `CDCRetryableError` / `CDCTerminalError`; raw native `FDBError` for any other code; `SerializationError` subclasses including `RecordTooLargeError`; anything `start()` raises on first call. |
| `acknowledge()` | `CDCConsumerClosedError` (closed, including closed while the ack was in flight); `CDCError` (nothing awaiting ack, prior serialization failure, or wrapped non-native ack failure); `CDCRetryableError` / `CDCTerminalError`; raw native `FDBError` for any other code. |
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
| <a id="err-cdc-retryable"></a>`CDCRetryableError` | `server_overloaded` (1211): reply-budget or buffer-limit trip. | Yes, in place, with bounded backoff. A failed ack leaves the reply awaiting ack, so `acknowledge()` may be retried (INV-CDC-4). If it repeats at one commit version it is the L1 poison pill: escalate, do not spin. |
| <a id="err-cdc-terminal"></a>`CDCTerminalError` | `transaction_too_old` (1007): the cursor is behind the acked watermark (a bridge bug) or the data it needs was popped (a retention-invariant violation). `client_invalid_operation` (2000): concurrent consumer, unproven cursor, or a predecessor's poll still holding the proxy lease. | No. 1007: alert and rebuild. 2000: terminal, apart from the narrow ≤5 s retry-once-past-the-lease case, which the caller implements from `code`. |
| <a id="err-cdc-raw-fdb"></a>raw `fdb.FDBError` | Any other native consume/ack code, re-raised unchanged so callers keep normal FDB retry and diagnostics. | Per the code's own FDB semantics. |

Only these three CDC error codes are classified ([CDC deep dive §3](CDC-DEEP-DIVE.md#errors-we-have-to-map-),
whose standard `is_retryable` predicate is wrong about two of them).

## 3. Testing

Unit tests run without FoundationDB. The live-cluster integration test is skipped unless
`FDB_CDC_INTEGRATION=1` is set in an environment containing the API-800 Python bindings,
CDC-enabled client library, and a reachable CDC-enabled cluster.

## Invariants

- <a id="inv-cdc-1"></a>**INV-CDC-1.** A listener holds at most one non-empty unacknowledged reply; `poll_records()` raises `CDCError` while one is pending. Enforced by: `listener.FDBMutationListener._consume_batch`. Pinned by: `tests/cdc/test_listener.py::test_poll_records_serializes_group_and_requires_ack`.
- <a id="inv-cdc-2"></a>**INV-CDC-2.** A reply that yields zero records needs no ack and does not block the next poll. Enforced by: `listener.FDBMutationListener.poll_records`. Pinned by: `tests/cdc/test_listener.py::test_zero_record_reply_needs_no_ack_and_does_not_block_poll`.
- <a id="inv-cdc-3"></a>**INV-CDC-3.** `delivery_failed` is set before a reply is serialized and cleared only after every record parses; once a serialization fails it stays set, and both `poll_records()` and `acknowledge()` raise `CDCError` for the listener's life. Enforced by: `listener.FDBMutationListener.poll_records`, `listener.FDBMutationListener.acknowledge`. Pinned by: `tests/cdc/test_listener.py::test_serialization_failure_blocks_ack_and_poll`.
- <a id="inv-cdc-4"></a>**INV-CDC-4.** A failed native ack leaves `awaiting_ack` set, so `acknowledge()` can be retried in place. Enforced by: `listener.FDBMutationListener.acknowledge`. Pinned by: `tests/cdc/test_listener.py::test_failed_native_ack_can_be_retried_in_place`.
- <a id="inv-cdc-5"></a>**INV-CDC-5.** Every returned record, including the watermark `VersionEnd`, is at most `max_record_bytes`, or `poll_records()` raises `RecordTooLargeError`. Enforced by: `serialization.serialize_version_group`, `listener.FDBMutationListener.poll_records`. Pinned by: `tests/cdc/test_listener.py::test_poll_records_slices_large_version_group_to_record_budget`, `tests/cdc/test_listener.py::test_oversized_watermark_version_end_raises_and_blocks_delivery`.
- <a id="inv-cdc-6"></a>**INV-CDC-6.** An extra zero-mutation `VersionEnd` is emitted iff the reply watermark is ≥ 0 and greater than the last version group's version. Enforced by: `listener.FDBMutationListener.poll_records`. Pinned by: `tests/cdc/test_listener.py::test_poll_records_preserves_watermark_after_final_group`, `tests/cdc/test_listener.py::test_no_watermark_version_end_unless_beyond_last_group`.
- <a id="inv-cdc-7"></a>**INV-CDC-7.** Resume never acks until a fresh read version reaches the cursor version, re-acks before the first poll, and closes the partial consumer on any failure, re-ack failures classified as in §2.4. Enforced by: `listener.FDBMutationListener.start`, `listener.FDBMutationListener._reconcile_resumed_consumer`. Pinned by: `tests/cdc/test_listener.py::test_resume_waits_for_read_version_and_restores_ack`, `tests/cdc/test_listener.py::test_resume_timeout_closes_partial_consumer`, `tests/cdc/test_listener.py::test_resume_reack_failure_is_classified_and_closes_partial_consumer`.
- <a id="inv-cdc-8"></a>**INV-CDC-8.** Native consume/ack failures are classified: 1211 → `CDCRetryableError`; 1007 and 2000 → `CDCTerminalError`, both with `.code`; any other native code re-raised unchanged; a non-native failure → `CDCError` with the original as `__cause__`. Enforced by: `client._raise_consume_failure`. Pinned by: `tests/cdc/test_listener.py::test_poll_records_maps_terminal_fdb_codes`, `tests/cdc/test_listener.py::test_poll_records_maps_server_overloaded_to_retryable`, `tests/cdc/test_listener.py::test_poll_records_passes_through_unknown_fdb_error`, `tests/cdc/test_listener.py::test_poll_records_wraps_non_native_failure_as_cdc_error`.
- <a id="inv-cdc-9"></a>**INV-CDC-9.** Only `acknowledge()` and resume reconciliation ack; `close()` never acks and never removes the stream. Enforced by: `listener.FDBMutationListener.close`. Pinned by: `tests/cdc/test_listener.py::test_close_never_acks_or_removes_stream`.
- <a id="inv-cdc-10"></a>**INV-CDC-10.** The API-version gate runs before the bindings check, so a version < 800 raises the version error even without `fdb`. Enforced by: `client.init_fdb`. Pinned by: `tests/cdc/test_client.py::test_init_fdb_version_too_low_without_fdb`.
- <a id="inv-cdc-11"></a>**INV-CDC-11.** Registration ranges and resume cursors are validated locally before any native call. Enforced by: `client.FDBClient.register_cdc_stream`, `client.FDBClient.resume_cdc_consumer`. Pinned by: `tests/cdc/test_client.py::test_register_cdc_stream_rejects_invalid_range`, `tests/cdc/test_client.py::test_resume_cdc_consumer_rejects_invalid_cursor`.
- <a id="inv-cdc-12"></a>**INV-CDC-12.** A subspace registers `[prefix, strinc(prefix))`. Enforced by: `listener.subspace_to_key_range`. Pinned by: `tests/cdc/test_listener.py::test_subspace_listener_registers_strinc_bounded_range`.
- <a id="inv-cdc-13"></a>**INV-CDC-13.** Once closed, `poll_records()` and `acknowledge()` raise `CDCConsumerClosedError`, including when `close()` races an in-flight native call. Enforced by: `listener.FDBMutationListener.consumer`, `listener.FDBMutationListener._consume_batch`, `listener.FDBMutationListener.acknowledge`. Pinned by: `tests/cdc/test_listener.py::test_closed_listener_rejects_poll_and_ack`, `tests/cdc/test_listener.py::test_close_racing_consume_raises_closed_error`.
- <a id="inv-cdc-14"></a>**INV-CDC-14.** A lazy start inside `poll_records()` raises `start()`'s exceptions with their own class, never re-wrapped as a consume failure. Enforced by: `listener.FDBMutationListener._consume_batch`. Pinned by: `tests/cdc/test_listener.py::test_lazy_start_in_poll_keeps_cursor_timeout_class`, `tests/cdc/test_listener.py::test_lazy_start_in_poll_keeps_not_supported_class`.
