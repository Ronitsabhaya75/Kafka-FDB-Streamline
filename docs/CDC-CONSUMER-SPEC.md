# CDC Consumer Module Spec

Contract for `src/cdc/`: FoundationDB native CDC client initialization, stream
lifecycle, continuous polling loop, and Protobuf message mapping.

Trello: [Basic FDB Client & Mutation Stream Listener](https://trello.com) (CDC Engine).
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
  non-empty delivered reply is acknowledged. Stream limits retain undelivered items and
  never acknowledge a partial native reply. Streaming helpers with `auto_ack=False`
  return after one complete native reply, allowing the caller to checkpoint and
  acknowledge before requesting the next reply.
- **Protobuf Fidelity:** Raw FDB mutations are mapped directly into Protobuf types
  (`FDBSingleKeyMutation`, `FDBClearRange`, `FDBMutation`, `FDBMutationBatch`, `FDBMutationRecord`).
  `poll_records()` emits a `VersionEnd` after every complete version group and represents
  empty watermark advances as zero-mutation version boundaries.

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
        auto_register: bool = True,
    ) -> None: ...

    def start(self) -> None: ...
    def close(self) -> None: ...
    def consume_batch(self) -> Any: ...
    def poll(self) -> list[mutations_pb2.FDBMutation]: ...
    def poll_records(self, bridge_timestamp_ns: int | None = None) -> list[mutations_pb2.FDBMutationRecord]: ...
    def acknowledge(self) -> None: ...
    def get_position(self) -> Any: ...

    def stream_mutations(self, max_mutations: int | None = None, auto_ack: bool = False) -> Iterator[mutations_pb2.FDBMutation]: ...
    def stream_records(self, max_records: int | None = None, auto_ack: bool = False) -> Iterator[mutations_pb2.FDBMutationRecord]: ...
```

### 2.3 Prefix Utilities (`src/cdc/listener.py`)

- `strinc(key: bytes) -> bytes`: Computes the lexicographically adjacent prefix upper bound.
- `subspace_to_key_range(subspace: Any) -> tuple[bytes, bytes]`: Extracts `(prefix, strinc(prefix))`.

### 2.4 Protobuf Mappers (`src/cdc/mapper.py`)

```python
to_single_key_mutation(key: bytes, value: bytes, mutation_type: int) -> FDBSingleKeyMutation
to_clear_range(begin_key: bytes, end_key: bytes) -> FDBClearRange
to_version_index(fdb_version: int, sequence_no: int) -> FDBVersionIndex
to_fdb_mutation(raw_mutation: Any, fdb_version: int, sequence_no: int) -> FDBMutation
to_fdb_mutation_batch(raw_mutations: Iterable[Any], fdb_version: int, first_sequence_no: int = 0) -> FDBMutationBatch
to_version_end(fdb_version: int, total_mutations: int, bridge_timestamp_ns: int | None = None) -> VersionEnd
to_mutation_record(stream_name: str, *, mutation=None, batch=None, version_end=None, bridge_timestamp_ns=None) -> FDBMutationRecord
```

### 2.5 Errors (`src/cdc/errors.py`)

- `CDCError`: Base class for CDC exceptions.
- `CDCNotSupportedError`: FDB bindings missing or API version < 800.
- `CDCStreamNotFoundError`: Stream missing or removed.
- `CDCConsumerClosedError`: Operation attempted on closed consumer handle.
- `CDCInvalidCursorError`: Invalid resume cursor coordinates.
- `CDCInvalidRangeError`: Invalid key range supplied.

## 3. Testing

Unit tests run without FoundationDB. The live-cluster integration test is skipped unless
`FDB_CDC_INTEGRATION=1` is set in an environment containing the API-800 Python bindings,
CDC-enabled client library, and a reachable CDC-enabled cluster.
