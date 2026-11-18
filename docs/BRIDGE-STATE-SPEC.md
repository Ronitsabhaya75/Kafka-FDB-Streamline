# Bridge state spec

Contract for control-plane state schemas defined in
`protobuf/proto/fdbkafka/bridge/state/v1/state.proto` and implemented in
`src/fdb_kafka_bridge/serialization/state.py`.

## 1. Overview

FoundationDB CDC bridges require coordinated distributed state for:
1. **Durable checkpointing**: Persisting the last safely acknowledged commit version in FoundationDB.
2. **Sink-mismatch protection**: Guarding against writing to or resuming from mismatched Kafka clusters or topics.
3. **Active leader leasing & fencing**: Ensuring exactly one active bridge worker produces per stream and guarding against split-brain execution.

Control-plane state is kept strictly separate from data-plane mutation records (`mutations.proto`). State records are committed in dedicated FoundationDB control-plane keyspace subspaces.

## 2. Schemas & Models

Defined in package `fdbkafka.bridge.state.v1`:

### 2.1 KafkaSinkIdentity

Identifies the destination Kafka cluster and topic sink:

| Field | Protobuf Type | Validation Rules | Description |
|---|---|---|---|
| `cluster_id` | `string` | Non-empty UTF-8 `str` | Destination Kafka cluster identifier. |
| `topic` | `string` | Non-empty UTF-8 `str` | Target Kafka topic name. |

### 2.2 CDCCheckpoint

Durable checkpoint state committed inside FoundationDB:

| Field | Protobuf Type | Validation Rules | Description |
|---|---|---|---|
| `format_version` | `uint32` | Must equal `1` | State schema format version. |
| `source_id` | `string` | Non-empty UTF-8 `str` | Stable operator-configured identity for the FDB cluster. |
| `stream_name` | `string` | Non-empty UTF-8 `str` | Registered CDC stream name. |
| `stream_id` | `optional uint64` | `0..2**64 - 1` or unset | Native CDC stream ID (distinguishes recreated streams; 0 is distinct from unset). |
| `last_consumed_version` | `int64` | `-1..2**63 - 1` | Highest commit version fully acknowledged to Kafka (`-1` indicates fresh cursor). |
| `sink` | `KafkaSinkIdentity` | Valid `KafkaSinkIdentity` | Destination Kafka cluster and topic. |
| `updating_instance_id` | `string` | Non-empty UTF-8 `str` | Unique instance ID of the bridge worker that wrote this checkpoint. |
| `update_timestamp` | `google.protobuf.Timestamp` | `0 <= seconds <= 253402300799`, `0 <= nanos < 10**9` | Wall-clock timestamp when checkpoint was committed. |
| `lease_epoch` | `uint64` | `1..2**64 - 1` | Positive leader lease epoch (`>= 1`; 0 is reserved/unset). |
| `range_config_fingerprint` | `bytes` | `bytes` (may be empty) | Binary fingerprint of Directory Layer range configuration. |

### 2.3 LeaderLease

Leader lease state committed inside FoundationDB for active leader coordination:

| Field | Protobuf Type | Validation Rules | Description |
|---|---|---|---|
| `format_version` | `uint32` | Must equal `1` | State schema format version. |
| `source_id` | `string` | Non-empty UTF-8 `str` | Stable operator-configured identity for the FDB cluster. |
| `stream_name` | `string` | Non-empty UTF-8 `str` | Registered CDC stream name. |
| `owner_instance_id` | `string` | Non-empty UTF-8 `str` | Unique instance ID of the active leaseholder. |
| `lease_epoch` | `uint64` | `1..2**64 - 1` | Monotonically increasing positive lease epoch (`>= 1`; 0 is reserved/unset). |
| `heartbeat_sequence` | `uint64` | `0..2**64 - 1` | Monotonically increasing heartbeat sequence number within the epoch. |
| `expiration_timestamp` | `google.protobuf.Timestamp` | `0 <= seconds <= 253402300799`, `0 <= nanos < 10**9` | Lease expiration wall-clock timestamp. |
| `stream_id` | `optional uint64` | `0..2**64 - 1` or unset | Optional native CDC stream ID bound to this lease. |

## 3. Operational Invariants & Semantics

The schemas and codecs in `fdbkafka.bridge.state.v1` define the wire formats, models, and domain validation. The operational invariants described below (inclusive cursor resumption, transactional epoch fencing, heartbeat renewal, and sink-mismatch startup refusal) are the architectural contract for the upcoming Checkpoint Store and Leader Lease cards, which will enforce them at runtime.

### 3.1 Inclusive Cursor Semantics

- `last_consumed_version` is **inclusive**: it records the highest FDB commit version whose mutations have been fully persisted in Kafka and acknowledged.
- A fresh CDC stream with no prior committed versions is initialized with `last_consumed_version = -1`, meaning the stream's current `minVersion`.
- When resuming replication, the bridge supplies the persisted cursor (`stream_id`, `last_consumed_version`) unchanged to `resume_cdc_consumer(cursor)`, which natively delivers versions strictly after `last_consumed_version` (see `CDC-DEEP-DIVE.md` §1.3–1.6).

### 3.2 Leader Lease Fencing & Recovery (Contract for Leader Lease & Checkpoint Store)

- Exactly one bridge instance may hold an active lease for a given `(source_id, stream_name)` (or `(source_id, stream_name, stream_id)` when configured).
- Epochs start at 1 (`lease_epoch >= 1`); 0 represents unset/unassigned.
- Checkpoint updates require transactional verification against the active lease in FoundationDB: if `checkpoint.lease_epoch` does not match the active `LeaderLease.lease_epoch`, the transaction aborts.
- During failover, a new leader increments `lease_epoch`, updates `owner_instance_id`, resets `heartbeat_sequence = 0`, and writes a new `expiration_timestamp`.
- Fenced former leaders attempting to commit stale checkpoints are rejected by FDB conflict detection or explicit epoch mismatch.

### 3.3 Sink Mismatch Protection (Contract for Bridge Startup)

- The `sink` field records destination Kafka coordinates (`cluster_id`, `topic`).
- On bridge startup, the worker compares its configured Kafka target against `checkpoint.sink`. If they do not match, the worker refuses to start, preventing cross-cluster corruption or duplicate delivery across disjoint clusters.

## 4. Error Handling & Fail-Closed Contract

Control-plane state operations differ fundamentally from streaming mutation records:

- **Fail-Closed**: In streaming data ingestion, corrupt records may optionally be dead-lettered to preserve stream continuity. In contrast, corrupt or unparseable control-plane state (`CDCCheckpoint` or `LeaderLease`) must **fail closed**. The bridge must abort execution and raise an alert rather than skipping or guessing state coordinates.
- **Error Types**:
  - `InputTypeError`: An argument has the wrong Python type (e.g. non-bytes passed to deserializer).
  - `InputValueError`: A model field value is outside its valid domain (e.g. invalid format version, negative epoch).
  - `StateDecodeError`: Raised when decoding malformed or unsupported state bytes:
    - `DecodeFailure.UNSUPPORTED_FORMAT_VERSION`: `format_version` not in supported set (currently only `1`).
    - `DecodeFailure.VERSION_OUT_OF_RANGE`: `last_consumed_version` outside `-1..2**63 - 1`.
    - `DecodeFailure.MALFORMED_WIRE`: Corrupt protobuf bytes, missing required identities, or invalid timestamp components.

## 5. Python API

```python
from fdb_kafka_bridge.serialization import (
    CDCCheckpoint,
    KafkaSinkIdentity,
    LeaderLease,
    StateDecodeError,
    deserialize_checkpoint,
    deserialize_leader_lease,
    serialize_checkpoint,
    serialize_leader_lease,
)

# Example Checkpoint Serialization
checkpoint = CDCCheckpoint(
    format_version=1,
    source_id="fdb-primary",
    stream_name="orders",
    stream_id=12345,
    last_consumed_version=1000500,
    sink=KafkaSinkIdentity(cluster_id="kafka-prod", topic="orders-cdc"),
    updating_instance_id="bridge-pod-1",
    update_timestamp_ns=1700000000_000000000,
    lease_epoch=3,
    range_config_fingerprint=b"sha256:abc123hash",
)

wire_bytes = serialize_checkpoint(checkpoint)
restored = deserialize_checkpoint(wire_bytes)
assert restored == checkpoint

# Example Leader Lease Serialization
lease = LeaderLease(
    format_version=1,
    source_id="fdb-primary",
    stream_name="orders",
    owner_instance_id="bridge-pod-1",
    lease_epoch=3,
    heartbeat_sequence=42,
    expiration_timestamp_ns=1700000030_000000000,
    stream_id=12345,
)

lease_wire = serialize_leader_lease(lease)
restored_lease = deserialize_leader_lease(lease_wire)
assert restored_lease == lease
```
