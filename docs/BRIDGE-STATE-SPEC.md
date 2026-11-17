# Bridge state spec

Contract for control-plane state schemas defined in
`protobuf/proto/fdbkafka/bridge/state/v1/state.proto` and implemented in
`src/fdb_kafka_bridge/serialization/state.py`.

## 1. Overview

FoundationDB CDC bridges require coordinated distributed state for:
1. **Durable checkpointing**: Storing the last safely delivered commit version in FoundationDB.
2. **Sink-mismatch protection**: Guarding against writing to or resuming from mismatched Kafka clusters/topics.
3. **Active leader leasing & fencing**: Ensuring exactly one active bridge worker produces per stream.

Control-plane infrastructure state is kept strictly separate from data-plane mutation records (`mutations.proto`).

## 2. Schemas & Models

### 2.1 KafkaSinkIdentity

Identifies the destination Kafka cluster and topic:

```protobuf
message KafkaSinkIdentity {
    string cluster_id = 1;
    string topic = 2;
}
```

### 2.2 CDCCheckpoint

Durable checkpoint state committed inside FoundationDB:

| Field | Type | Description |
|---|---|---|
| `format_version` | `uint32` | State schema format version (e.g. `1` for v0.2.0). |
| `source_id` | `string` | Stable operator-configured identity for the FDB source. |
| `stream_name` | `string` | Registered CDC stream name. |
| `stream_id` | `uint64` | Native CDC stream ID (distinguishes recreated streams). |
| `last_consumed_version` | `uint64` | Highest commit version fully acknowledged. |
| `sink` | `KafkaSinkIdentity` | Destination Kafka cluster and topic. |
| `updating_instance_id` | `string` | Unique instance ID of the bridge worker. |
| `update_timestamp` | `google.protobuf.Timestamp` | Wall-clock timestamp when checkpoint was committed. |
| `lease_epoch` | `uint64` | Active leader lease epoch during commit. |
| `range_config_fingerprint` | `string` | Fingerprint of Directory Layer range configuration. |

### 2.3 LeaderLease

Leader lease state committed inside FoundationDB:

| Field | Type | Description |
|---|---|---|
| `source_id` | `string` | Stable operator-configured identity for the FDB source. |
| `stream_name` | `string` | Registered CDC stream name. |
| `owner_instance_id` | `string` | Unique instance ID of the active leaseholder. |
| `lease_epoch` | `uint64` | Monotonically increasing lease epoch. |
| `heartbeat_sequence` | `uint64` | Monotonically increasing heartbeat sequence number. |
| `expiration_timestamp` | `google.protobuf.Timestamp` | Lease expiration wall-clock timestamp. |

> [!NOTE]
> The schema defines state representations only. Lease correctness requires
> transactional acquisition, conflict ranges, fencing checks, renewal, and takeover logic.

## 3. Python API

```python
from fdb_kafka_bridge.serialization import (
    CDCCheckpoint,
    KafkaSinkIdentity,
    LeaderLease,
    deserialize_checkpoint,
    deserialize_leader_lease,
    serialize_checkpoint,
    serialize_leader_lease,
)
```
