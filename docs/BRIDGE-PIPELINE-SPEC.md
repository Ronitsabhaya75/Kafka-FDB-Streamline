# Bridge pipeline specification

`src.bridge.BridgePipeline` is the CDC-to-Kafka bridge.  Each `step()` call
processes exactly one native CDC reply: poll → serialize → produce → confirm →
acknowledge.

## Overview

```
FDB CDC listener
       │
       ▼
  poll_records()          ← one native reply → list[FDBMutationRecord]
       │
       ▼
  produce(record, key)    ← for every record in the batch
       │
       ▼
  flush() / commit()      ← idempotent: flush; transactional: commit
       │
       ▼
  acknowledge()           ← advance the FDB cursor
```

The pipeline does not own the listener or producer lifecycle.  The caller
creates, starts, initializes, and closes them.

## Idempotent mode

When the producer is **not** transactional:

1. All records are produced with `produce()`.
2. `flush()` blocks until all messages are broker-acknowledged.
3. If `flush()` returns zero outstanding messages and no delivery error was
   recorded, the CDC batch is acknowledged.
4. If `flush()` returns nonzero, or raises `KafkaException`, or a delivery
   callback recorded an error, `PublishFailedError` is raised and **no
   acknowledgement occurs**.

This provides **at-least-once** delivery within one producer session.  A
crash after `flush()` but before `acknowledge()` causes FDB to replay the
batch, so downstream consumers may see duplicates.

## Transactional mode

When the producer has a `transactional_id`:

1. `begin_transaction()` opens a Kafka transaction.
2. All records are produced with `produce()`.
3. `commit_transaction()` atomically commits the batch.
4. On success, the CDC batch is acknowledged.
5. On failure, the error is classified:
   - **Abort-required** or **retriable**: the transaction is aborted and
     `PublishFailedError` is raised.  No acknowledgement.
   - **Fatal**: the producer is marked fatal and `KafkaException` propagates.
     No acknowledgement.

If `begin_transaction()` itself fails, `PublishFailedError` is raised
immediately.

If `BufferError` occurs during production, the transaction is aborted and
`PublishFailedError` is raised.

Transactional mode provides **Kafka-side atomicity**: `read_committed`
consumers observe each batch as all-or-nothing.  However, since the FDB
cursor acknowledgement is outside the Kafka transaction boundary, the
overall guarantee remains **at-least-once**.

## Failure handling

### Kafka delivery failure (idempotent)

`PublishFailedError` is raised.  The CDC batch is not acknowledged.  The
caller must close the listener and resume from the last persisted cursor.

### Transaction abort (transactional)

`PublishFailedError` is raised.  The CDC batch is not acknowledged.  The
listener's `_awaiting_ack` flag is still set, so the caller must close and
resume the listener to replay the batch.

### Fatal producer error

`KafkaException` propagates.  The producer is marked fatal via
`mark_fatal()`.  The caller must replace the producer and close/resume the
listener.

### FDB acknowledgement failure

`acknowledge()` can raise `CDCRetryableError` for transient conditions like
`server_overloaded`.  The pipeline retries acknowledgement up to
`ack_retries` times (default 3).  During retries, no new CDC batch is polled
and no Kafka records are republished.  If retries are exhausted,
`CDCRetryableError` propagates.

### Process crash

If the process crashes after Kafka delivery but before `acknowledge()`, FDB
replays the batch on the next session.  In idempotent mode, duplicates are
possible.  In transactional mode, the batch appears once per transaction
epoch for `read_committed` consumers.

## Partition ordering

All records from one `step()` call use the same `partition_key`.  The caller
should set this to the UTF-8-encoded stream name so all mutations for one
CDC stream land on the same Kafka partition, preserving per-stream ordering.

Kafka guarantees ordering within a partition, not globally across partitions.
Version markers use the same key as their associated mutation batches.

If no `partition_key` is supplied, `None` is passed to `produce()`, which
causes Kafka to distribute records across partitions (breaking per-stream
ordering).  This is permitted but not recommended.

## Caller responsibilities

After `PublishFailedError`:

1. Close the listener.
2. Resume from the last persisted cursor (the failed batch will replay).
3. Optionally replace the producer if it was marked fatal.

After `KafkaException` (fatal):

1. Replace the producer.
2. Close and resume the listener.

After `CDCRetryableError` (ack retries exhausted):

1. Close and resume the listener.
2. Accept that the batch may be replayed and duplicated in Kafka.

## Guarantees and limitations

| Guarantee | Status |
|-----------|--------|
| At-least-once delivery | ✓ Both modes |
| Per-partition ordering | ✓ When partition key is set |
| Kafka batch atomicity | ✓ Transactional mode only |
| No FDB ack before Kafka confirm | ✓ Enforced by code |
| End-to-end exactly-once | ✗ CDC ack is outside Kafka txn |
| Replay after crash | ✓ FDB replays unacked batch |
| Bounded ack retries | ✓ Configurable, default 3 |
