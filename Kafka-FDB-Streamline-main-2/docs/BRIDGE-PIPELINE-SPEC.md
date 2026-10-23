# Bridge pipeline specification

`src.bridge.BridgePipeline` is the composition layer that wires the CDC
listener, serialization, and Kafka producer into a single step function.

## Ownership

The pipeline does **not** own the listener or producer lifecycle. The caller
creates, starts (and for transactions, calls `init_transactions`), and closes
them. The pipeline only drives one poll-produce-ack cycle per `step()` call.

## `step()` contract

One call to `step()` processes exactly one CDC reply end-to-end:

```
poll_records()          → raw mutations from FDB CDC
    │
produce() × N          → enqueue each record to Kafka
    │
flush() or commit()    → block until Kafka durably confirms
    │
acknowledge()          → advance CDC cursor
```

**Return value**: the number of records published. `0` means the poll returned
no data (the stream is idle). Any non-zero value means all records were
durably confirmed and the CDC cursor was advanced.

**Exception**: if any stage fails, `step()` raises. The CDC cursor is **not**
advanced, so the next `poll_records()` replays the same version group. The
caller should apply backoff before retrying.

## Modes

### Idempotent (non-transactional)

When the producer has no `transactional_id`:

1. Produce all records.
2. `flush()` — blocks until every record is broker-acknowledged. Raises on
   delivery failure **or** timeout.
3. `acknowledge()` — advance the CDC cursor.

Flush errors propagate to the caller. The producer is poisoned (sticky error),
so recovery requires a new producer instance.

### Transactional

When the producer has a `transactional_id`:

1. `begin_transaction()`
2. Produce all records inside the transaction.
3. `commit_transaction()` — atomically make all records visible.
4. `acknowledge()` — advance the CDC cursor.

On commit failure:

- **Abortable** (`txn_requires_abort()`): the pipeline aborts the transaction,
  then re-raises.
- **Retriable** (`retriable()`): the pipeline aborts the transaction, then
  re-raises. The caller should back off before retrying.
- **Fatal**: the pipeline re-raises immediately.

In all cases the CDC cursor is **not** advanced. The exception propagates to the
caller, who must decide whether to retry or halt.

## When is it safe to ack?

The pipeline acknowledges FDB **only** after:

- **Idempotent mode**: `flush()` returns without raising (all records
  broker-confirmed, zero remaining in queue).
- **Transactional mode**: `commit_transaction()` returns without raising.

If either fails, `acknowledge()` is never called. This is the at-least-once
invariant — the foundation of the entire bridge's correctness.

## Caller recovery

| Failure | What happened | How to recover |
|---------|---------------|----------------|
| `flush()` raises (delivery error) | Some records may be in Kafka; CDC cursor unchanged | Discard the poisoned producer, create a new one, call `step()` again — the listener replays the same version group |
| `flush()` raises (timeout) | Some records may be in Kafka; producer poisoned; CDC cursor unchanged | Same as delivery error |
| `commit_transaction()` raises (abortable) | Transaction aborted; no records visible; CDC cursor unchanged | Call `step()` again (the listener replays) |
| `commit_transaction()` raises (retriable) | Transaction aborted; no records visible; CDC cursor unchanged | Back off, then call `step()` again |
| `commit_transaction()` raises (fatal) | Producer unrecoverable | Discard the producer, create a new one, call `step()` again |
| `CDCTerminalError` (1007) | Mutation log GC'd past cursor; data permanently lost | Halt and trigger a full rebuild |
| `CDCRetryableError` (1211) | Storage server backpressured | Retry with bounded backoff; halt if the same version fails repeatedly (L1 poison pill) |

## Partition key

The `partition_key` constructor argument is the Kafka record key for every
produced message. It should be the UTF-8-encoded stream name so that all
mutations for a stream land on the same Kafka partition, preserving
commit-version order.

## Tests

Unit tests mock both the listener and producer and verify:

- Empty poll returns 0, no ack.
- Idempotent path: produce → flush → ack ordering.
- Flush failure prevents ack.
- Transactional path: begin → produce → commit → ack ordering.
- Abortable/retriable commit errors abort and re-raise.
- Fatal commit errors propagate without abort.
- Abort failure re-raises the original commit error.
- `None` partition key is forwarded correctly.
