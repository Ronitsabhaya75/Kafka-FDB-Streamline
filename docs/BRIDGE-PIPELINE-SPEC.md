# Bridge pipeline specification

`src.bridge.BridgePipeline` is the single-step CDC-to-Kafka delivery loop.
Each call to `step()` processes one CDC reply end-to-end: poll, produce,
flush or commit, then acknowledge.

## Flow

```
FDB CDC → poll_records() → produce() × N → flush() / commit_transaction() → acknowledge()
```

The pipeline does not own the listener or producer lifecycle.  The caller
creates, starts, and closes them.

## Idempotent mode

```
produce all records
flush (timeout: 10 s)
if remaining > 0 → raise KafkaException, no ack
acknowledge FDB
```

Delivery errors from `produce()` or `flush()` propagate as the original
`KafkaException` or `BufferError`.  FDB is never acknowledged.

## Transactional mode

```
begin_transaction
produce all records
commit_transaction
acknowledge FDB
```

On any failure during produce or commit:

1. If the error is fatal (`error.fatal()`), mark the producer fatal and
   re-raise.  No abort is attempted.
2. Otherwise, abort the transaction and re-raise the original exception.
3. If the abort itself fails, mark the producer fatal and raise the abort
   failure chained to the original.

FDB is never acknowledged on failure.

## Failure handling

| Failure | FDB acked? | Producer state | Caller action |
|---------|-----------|----------------|---------------|
| `flush()` timeout (remaining > 0) | No | Usable | Close listener, resume |
| `KafkaException` from produce/flush | No | Check `fatal()` | Replace producer if fatal; close listener, resume |
| `BufferError` from produce | No | Usable (idempotent) / aborted (txn) | Close listener, resume |
| Fatal `KafkaException` | No | Fatal | Replace producer and listener |
| Abort-required / retriable commit | No | Aborted | Close listener, resume |
| Abort failure | No | Fatal | Replace producer and listener |
| `CDCRetryableError` during ack | After retries | Delivered | Close listener if retries exhausted |
| Process crash after commit | No | N/A | FDB replays; at-least-once duplicates |

## Partition ordering

All records from one `step()` carry the same `partition_key`, so they land
on a single Kafka partition in produce order.

## Acknowledgement retries

After successful Kafka delivery, `acknowledge()` is retried up to
`ack_retries` times on `CDCRetryableError`.  No records are re-produced
during retries, and no new poll is issued.

## Caller responsibilities

1. Create and start the listener and producer.
2. Call `step()` in a loop.
3. On any exception, close the listener and resume from the last cursor.
4. On fatal errors, also replace the producer.
5. Close both on shutdown.

## Guarantees and limitations

| Property | Status |
|----------|--------|
| At-least-once (both modes) | Yes |
| No ack before delivery | Yes |
| Kafka-side atomicity (txn mode) | Yes |
| Cross-system exactly-once | No — requires CDC cursor inside txn |
| Duplicate suppression | No — consumer-side dedup required |
