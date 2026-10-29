# Bridge pipeline specification

`src.bridge.BridgePipeline` delivers one CDC reply at a time: poll, produce,
flush or commit, checkpoint `get_position()`, then acknowledge.

```
FDB CDC → poll_records() → produce() × N → flush_or_raise() / commit_transaction()
        → checkpoint(get_position()) → acknowledge()
```

The pipeline does not own the listener or producer lifecycle. The caller
creates, starts, and closes them, and supplies the checkpoint callback.

## Partition key

Every record in a step uses one non-empty key, so the batch and its
`VersionEnd` land on a single Kafka partition. Pass `partition_key`, or omit
it and the pipeline uses the listener's `raw_stream_name` (else
`stream_name`). An empty or missing key is rejected at construction.
librdkafka's default partitioner spreads null keys across partitions.

## Idempotent mode

```
produce all records
flush_or_raise()
checkpoint(get_position())
acknowledge FDB
```

`BufferError`, `KafkaException`, or `TimeoutError` closes the producer and
does not checkpoint or acknowledge. `close()` flushes and then purges queued
messages. Because some messages may have reached the broker, downstream consumers
may see duplicate records upon replay, but at-least-once is preserved because
FDB is not acknowledged. Replace the producer, close the listener, and resume
from the last checkpoint.

## Transactional mode

```
begin_transaction
produce all records
commit_transaction   # retried while error.retriable() and not txn_requires_abort()
checkpoint(get_position())
acknowledge FDB
```

| Commit error | Action |
|--------------|--------|
| `fatal()` | `mark_fatal()`, no abort, no ack. Replace the producer and close the listener. |
| `txn_requires_abort()`, or not retriable | `abort_transaction()`, no ack. Close the listener and resume from checkpoint (producer is reusable). |
| `retriable()` within the budget | `commit_transaction()` again. |
| `retriable()` after the budget | Leave the transaction open and raise. Call `step()` again. Do not `close()` the producer: that aborts a commit which may still be finishing. |

A produce failure or non-fatal abort-required commit failure aborts the open
transaction unless the error is fatal. FDB is not acknowledged. After an abort,
the transactional producer remains reusable: close the listener and resume from
the last checkpoint with the existing producer.

## Checkpoint and acknowledgement

`checkpoint` receives the object returned by `get_position()` and must make
it durable. It runs only after Kafka delivery succeeds. If it raises, FDB is
not acknowledged.

`acknowledge()` is then retried up to `ack_retries` times on
`CDCRetryableError`, with `ack_retry_delay_seconds` between attempts. Nothing
is re-polled or re-produced.

If the checkpoint or the acknowledgement budget fails after delivery, the
next `step()` retries that phase only (`checkpoint` is not re-invoked on an
ack-only retry).

## Caller responsibilities

1. Create and start the listener and producer. Call `init_transactions()` once
   before using a transactional producer.
2. Pass a checkpoint callback that durably stores the CDC cursor.
3. Call `step()` in a loop.
4. After an idempotent delivery failure, replace the closed producer, close
   the listener, and resume from the last checkpoint.
5. After a transactional abort, close the listener and resume from the last
   checkpoint (the transactional producer is reusable; do not close it).
6. After retriable-commit exhaustion, or a checkpoint/ack failure, call
   `step()` again.
7. On shutdown: check `pipeline.commit_outstanding`. If true, continue calling
   `step()` until the commit succeeds or raises a non-retriable error; do not
   close the producer while a commit is in progress. Once no commit is
   outstanding, close the listener and producer.
8. On a fatal producer error, replace the producer and close the listener.


## Guarantees and limitations

| Property | Status |
|----------|--------|
| At-least-once (both modes) | Yes |
| No ack before Kafka delivery and a successful checkpoint | Yes |
| One partition per step | Yes, via a non-empty stable key |
| Kafka-side atomicity (txn mode) | Yes, for `read_committed` consumers |
| Cross-system exactly-once | No. The CDC cursor is not inside the Kafka transaction. |
| Duplicate suppression | No. Consumers dedup on `(stream, version, sequence)`. |
