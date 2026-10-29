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
does not checkpoint or acknowledge. `close()` purges anything still queued,
so a later flush cannot publish the failed batch. Replace the producer,
close the listener, and resume from the last checkpoint.

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
| `fatal()` | `mark_fatal()`, no abort, no ack. Replace the producer. |
| `txn_requires_abort()`, or not retriable | `abort_transaction()`, no ack. |
| `retriable()` within the budget | `commit_transaction()` again. |
| `retriable()` after the budget | Leave the transaction open and raise. Call `step()` again. Do not `close()` the producer: that aborts a commit which may still be finishing. |

A produce failure aborts the open transaction unless the error is fatal.
FDB is not acknowledged.

## Checkpoint and acknowledgement

`checkpoint` receives the object returned by `get_position()` and must make
it durable. It runs only after Kafka delivery succeeds. If it raises, FDB is
not acknowledged.

`acknowledge()` is then retried up to `ack_retries` times on
`CDCRetryableError`, with `ack_retry_delay_seconds` between attempts. Nothing
is re-polled or re-produced.

If the checkpoint or the acknowledgement budget fails after delivery, the
next `step()` retries that phase only.

## Caller responsibilities

1. Create and start the listener and producer. Call `init_transactions()` once
   before using a transactional producer.
2. Pass a checkpoint that durably stores the CDC cursor.
3. Call `step()` in a loop.
4. After an idempotent delivery failure, replace the closed producer, close
   the listener, and resume from the last checkpoint.
5. After retriable-commit exhaustion, or a checkpoint/ack failure, call
   `step()` again. Do not close the producer while a commit is outstanding.
6. On a fatal producer error, replace the producer and close the listener.
7. Close both on shutdown.

## Guarantees and limitations

| Property | Status |
|----------|--------|
| At-least-once (both modes) | Yes |
| No ack before Kafka delivery and a successful checkpoint | Yes |
| One partition per step | Yes, via a non-empty stable key |
| Kafka-side atomicity (txn mode) | Yes, for `read_committed` consumers |
| Cross-system exactly-once | No. The CDC cursor is not inside the Kafka transaction. |
| Duplicate suppression | No. Consumers dedup on `(stream, version, sequence)`. |
