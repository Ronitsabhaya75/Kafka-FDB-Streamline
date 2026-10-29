# Bridge pipeline specification

The bridge module provides two complementary layers:
1. `src.bridge.BridgePipeline`: A single-step coordinator that processes one CDC reply: poll, produce, flush or commit, checkpoint `get_position()`, then acknowledge.
2. `src.bridge.Bridge`: An autonomous streaming daemon that manages connection lifecycles, background execution, error recovery, cursor persistence, and metrics collection.

```
FDB CDC → poll_records() → produce() × N → flush_or_raise() / commit_transaction()
        → checkpoint(get_position()) → acknowledge()
```

---

## 1. Single-Step Pipeline (`BridgePipeline`)

The single-step pipeline does not own the listener or producer lifecycle. The caller creates, starts, and closes them, and supplies the checkpoint callback.

### Partition key

Every record in a step uses one non-empty key, so the batch and its `VersionEnd` land on a single Kafka partition. Pass `partition_key`, or omit it and the pipeline uses the listener's `raw_stream_name` (else `stream_name`). An empty or missing key is rejected at construction. librdkafka's default partitioner spreads null keys across partitions.

### Idempotent mode

```
produce all records
flush_or_raise(flush_timeout_seconds)
checkpoint(get_position())
acknowledge FDB
```

`BufferError`, `KafkaException`, or `TimeoutError` closes the producer and does not checkpoint or acknowledge. `close()` flushes and then purges queued messages. Because some messages may have reached the broker, downstream consumers may see duplicate records upon replay, but at-least-once is preserved because FDB is not acknowledged. The caller must replace the producer, close the listener, and resume from the last checkpoint.

### Transactional mode

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

A produce failure or non-fatal abort-required commit failure aborts the open transaction unless the error is fatal. FDB is not acknowledged. After an abort, the transactional producer remains reusable: close the listener and resume from the last checkpoint with the existing producer.

### Checkpoint and acknowledgement

`checkpoint` receives the object returned by `get_position()` and must make it durable. It runs only after Kafka delivery succeeds. If it raises, FDB is not acknowledged.

`acknowledge()` is then retried up to `ack_retries` times on `CDCRetryableError`, with `ack_retry_delay_seconds` between attempts. Nothing is re-polled or re-produced.

If the checkpoint or the acknowledgement budget fails after delivery, the next `step()` retries that phase only (`checkpoint` is not re-invoked on an ack-only retry).

---

## 2. Autonomous Daemon (`Bridge`)

`src.bridge.Bridge` orchestrates continuous streaming with declarative configuration:

### Configuration (`BridgeConfig`)

Parsed via `from_dict(data)`:
- `fdb`: `api_version` (default 800), `cluster_file` (optional path).
- `cdc`: `stream_name` (required non-empty), `begin_key` / `end_key` (paired hex strings or bytes), `max_record_bytes`, `cursor_file` (optional path).
- `kafka`: `bootstrap_servers`, `topic`.
- `pipeline`: `flush_timeout_seconds`, `backoff_base_seconds`, `backoff_max_seconds`, `shutdown_timeout_seconds`.

### Cursor File Persistence

When `config.cdc.cursor_file` is specified, `Bridge` persists the cursor atomically and durably:
1. Writes JSON containing `{"stream_id": <int>, "last_consumed_version": <int>}` to a `.tmp` file.
2. Calls `f.flush()` and `os.fsync(f.fileno())` to sync data blocks to disk.
3. Performs atomic rename via `os.replace(tmp_path, cursor_file)`.
4. Fsyncs the parent directory to guarantee the directory entry update is durable.
5. Propagates any `OSError` or coordinate `ValueError` directly to prevent acknowledging FoundationDB if the checkpoint cannot be written.

On startup, if `cursor_file` exists, it is parsed strictly; missing fields or invalid JSON raise `ValueError` to prevent silently running from a fresh consumer.

### Error Classification and Recovery Loop

In `Bridge.run()`:
- **Terminal Errors** (`CDCTerminalError`, fatal Kafka errors via `is_fatal_kafka_error`): Sets shutdown, logs with `exc_info=True`, and re-raises immediately.
- **Recoverable Step Failures** (`CDCRetryableError` during acks): Retried on the active pipeline.
- **Handle Invalidation Failures** (`BufferError`, flush `TimeoutError`, non-fatal `KafkaException` closing the idempotent producer):
  1. Safely closes the existing invalid producer and listener.
  2. Reloads the last durable cursor from `config.cdc.cursor_file`.
  3. Instantiates and starts a new `FDBMutationListener` from that cursor.
  4. Instantiates a new `MutationProducer` (and calls `init_transactions()` if transactional).
  5. Binds a new `BridgePipeline` and continues streaming without dropping data.

### Thread Safety and Teardown

`Bridge.stop()` sets `request_shutdown()`, acquires an internal step lock to ensure no in-flight delivery is interrupted, and safely closes the producer, listener, and FDB client in order.

---

## Guarantees and limitations

| Property | Status |
|----------|--------|
| At-least-once (both modes) | Yes |
| No ack before Kafka delivery and a successful checkpoint | Yes |
| One partition per step | Yes, via a non-empty stable key |
| Kafka-side atomicity (txn mode) | Yes, for `read_committed` consumers |
| Cross-system exactly-once | No. The CDC cursor is not inside the Kafka transaction. |
| Duplicate suppression | No. Consumers dedup on `(stream, version, sequence)`. |
