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

### Caller responsibilities

`Bridge` follows these rules; a caller driving `BridgePipeline` directly must too.

1. Create and start the listener and producer. Call `init_transactions()` once before using a transactional producer.
2. Pass a checkpoint callback that durably stores the CDC cursor.
3. Call `step()` in a loop.
4. After an idempotent delivery failure, replace the closed producer, close the listener, and resume from the last checkpoint.
5. After a transactional abort, close the listener and resume from the last checkpoint (the transactional producer is reusable; do not close it).
6. After retriable-commit exhaustion, or a checkpoint/ack failure, call `step()` again.
7. On shutdown: check `pipeline.commit_outstanding`. If true, continue calling `step()` until the commit succeeds or raises a non-retriable error; do not close the producer while a commit is in progress. Once no commit is outstanding, close the listener and producer.
8. On a fatal producer error, replace the producer and close the listener.

---

## 2. Autonomous Daemon (`Bridge`)

`src.bridge.Bridge` orchestrates continuous streaming with declarative configuration:

### Configuration (`BridgeConfig`)

Parsed via `from_dict(data)`:
- `fdb`: `api_version` (default 800), `cluster_file` (optional path).
- `cdc`: `stream_name` (required; empty or whitespace-only is rejected), `begin_key` / `end_key` (paired hex strings or bytes, `begin_key < end_key`), `max_record_bytes`, `cursor_file` (optional path).
- `kafka`: `bootstrap_servers`, `topic`, `transactional_id` (optional non-empty string; setting it enables transactional mode).
- `pipeline`: `flush_timeout_seconds`, `backoff_base_seconds`, `backoff_max_seconds`, `shutdown_timeout_seconds`.

Unknown keys at any level raise `ValueError`, so a typo fails instead of silently falling back to a default.

### Producer wiring

`Bridge(config, *, client=None, listener=None, producer=None, producer_factory=None)`. The first pipeline uses the injected listener/producer if given. Every replacement producer comes from `producer_factory`, defaulting to `MutationProducer(kafka.bootstrap_servers, transactional_id=kafka.transactional_id)`.

- Transactional mode is enabled by `kafka.transactional_id`, or by injecting a transactional producer together with a `producer_factory` that builds transactional replacements. Injecting a transactional producer with neither raises `ValueError` at construction, since a rebuild would otherwise downgrade it to idempotent.
- `start()` calls `init_transactions()` on a transactional producer, so an injected one must not be pre-initialised.

### Cursor File Persistence

When `config.cdc.cursor_file` is specified, `Bridge` persists the cursor atomically and durably:
1. Writes JSON containing `{"stream_id": <int>, "last_consumed_version": <int>, "stream_name": <str>}` to a `.tmp` file.
2. Calls `f.flush()` and `os.fsync(f.fileno())` to sync data blocks to disk.
3. Performs atomic rename via `os.replace(tmp_path, cursor_file)`.
4. Fsyncs the parent directory to guarantee the directory entry update is durable. Only `EINVAL`/`ENOTSUP`/`EOPNOTSUPP` (filesystems that cannot fsync a directory) are tolerated.
5. Propagates any other `OSError`, a `None` position, or invalid coordinates (`ValueError`) so FoundationDB is not acknowledged unless the checkpoint is durable.

On start and on every rebuild, if `cursor_file` exists it is parsed strictly. Invalid JSON, missing or non-integer coordinates, or a `stream_name` other than `cdc.stream_name` (including a file with no `stream_name`) raise `ValueError` rather than silently resuming from a fresh or foreign cursor.

### Error Classification and Recovery Loop

In `Bridge.run()`:
- **Terminal errors** (`CDCTerminalError`, fatal Kafka errors via `is_fatal_kafka_error`): sets shutdown, logs with `exc_info=True`, and re-raises.
- **Permanent errors** (a `ValueError` that is not a `SerializationError`, e.g. invalid checkpoint coordinates or a corrupt or foreign cursor file on rebuild): sets shutdown and re-raises rather than retrying forever.
- **Retry in place** (e.g. `CDCRetryableError` from poll, checkpoint or ack failure, retriable-commit exhaustion): backs off and calls `step()` again on the same pipeline.
- **Rebuild** when the failure left state only a resume from checkpoint clears: producer `closed` or `fatal`, listener `delivery_failed` (`SerializationError`/`RecordTooLargeError`), or listener `awaiting_ack` after a non-fatal transactional abort (no `commit_outstanding`). `_rebuild_pipeline()`:
  1. Does nothing while `commit_outstanding` is set (rule 7); the next `step()` retries the commit.
  2. Closes the old producer and listener.
  3. Reloads the last durable cursor from `cdc.cursor_file`.
  4. Starts a new `FDBMutationListener` from that cursor and builds a producer from `producer_factory` (calling `init_transactions()` if transactional).
  5. Binds a new `BridgePipeline`.

  A failed rebuild closes whatever it built, leaves the listener, producer and pipeline unset, and raises. `run()` backs off and retries the rebuild. It never exits silently because no pipeline is bound; it returns normally only after shutdown is requested.

A `RecordTooLargeError` caused by the data itself recurs after every rebuild, so the bridge retries it with backoff indefinitely.

### Thread Safety and Teardown

`Bridge.stop()`:
1. Calls `request_shutdown()` and waits up to `shutdown_timeout_seconds` for the step lock. If a step is still running (typically parked in `consume()` on an idle stream), it closes the listener to unblock it and waits once more, then proceeds regardless.
2. Only if it holds the step lock, and while `commit_outstanding`, calls `step()` until the commit resolves, an exception is raised, or `shutdown_timeout_seconds` elapses (rule 7). Closing the producer after that is logged as an error, because it aborts a commit that may still complete. FDB was not acknowledged, so the replay duplicates rather than loses data.
3. Attempts every close (producer, listener, FDB client) and unsets all handles, then re-raises the first close error.

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
