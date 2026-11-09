# Bridge pipeline specification

The bridge module provides two complementary layers:
1. `fdb_kafka_bridge.bridge.BridgePipeline`: A single-step coordinator that processes one CDC reply: poll, produce, flush or commit, checkpoint `get_position()`, then acknowledge.
2. `fdb_kafka_bridge.bridge.Bridge`: An autonomous streaming daemon that manages connection lifecycles, background execution, error recovery, cursor persistence, and metrics collection.

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

`BufferError`, `KafkaException`, `TimeoutError`, or `RuntimeError` (producer already closed or fatal) closes the producer and does not checkpoint or acknowledge. `close()` flushes and then purges queued messages. Because some messages may have reached the broker, downstream consumers may see duplicate records upon replay, but at-least-once is preserved because FDB is not acknowledged. The caller must replace the producer, close the listener, and resume from the last checkpoint.

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
| `txn_requires_abort()`, not retriable, or no `KafkaError` attached | `abort_transaction()`, no ack. Close the listener and resume from checkpoint. The producer is reusable unless the abort failed (see below). |
| `retriable()` within the budget | `commit_transaction()` again. |
| `retriable()` after the budget | Leave the transaction open and raise. Call `step()` again. Do not `close()` the producer: that aborts a commit which may still be finishing. |

A produce failure or non-fatal abort-required commit failure aborts the open transaction unless the error is fatal. FDB is not acknowledged. After a successful abort the transactional producer may be reused: close the listener and resume from the last checkpoint. If `abort_transaction()` itself raises, the abort error is raised, `producer.fatal` is true, and the producer must be replaced. What makes a producer fatal is owned by the [Kafka producer spec](KAFKA-PRODUCER-SPEC.md#fatal-and-closed-states).

### Checkpoint and acknowledgement

`checkpoint` receives the object returned by `get_position()` and must make it durable. It runs only after Kafka delivery succeeds. If it raises, FDB is not acknowledged.

`acknowledge()` is then attempted up to `ack_retries` times in total (not `ack_retries` retries) while it raises `CDCRetryableError`, with `ack_retry_delay_seconds` between attempts. `commit_retries` is likewise a total attempt count for a retriable commit. Nothing is re-polled or re-produced.

While a batch is delivered but unacknowledged, or a commit is outstanding, `step()` neither polls nor produces; it only finishes that batch. If the checkpoint fails, or every acknowledgement attempt raises `CDCRetryableError`, the next `step()` retries that phase only (`checkpoint` is not re-invoked on an ack-only retry). A `CDCTerminalError` from `acknowledge()` propagates at once; the batch stays delivered and checkpointed but unacknowledged, and the caller must not call `step()` again (see [CDC errors](CDC-CONSUMER-SPEC.md#err-cdc-terminal)). Resume from the checkpoint instead.

### Caller responsibilities

A caller driving `BridgePipeline` directly must follow these rules. `Bridge` follows them except where noted under [Deviations](#deviations-in-bridge).

1. Create and start the listener and producer. Call `init_transactions()` once before using a transactional producer.
2. Pass a checkpoint callback that durably stores the CDC cursor.
3. Call `step()` in a loop.
4. After an idempotent delivery failure, replace the closed producer, close the listener, and resume from the last checkpoint.
5. After a transactional abort, close the listener and resume from the last checkpoint. The transactional producer may be reused unless `producer.fatal` is true (the abort itself failed), in which case replace it.
6. After retriable-commit exhaustion, a checkpoint failure, or a `CDCRetryableError` from `acknowledge()`, call `step()` again. After a `CDCTerminalError` stop calling `step()`, close the listener, and resume from the last checkpoint ([CDC errors](CDC-CONSUMER-SPEC.md#err-cdc-terminal)). Whether to retry `client_invalid_operation` (2000) once, as the CDC spec allows within ~5 s of a crash, is the caller's decision.
7. On shutdown: check `pipeline.commit_outstanding`. If true, call `step()` again while it stays true; do not close the producer while a commit is in progress. Once no commit is outstanding, close the listener and producer.
8. On a fatal producer error, replace the producer and close the listener.

#### Deviations in `Bridge`

- Rule 5: `Bridge` never reuses the producer after an abort. A rebuild always closes and replaces it.
- Rule 6: `Bridge` treats 2000 as terminal everywhere and does not implement retry-once, so a restart within ~5 s of a crash can fail `start()` while the predecessor's lease is held.
- Rule 7: `Bridge.stop()` gives up on the first exception of any kind, or after `shutdown_timeout_seconds`, then closes the producer. Each `step()` already spends the full commit budget.
- Rule 8: a fatal Kafka error raised by `step()` shuts `Bridge` down rather than replacing the producer. A producer that is `fatal` without one being raised (a failed `abort_transaction()`, or a librdkafka fatal seen only by `error_cb`) is replaced by a rebuild.

---

## 2. Autonomous Daemon (`Bridge`)

`fdb_kafka_bridge.bridge.Bridge` orchestrates continuous streaming with declarative configuration:

### Configuration (`BridgeConfig`)

Parsed via `from_dict(data)`:
- `fdb`: `api_version` (default 800), `cluster_file` (optional path). Any positive integer is accepted, but a version below 800 fails later at `FDBClient.open()` (`CDCNotSupportedError`).
- `cdc`: `stream_name` (required; empty or whitespace-only is rejected), `begin_key` / `end_key` (paired hex strings or bytes, `begin_key < end_key`; when set, `start()` registers the stream over that range, otherwise it consumes an existing stream), `max_record_bytes` (default 1,000,000; see [Record size](KAFKA-PRODUCER-SPEC.md#record-size)), `cursor_file` (optional path).
- `kafka`: `bootstrap_servers` (default `localhost:9092`), `topic` (default `fdb-cdc`), `transactional_id` (optional non-empty string; setting it enables transactional mode).
- `pipeline`: `flush_timeout_seconds`, `backoff_base_seconds`, `backoff_max_seconds`, `shutdown_timeout_seconds`.

Unknown keys at any level raise `ValueError`, so a typo fails instead of silently falling back to a default.

### Producer wiring

`Bridge(config, *, client=None, listener=None, producer=None, producer_factory=None)`. The first pipeline uses the injected listener/producer if given; an injected listener is used as-is, so `cursor_file` is not read on that first start. Every record is keyed by `cdc.stream_name` encoded as UTF-8. Every replacement producer comes from `producer_factory`, defaulting to `MutationProducer(kafka.bootstrap_servers, transactional_id=kafka.transactional_id)`.

- Transactional mode is enabled by `kafka.transactional_id`, or by injecting a transactional producer together with a `producer_factory` that builds transactional replacements. Injecting a transactional producer with neither raises `ValueError` at construction, since a rebuild would otherwise downgrade it to idempotent.
- `start()` calls `init_transactions()` on a transactional producer, so an injected one must not be pre-initialised.

### Cursor File Persistence

Without `cursor_file` the checkpoint is a no-op, and every start or rebuild creates a fresh consumer that resumes from the cluster ack frontier (last acknowledged version + 1, [CDC deep dive §1.3](CDC-DEEP-DIVE.md#13-acknowledgement-semantics-)). Safety then rests on the ack ordering alone.

When `config.cdc.cursor_file` is specified, `Bridge` persists the cursor atomically and durably:
1. Writes JSON containing `{"stream_id": <int>, "last_consumed_version": <int>, "stream_name": <str>}` to a `.tmp` file.
2. Calls `f.flush()` and `os.fsync(f.fileno())` to sync data blocks to disk.
3. Performs atomic rename via `os.replace(tmp_path, cursor_file)`.
4. Fsyncs the parent directory to guarantee the directory entry update is durable. Only `EINVAL`/`ENOTSUP`/`EOPNOTSUPP` (filesystems that cannot fsync a directory) are tolerated.
5. Propagates any other `OSError`, a `None` position, or invalid coordinates (`ValueError`) so FoundationDB is not acknowledged unless the checkpoint is durable.

On start (unless a listener was injected) and on every rebuild, if `cursor_file` exists it is parsed strictly. Invalid JSON, missing or non-integer coordinates, or a `stream_name` other than `cdc.stream_name` (including a file with no `stream_name`) raise `ValueError` rather than silently resuming from a fresh or foreign cursor.

### Error Classification and Recovery Loop

In `Bridge.run()`:
- **Terminal errors** (`CDCTerminalError`, fatal Kafka errors via `is_fatal_kafka_error`): sets shutdown, logs with `exc_info=True`, and re-raises.
- **Permanent errors** (a `ValueError` that is not a `SerializationError`, e.g. invalid checkpoint coordinates or a corrupt or foreign cursor file on rebuild): sets shutdown and re-raises rather than retrying forever.
- **Retry in place** (e.g. `CDCRetryableError` from poll, checkpoint or ack failure, retriable-commit exhaustion): backs off and calls `step()` again on the same pipeline.
- **Rebuild** when the failure left state only a resume from checkpoint clears: producer `closed` or `fatal`, listener `delivery_failed` (`SerializationError`/`RecordTooLargeError`), or a `KafkaException`/`BufferError` while the listener is `awaiting_ack` and no commit is outstanding (a non-fatal abort, or a non-fatal `begin_transaction()` failure). `_rebuild_pipeline()`:
  1. Does nothing while `commit_outstanding` is set (rule 7); the next `step()` retries the commit.
  2. Closes the old producer and listener.
  3. Reloads the last durable cursor from `cdc.cursor_file`.
  4. Starts a new `FDBMutationListener` from that cursor and builds a producer from `producer_factory` (calling `init_transactions()` if transactional).
  5. Binds a new `BridgePipeline`.

  A failed rebuild closes whatever it built, leaves the listener, producer and pipeline unset, and raises. `run()` backs off and retries the rebuild. It never exits silently because no pipeline is bound; it returns normally only after shutdown is requested.

Known liveness limitations, both of which pin TLog retention while they last:

- A `RecordTooLargeError` caused by the data itself recurs after every rebuild, so the bridge retries it with backoff indefinitely.
- `CDCRetryableError` is retried forever with capped backoff. The repeated-at-one-version poison pill that the [CDC spec](CDC-CONSUMER-SPEC.md#err-cdc-retryable) says to escalate is never detected or escalated.

### Thread Safety and Teardown

`Bridge.stop()`:
1. Calls `request_shutdown()` and waits up to `shutdown_timeout_seconds` for the step lock. If a step is still running (typically parked in `consume()` on an idle stream), it closes the listener to unblock it and waits once more, then proceeds regardless.
2. Only if it holds the step lock, and while `commit_outstanding`, calls `step()` until the commit resolves, `step()` raises (any exception ends the attempt, since each `step()` already spends the full commit budget), or `shutdown_timeout_seconds` elapses (rule 7). Closing the producer after that is logged as an error, because it aborts a commit that may still complete. FDB was not acknowledged, so the replay duplicates rather than loses data.
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

