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

A produce failure or non-fatal abort-required commit failure aborts the open transaction unless the error is fatal. FDB is not acknowledged. After a successful abort the transactional producer may be reused: close the listener and resume from the last checkpoint. If `abort_transaction()` itself raises, the abort error is raised, `producer.fatal` is true (`fatal_cause` is `FatalCause.ABORT_FAILED`, or `FatalCause.KAFKA` if the abort error was itself a fatal Kafka error), and the producer must be replaced. What makes a producer fatal is owned by the [Kafka producer spec](KAFKA-PRODUCER-SPEC.md#invariants).

### Checkpoint and acknowledgement

`checkpoint` receives the object returned by `get_position()` and must make it durable. It runs only after Kafka delivery succeeds. If it raises, FDB is not acknowledged.

`acknowledge()` is then attempted up to `ack_retries` times in total (not `ack_retries` retries) while it raises `CDCRetryableError`, with `ack_retry_delay_seconds` between attempts. `commit_retries` is likewise a total attempt count for a retriable commit. Nothing is re-polled or re-produced.

If the checkpoint fails, or every acknowledgement attempt raises `CDCRetryableError`, the next `step()` retries that phase only (`checkpoint` is not re-invoked on an ack-only retry). A `CDCTerminalError` from `acknowledge()` propagates at once; the batch stays delivered and checkpointed but unacknowledged, and the caller must not call `step()` again (see [CDC errors](CDC-CONSUMER-SPEC.md#err-cdc-terminal)). Resume from the checkpoint instead.

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
- Rule 8: when the producer went fatal from a librdkafka fatal error (`FatalCause.KAFKA`), `Bridge` shuts down rather than replacing it. Only `FatalCause.ABORT_FAILED` is replaced.

---

## 2. Autonomous Daemon (`Bridge`)

`fdb_kafka_bridge.bridge.Bridge` orchestrates continuous streaming with declarative configuration:

### Configuration (`BridgeConfig`)

Parsed via `from_dict(data)`:
- `fdb`: `api_version` (default 800), `cluster_file` (optional path). Any positive integer is accepted, but a version below 800 fails later at `FDBClient.open()` (`CDCNotSupportedError`).
- `cdc`: `stream_name` (required; empty or whitespace-only is rejected), `begin_key` / `end_key` (paired hex strings or bytes, `begin_key < end_key`; when set, `start()` registers the stream over that range, otherwise it consumes an existing stream), `max_record_bytes` (default leaves room for a stream-name key of up to 964 UTF-8 bytes; see [Record size](KAFKA-PRODUCER-SPEC.md#record-size)), `cursor_file` (optional path).
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
- **Terminal errors** (`CDCTerminalError`, fatal Kafka errors via `is_fatal_kafka_error`, or any other error once `producer.fatal_cause is FatalCause.KAFKA`): sets shutdown, logs with `exc_info=True`, and re-raises. A librdkafka fatal can surface only through `error_cb`, behind a non-fatal exception; rebuilding would reuse the `transactional_id` and could fence a live peer.
- **Permanent errors** (a `ValueError` that is not a `SerializationError`, e.g. invalid checkpoint coordinates or a corrupt or foreign cursor file on rebuild): sets shutdown and re-raises rather than retrying forever.
- **Retry in place** (e.g. `CDCRetryableError` from poll, checkpoint or ack failure, retriable-commit exhaustion): backs off and calls `step()` again on the same pipeline.
- **Rebuild** when the failure left state only a resume from checkpoint clears: producer `closed`, producer `fatal` with `FatalCause.ABORT_FAILED`, listener `delivery_failed` (`SerializationError`/`RecordTooLargeError`), or a `KafkaException`/`BufferError` while the listener is `awaiting_ack` and no commit is outstanding (a non-fatal abort, or a non-fatal `begin_transaction()` failure). `_rebuild_pipeline()`:
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

---

## Invariants

Rules owned by other components are linked, not restated: CDC error classes and the ack contract in the [CDC consumer spec](CDC-CONSUMER-SPEC.md#invariants), producer fatal and closed states in the [Kafka producer spec](KAFKA-PRODUCER-SPEC.md#invariants).

- <a id="inv-bridge-1"></a>**INV-BRIDGE-1.** `acknowledge()` is never called before the batch is delivered to Kafka (flush or commit) and the checkpoint has succeeded. Enforced by: `pipeline.BridgePipeline._finish_delivery`, `pipeline.BridgePipeline._checkpoint_and_acknowledge`. Pinned by: `tests/bridge/test_pipeline.py::TestIdempotentPath::test_delivery_order`, `tests/bridge/test_pipeline.py::TestTransactionalPath::test_wraps_in_transaction_then_checkpoints`, `tests/bridge/test_pipeline.py::TestCheckpoint::test_checkpoint_failure_skips_ack_and_retries_without_republish`.
- <a id="inv-bridge-2"></a>**INV-BRIDGE-2.** While a batch is delivered but unacknowledged, or a commit is outstanding, `step()` neither polls nor produces; it only finishes that batch. Enforced by: `pipeline.BridgePipeline.step`. Pinned by: `tests/bridge/test_pipeline.py::TestAckRetry::test_retry_budget_exhausted_then_step_retries_ack_only`, `tests/bridge/test_pipeline.py::TestTransactionalPath::test_exhausted_retriable_commit_retries_on_next_step`.
- <a id="inv-bridge-3"></a>**INV-BRIDGE-3.** A checkpoint that succeeded is not re-invoked on an ack-only retry. Enforced by: `pipeline.BridgePipeline._checkpoint_and_acknowledge`. Pinned by: `tests/bridge/test_pipeline.py::TestAckRetry::test_retry_budget_exhausted_then_step_retries_ack_only`.
- <a id="inv-bridge-4"></a>**INV-BRIDGE-4.** A retriable commit failure is never aborted; after the budget the transaction stays open across `step()` calls. Enforced by: `pipeline.BridgePipeline._commit_with_retry`. Pinned by: `tests/bridge/test_pipeline.py::TestTransactionalPath::test_retriable_commit_is_retried_then_succeeds`, `tests/bridge/test_pipeline.py::TestTransactionalPath::test_exhausted_retriable_commit_retries_on_next_step`.
- <a id="inv-bridge-5"></a>**INV-BRIDGE-5.** `Bridge` never closes the producer while `commit_outstanding`, except `stop()` once its bounded commit retry gives up (logged as an error; FDB is unacked, so the batch replays). Enforced by: `pipeline.Bridge._rebuild_pipeline`, `pipeline.Bridge._finish_outstanding_commit`. Pinned by: `tests/bridge/test_bridge_lifecycle.py::test_rebuild_skipped_while_commit_outstanding`, `tests/bridge/test_bridge_lifecycle.py::test_stop_finishes_outstanding_commit_before_closing`.
- <a id="inv-bridge-6"></a>**INV-BRIDGE-6.** `stop()` calls `step()` only while holding `_step_lock`. Enforced by: `pipeline.Bridge.stop`. Pinned by: `tests/bridge/test_bridge_lifecycle.py::test_stop_skips_commit_retry_while_step_still_running`.
- <a id="inv-bridge-7"></a>**INV-BRIDGE-7.** A fatal Kafka error is never followed by `abort_transaction()`; the producer is marked fatal instead. Enforced by: `pipeline.BridgePipeline._abort_and_raise`, `pipeline.BridgePipeline._commit_with_retry`. Pinned by: `tests/bridge/test_pipeline.py::TestTransactionalPath::test_fatal_error_propagates_and_marks_fatal`, `tests/bridge/test_pipeline.py::TestTransactionalPath::test_fatal_produce_does_not_abort`.
- <a id="inv-bridge-8"></a>**INV-BRIDGE-8.** A producer that went fatal only because `abort_transaction()` failed (`FatalCause.ABORT_FAILED`) is replaced by a rebuild. Enforced by: `pipeline.Bridge._needs_rebuild`. Pinned by: `tests/bridge/test_bridge.py::test_run_rebuilds_when_abort_failed`.
- <a id="inv-bridge-9"></a>**INV-BRIDGE-9.** A librdkafka fatal error, whether raised or seen as `fatal_cause is FatalCause.KAFKA`, shuts `Bridge` down and is never rebuilt, so a fenced instance cannot re-fence its peer with the same `transactional_id`. Enforced by: `pipeline.Bridge.run`. Pinned by: `tests/bridge/test_bridge.py::test_run_reraises_fatal_kafka_error`, `tests/bridge/test_bridge.py::test_run_shuts_down_when_producer_went_fatal_from_kafka`.
- <a id="inv-bridge-10"></a>**INV-BRIDGE-10.** Every record in a step carries the same non-empty key; `Bridge` uses `cdc.stream_name` as UTF-8 bytes. Enforced by: `pipeline._resolve_partition_key`, `pipeline.Bridge._build_pipeline`. Pinned by: `tests/bridge/test_pipeline.py::TestPartitionKey::test_stable_key_for_stream`, `tests/bridge/test_pipeline.py::TestPartitionKey::test_empty_key_rejected`, `tests/bridge/test_bridge.py::test_bridge_keys_records_by_configured_stream_name`.
- <a id="inv-bridge-11"></a>**INV-BRIDGE-11.** A rebuild never downgrades a transactional producer to idempotent. Enforced by: `pipeline.Bridge.__init__`, `pipeline.Bridge._producer_from_config`. Pinned by: `tests/bridge/test_bridge_lifecycle.py::test_rebuild_keeps_transactional_mode_from_config`, `tests/bridge/test_bridge_lifecycle.py::test_injected_transactional_producer_without_rebuild_source_rejected`.
- <a id="inv-bridge-12"></a>**INV-BRIDGE-12.** The cursor file is durable (tmp fsync, `os.replace`, directory fsync) before the ack; any other failure blocks the ack. Enforced by: `pipeline._save_cursor_to_file`. Pinned by: `tests/bridge/test_cursor_file.py::test_save_dir_sync_failure_propagates`, `tests/bridge/test_bridge.py::test_checkpoint_persistence_failure_blocks_ack`.
- <a id="inv-bridge-13"></a>**INV-BRIDGE-13.** A corrupt or foreign cursor file is permanent: `run()` shuts down instead of retrying or resuming from it. Enforced by: `pipeline._load_cursor_from_file`, `pipeline.Bridge.run`. Pinned by: `tests/bridge/test_cursor_file.py::test_load_stream_name_mismatch_raises`, `tests/bridge/test_bridge.py::test_run_shuts_down_on_foreign_cursor_file_during_rebuild`.
- <a id="inv-bridge-14"></a>**INV-BRIDGE-14.** Without `cursor_file` the checkpoint is a no-op and resume comes from the cluster ack frontier. Enforced by: `pipeline._save_cursor_to_file`. Pinned by: `tests/bridge/test_cursor_file.py::test_save_without_path_is_noop` (no-op only; the resume point is FDB-side and **Unpinned**).
- <a id="inv-bridge-15"></a>**INV-BRIDGE-15.** Every Kafka failure leaves FDB unacknowledged, so the batch replays: duplicates, never loss. Enforced by: `pipeline.BridgePipeline._produce_and_flush`, `pipeline.BridgePipeline._commit_with_retry`, `pipeline.BridgePipeline._abort_and_raise`. Pinned by: `tests/bridge/test_pipeline.py::TestIdempotentPath::test_flush_exception_closes_producer_and_skips_ack`, `tests/bridge/test_pipeline.py::TestTransactionalPath::test_abortable_error_aborts_no_ack`.
- <a id="inv-bridge-16"></a>**INV-BRIDGE-16.** `run()` never returns while no pipeline is bound and shutdown is not requested; a failed rebuild is retried. Enforced by: `pipeline.Bridge.run`, `pipeline.Bridge._rebuild_pipeline`. Pinned by: `tests/bridge/test_bridge.py::test_run_retries_failed_rebuild_until_it_succeeds`.
- <a id="inv-bridge-17"></a>**INV-BRIDGE-17.** `CDCTerminalError` is never retried: `BridgePipeline` propagates it without another attempt and `Bridge.run()` shuts down. What the class means is owned by the [CDC consumer spec](CDC-CONSUMER-SPEC.md#invariants). Enforced by: `pipeline.BridgePipeline._acknowledge_with_retry`, `pipeline.Bridge.run`. Pinned by: `tests/bridge/test_pipeline.py::TestAckRetry::test_terminal_ack_error_propagates_without_retry`, `tests/bridge/test_bridge.py::test_bridge_terminates_on_cdc_terminal_error`.
