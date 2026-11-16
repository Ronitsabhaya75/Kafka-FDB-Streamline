# FDB checkpoints and production CLI

Contract for `fdb_kafka_bridge.cdc.checkpoint`, `bridge.startup`, and `bridge.cli`.
Related: [bridge state machine](BRIDGE-PIPELINE-SPEC.md),
[CDC listener](CDC-CONSUMER-SPEC.md), [producer](KAFKA-PRODUCER-SPEC.md).
Trello: [FoundationDB Commit Version Checkpointing](https://trello.com/c/QmqdoJy5)
(card #59; daemon card #61 is only partially covered).

## Scope and card alignment

This change covers card #59's FDB checkpoint backend and sink validation, building on
the existing listener-to-Kafka pipeline from card #29. It does not complete card #61.
The signal-aware `streamline-daemon` / `python -m fdb_kafka_bridge.bridge` entrypoints
are included; JSON `--config`, `--checkpoint-backend`, an observability listen address,
and the package-root `python -m fdb_kafka_bridge` entrypoint remain daemon follow-ups.

A shared file/FDB checkpoint-store interface is deferred. `cdc.cursor_file` retains its
existing helpers for local/development use; it does not provide the FDB backend's sink
validation or ownership fence. Use `cdc.checkpoint_path` for the durable daemon. No
automatic migration between file and FDB progress is provided.

## Native client minimum

The durable bridge requires an API-800 native `libfdb_c` build containing
[FoundationDB #14351](https://github.com/apple/foundationdb/pull/14351), which fixes
an `onMainThread` completion-lifetime race during client-thread future release.
For `release-8.0`, the minimum source revision is
[`edc84bc37975fa83a3665bcd28b500838654b108`](https://github.com/apple/foundationdb/commit/edc84bc37975fa83a3665bcd28b500838654b108),
or a descendant/backport containing that fix. The upstream mainline fix is
`1ac38607cf0181f5ffd27b494f93e305fd84f4c9`.

The unpatched **8.0.0 release binaries do not meet this requirement**. The repository's
Dockerfile still pins those binaries as a development/CI fixture; it is not a supported
production image for the durable bridge. Supply a patched native client before deploying
this daemon. The Python `foundationdb` package version alone does not identify whether
`libfdb_c` contains the fix, and installing/upgrading that package does not patch the
native library. Verify the source revision or vendor backport provenance of the actual
client artifact, retaining its checksum with the deployment. No runtime check currently
verifies this provenance. Passing bridge or cancellation tests does not prove an
unpatched client is free of the race.

## Delivery boundary

The FDB backend extends the existing `Bridge` and `BridgePipeline`. Both producer modes
retain their existing ordering:

```text
poll → publish all envelopes → flush / commit succeeds → save full cursor → acknowledge
```

The listener already serializes native version groups into bounded Protobuf batches,
followed immediately by a `VersionEnd`. The bridge forwards these unchanged, including
idle watermark records. All records use the same non-empty stream-name key. Keep topic
partition count and partitioner stable, and use `cleanup.policy=delete`; compacting on
this key would remove earlier records of the stream.

Delivery remains at least once. A crash after Kafka delivery but before the checkpoint
can replay a partial or complete version. Consumers buffer through `VersionEnd` and
ignore versions at or below the last completely applied version for the same stream
name. Records do not carry the registration ID; re-registering a name requires an
explicit downstream replay/reset plan. Kafka transactions add atomic visibility for `read_committed` consumers;
they do not atomically commit the FDB cursor.

## Backend selection

The library keeps its existing `cdc.cursor_file` behavior. Configure
`cdc.checkpoint_path` to enable the FDB backend instead:

```python
from fdb_kafka_bridge.bridge import Bridge, from_dict

bridge = Bridge(from_dict({
    "cdc": {
        "stream_name": "orders",
        "begin_key": b"orders:",
        "end_key": b"orders;",
        "checkpoint_path": ["fdb-kafka-bridge", "checkpoints"],
        "checkpoint_timeout_seconds": 5,
        "checkpoint_retry_limit": 10,
    },
    "kafka": {"bootstrap_servers": "localhost:9092", "topic": "fdb.cdc.orders"},
}))
```

`cursor_file` and `checkpoint_path` are mutually exclusive. A supplied
`Bridge(..., checkpoint_store=store)` enables the backend too; the store must use the
same FDB client. FDB mode requires the bridge to construct its listener from validated
startup state, so an injected listener is rejected. Rebuilds use the same store and
revalidate the producer's destination before constructing a replacement listener.

The existing pipeline phase state is preserved: a transient `CheckpointWriteError`
retries the checkpoint without re-polling or re-publishing; ack retries skip an already
successful checkpoint. Format, stream, sink, overlap and regression failures are fatal
and stop the run loop. Outstanding Kafka commits retain the existing retry/teardown rules.

## Storage and identity

Default tuple prefix: `("fdb-kafka-bridge", "checkpoints")`.
Subspace: `Subspace(("fdb-kafka-bridge", "checkpoints"))`.
Key: `subspace.pack((raw_stream_name,))`.
Value: tuple-packed `(1, stream_id, last_consumed_version, kafka_cluster_id, topic)`.

The stream name is intentionally stored in the key, rather than duplicated in the value;
startup separately checks the checkpoint's registration ID against that named stream.
There is no `updated_by_instance` audit field in the cursor value. Process identity is
kept in the separate owner record's random token for fencing, not as a human-readable
writer history. A separate last-writer audit field remains a follow-up; deployments
still require one active process per stream.

`SinkIdentity` uses Kafka's broker-reported cluster ID, obtained through
`MutationProducer.cluster_id(timeout)`, plus the configured topic **name**. Bootstrap
addresses are not an identity: several addresses may name the same cluster. Missing
cluster metadata fails startup. Load and save reject a different cluster or topic
before any progress can be acknowledged. Unknown formats, malformed values and the old
experimental three-field values are rejected; there is no automatic migration that
guesses which sink received earlier records.

Topic ID (KIP-516) is intentionally not stored. Deleting a topic and recreating it under
the same name still matches the checkpoint, so the bridge resumes and the new topic never
receives earlier history. Operators who recreate a topic must plan an explicit replay/backfill before changing
the checkpoint; deleting progress alone cannot restore acknowledged history. Extending the sink record later would
require a new cursor format version.

The prefix is deterministic and does not use Directory Layer allocation. Directory
removal/recreation cannot redirect a new process to a different checkpoint prefix.
This replaces the pre-merge experimental Directory Layer layout; existing experimental
checkpoints need an explicit migration/replay plan before switching. No automatic
fallback can infer whether absent progress is safe.

Before loading a checkpoint, a process claims `(raw_stream_name, "owner")` by writing
tuple-packed `(1, epoch + 1, random_instance_token)`. The leading `1` is the owner
record format version so later lease/release fields can extend the layout without
colliding with unversioned binaries. The token prevents an ABA match if an owner key
is deleted and recreated. A retry after an uncertain claim commit reuses its token.
Rebuilds verify the original claim; they never take ownership back. Unversioned
two-field owner records are rejected as `CheckpointFormatError`.
`subspace` and `run_transaction()` are public seams for future lease support.

Every save reads and validates the owner in the same transaction, even for an equal
cursor. A takeover conflicts with an in-flight save; subsequent saves by the old owner
raise permanent `CheckpointFencedError`. Ownership is also checked before resume ack
and every pipeline ack attempt, including retries after a successful checkpoint.
These checks do not atomically fence a native ack already in flight or Kafka output.

Every save is an optimistic FDB transaction that reads the stored value before writing:

| Stored state | Result |
|---|---|
| Missing/superseded claim | `CheckpointFencedError` |
| No checkpoint, valid claim | Write |
| Different Kafka cluster/topic | `CheckpointSinkMismatchError` |
| Different stream registration ID | `CheckpointStreamMismatchError` |
| Newer stored version | `CheckpointRegressionError` |
| Equal cursor | No-op, including retry after an uncertain commit |
| Older stored version | Advance |

Transactions have configurable total timeout and retry limit, defaulting to five seconds
and ten retries. Exhausted retryable native errors and transaction timeout (1031)
become `CheckpointWriteError` and prevent ack while allowing phase-only retry in a new
transaction. FDB's native retry predicate excludes 1031 because the timed-out transaction
cannot be reused; the bridge can safely retry the idempotent operation in a fresh one.
Other native non-retryable errors become permanent `CheckpointError` and stop the bridge.
FDB's predicate receives the native exception; `on_error` receives its integer code.
Cursors resume as-is, without adding one. The listener waits for the read version and
re-acknowledges the saved cursor before consuming.

`run_transaction()` waits for `commit().wait()` before returning and only then reaches
its `finally: tr.cancel()`. That cleanup cannot roll back a confirmed commit. Cancelling
while a commit outcome is still unknown is different: the write may have committed,
which is why equal-cursor saves and claim-token reconciliation remain required. See
[FoundationDB transaction cancellation](https://apple.github.io/foundationdb/api-python.html#fdb.Transaction.cancel).
The live save/load and commit-unknown tests exercise cleanup with the shipped binding.

## Startup guards

Before stream registration or checkpoint advancement:

1. Read current stream metadata and reject multi-range streams or a different configured
   range with `StartupError`.
2. Open the plain subspace without writes and reject overlap between its complete prefix
   range and the captured range. Directory Layer metadata needs no exclusion because
   this backend never writes it.
3. Claim ownership (or verify the existing claim on rebuild), then load the checkpoint.
4. Require the saved registration to exist and match. Re-read stream metadata and check
   `last_consumed_version + 1 >= min_version` before restoring an ack. A foreign ack
   beyond this frontier raises `CDCInvalidCursorError`; automatic restart cannot recover
   missing history. The native 1007 refusal is also terminal if retention changes later.
5. Require an explicit range if the stream does not exist; only then register it.

An existing stream with no checkpoint logs a warning and uses cluster `min_version`.
No startup check removes or re-registers an existing stream. If both registration and
checkpoint are deleted, startup cannot detect the lost history and starts fresh.

## CLI

After `pip install .` (and installing the matching FDB client library):

```bash
streamline-daemon --stream-name orders --subspace-prefix orders: \
  --kafka-bootstrap-servers localhost:9092
```

From a checkout, use `PYTHONPATH=.:src:protobuf/gen python -m fdb_kafka_bridge.bridge`.
The CLI defaults to FDB checkpoints, topic `fdb.cdc.<stream_name>`, and the UTF-8 stream
name as partition key. This does not change the library's existing `fdb-cdc` topic default.

Flags override environment variables:

| Flag | Environment | Default |
|---|---|---|
| `--stream-name` | `STREAMLINE_STREAM_NAME` | Required |
| `--kafka-bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `--cluster-file` | `FDB_CLUSTER_FILE` | Binding default |
| `--topic` | `KAFKA_TOPIC` | `fdb.cdc.<stream_name>` |
| `--transactional-id` | `STREAMLINE_TRANSACTIONAL_ID` | Nontransactional |
| `--subspace-prefix` | `STREAMLINE_SUBSPACE_PREFIX` | Consume existing stream |
| `--checkpoint-path` | `STREAMLINE_CHECKPOINT_PATH` | `fdb-kafka-bridge/checkpoints` |
| `--checkpoint-timeout-s` | `STREAMLINE_CHECKPOINT_TIMEOUT_S` | 5 |
| `--checkpoint-retry-limit` | `STREAMLINE_CHECKPOINT_RETRY_LIMIT` | 10 |
| `--flush-timeout-s` | `STREAMLINE_FLUSH_TIMEOUT_S` | 30 |
| `--shutdown-timeout-s` | `STREAMLINE_SHUTDOWN_TIMEOUT_S` | 20 |
| `--ack-timeout-s` | `STREAMLINE_ACK_TIMEOUT_S` | 5 |
| `--proxy-consume-poll-timeout-s` | `CDC_PROXY_CONSUME_POLL_TIMEOUT` | 5 |
| `--max-record-bytes` | `STREAMLINE_MAX_RECORD_BYTES` | Derived from stream-name key |

Prefixes are UTF-8 text or `0x`-prefixed hexadecimal bytes. A path is slash-separated
non-empty tuple-prefix components. The default record cap accounts for the stream-name
key and Kafka framing. Explicit caps are validated against that budget; broker/topic
batch limits must still accommodate the producer's configured message size.

The CLI runs startup and `Bridge.run()` in a worker thread. SIGINT/SIGTERM request
shutdown; the main thread calls `Bridge.stop(deadline=...)` and restores previous
signal handlers. The first signal establishes one monotonic deadline shared by cleanup
and worker join. The default CLI budget is 20 seconds; configure the supervisor or
Kubernetes termination grace period above that budget (the Kubernetes default is 30s).
A parked consume future is explicitly cancelled before waiting for the step lock.
Ack futures have their own timeout. On expiry the listener requests cancellation and
waits for the future to settle before allowing another operation on that handle. Only
1101 (`operation_cancelled`) is swallowed; another native result/error is preserved.
Cancellation completion can extend the ack wait beyond its timeout; the separate total
shutdown deadline still bounds CLI teardown. Checkpoint-before-ack ordering is preserved.
Teardown never closes handles concurrently with a step.
An unresponsive dependency causes `TimeoutError` at the total deadline; outstanding
worker/cleanup threads are daemon threads, so they cannot hold the CLI process open.
The daemon never removes streams or deletes checkpoints on shutdown.

Every new consumer handle retries 2000 only on its first consume (fresh create or
resume), for at most the configured proxy consume poll timeout. Set this to the actual
FDB `CDC_PROXY_CONSUME_POLL_TIMEOUT`; setting the CLI environment variable does not
change the server knob. Subsequent 2000 errors are terminal. Retry waits are
interruptible by shutdown.

Exit statuses: 0 for clean shutdown, 1 for runtime/dependency failure, shutdown timeout,
or exhausted checkpoint write retries (including 1031), 2 for invalid configuration,
3 for deterministic startup/checkpoint/CDC refusals and permanent Kafka
`MSG_SIZE_TOO_LARGE`. Regression, fencing, invalid resume cursors (also on rebuild),
unsupported multi-range streams and 1007 refusals require operator action rather than
automatic retries.

A worker failure takes precedence over a secondary `stop()` failure: teardown errors are
logged, but the original worker error and its exit code are preserved. A teardown failure
without a worker error remains a runtime failure with exit 1. Checkpoint transaction
timeouts during startup likewise return 1; in the run loop they retry the current phase.

## Operator recovery: checkpoint reset

`CheckpointStore.delete(stream_name)` is an offline operator API. It deletes only the
cursor key, does not check ownership, and is not exposed by the CLI. Never use it to
bypass a live owner's checks or to make a sink mismatch disappear during normal startup.

1. Stop every bridge process for the stream and disable supervisor restarts. A takeover
   alone is insufficient: an old process can still have Kafka output in flight.
2. Preserve the cursor bytes, owner bytes, stream registration/range/retention metadata,
   and original Kafka sink identity before changing anything. Determine whether the
   target is the original topic, a recreated topic, or a different destination.
3. Establish the required replay/backfill source and downstream dedup/reset plan. FDB
   may already have released acknowledged history. If that history is needed, restore
   it from an independent snapshot/log or backfill before choosing a safe resume point.
   Deleting a cursor or re-registering a stream does not recreate that history.
4. Prefer restoring/migrating a verified checkpoint belonging to the same stream and
   sink. If intentionally resetting, open a store with the same `checkpoint_path` and
   call `delete(raw_stream_name)` only after accepting startup from the stream's current
   `min_version`. Leave the owner key intact; the replacement process claims a new epoch.
   Corrupt/incompatible owner data requires a separately planned offline migration.
5. Start one bridge process against the agreed sink and verify end-to-end progress and
   downstream state before re-enabling automatic restarts. Keep the preserved metadata
   with the recovery record.

## Scope and verification

Use `replicas: 1` and `strategy: Recreate` for Kubernetes deployments. Owner fencing protects checkpoint writes during
accidental overlap, but cannot retract old in-flight Kafka records or native
acknowledgements. There are no leases, heartbeats or automatic multi-pod failover in this
backend. All deployments of a stream must use the same checkpoint path and sink. A sink
change needs an explicit replay/backfill plan; deleting progress does not recover history
that was already acknowledged. A stalled stream retains TLog history, so monitor progress
and repeated failures.

Unit tests cover backend wiring, sink changes during rebuild, retry-in-place, CLI
precedence and signal cleanup. Live tests use real FDB transactions for monotonic saves,
sink mismatches, corruption, overlap rejection, retry exhaustion, ownership takeover,
commit-unknown recovery, retention boundaries and restart crash windows.
FDB-to-Kafka tests verify split version batches, partition ordering, both producer modes,
rejected publication recovery, and CLI restart. Packaging tests invoke the installed
entrypoint outside the source tree. The bridge CI job runs the live suite with 100%
branch coverage on the checkpoint module.

```bash
docker compose -f .devcontainer/docker-compose.yml up -d app kafka
docker compose -f .devcontainer/docker-compose.yml exec app bash -c '
  FDB_CDC_INTEGRATION=1 KAFKA_INTEGRATION=1 PYTHONPATH=.:src:protobuf/gen \
    python3 -m pytest tests/bridge tests/cdc/test_checkpoint*.py -v
'
```
