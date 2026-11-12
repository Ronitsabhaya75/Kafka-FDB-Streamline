# FDB checkpoints and production CLI

Contract for `fdb_kafka_bridge.cdc.checkpoint`, `bridge.startup`, and `bridge.cli`.
Related: [bridge state machine](BRIDGE-PIPELINE-SPEC.md),
[CDC listener](CDC-CONSUMER-SPEC.md), [producer](KAFKA-PRODUCER-SPEC.md).
Trello: [Wire FDB Listener into Kafka Mutation Producer](https://trello.com/c/IidPiFIp).

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

`SinkIdentity` uses Kafka's broker-reported cluster ID, obtained through
`MutationProducer.cluster_id(timeout)`. Bootstrap addresses are not an identity:
several addresses may name the same cluster. Missing cluster metadata fails startup.
Load and save reject a different cluster or topic before any progress can be acknowledged.
Unknown formats, malformed values and the old experimental three-field values are rejected;
there is no automatic migration that guesses which sink received earlier records.

The prefix is deterministic and does not use Directory Layer allocation. Directory
removal/recreation cannot redirect a new process to a different checkpoint prefix.
This replaces the pre-merge experimental Directory Layer layout; existing experimental
checkpoints need an explicit migration/replay plan before switching. No automatic
fallback can infer whether absent progress is safe.

Before loading a checkpoint, a process claims `(raw_stream_name, "owner")` by writing
tuple-packed `(epoch + 1, random_instance_token)`. The token prevents an ABA match if
an owner key is deleted and recreated. A retry after an uncertain claim commit reuses
its token. Rebuilds verify the original claim; they never take ownership back.
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
and ten retries. Exhausted retryable native errors become `CheckpointWriteError` and
prevent ack while allowing phase-only retry. Native non-retryable errors, including
transaction timeout (1031), become permanent `CheckpointError` and stop the bridge.
FDB's predicate receives the native exception; `on_error` receives its integer code.
Cursors resume as-is, without adding one. The listener waits for the read version and
re-acknowledges the saved cursor before consuming.

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
Ack futures have their own bounded timeout and are cancelled on expiry, preserving
checkpoint-before-ack ordering. Teardown never closes handles concurrently with a step.
An unresponsive dependency causes `TimeoutError` at the total deadline; outstanding
worker/cleanup threads are daemon threads, so they cannot hold the CLI process open.
The daemon never removes streams or deletes checkpoints on shutdown.

A resumed consumer retries 2000 only on its first consume, for at most the configured
proxy consume poll timeout. Set this to the actual FDB `CDC_PROXY_CONSUME_POLL_TIMEOUT`;
setting the CLI environment variable does not change the server knob. Subsequent 2000
errors are terminal. Retry waits are interruptible by shutdown.

Exit statuses: 0 for clean shutdown, 1 for runtime/dependency failure or shutdown timeout,
2 for invalid configuration, 3 for deterministic startup/checkpoint/CDC refusals.
Regression, fencing, invalid resume cursors (also on rebuild), unsupported multi-range
streams and 1007 refusals require operator action rather than automatic retries.

## Scope and verification

Use `replicas: 1` and `strategy: Recreate` for Kubernetes deployments. Owner fencing
protects checkpoint writes during accidental overlap, but cannot retract old in-flight
Kafka records or native acknowledgements. There are no leases, heartbeats or automatic
multi-pod failover in this backend. All deployments of a stream must use the same checkpoint
path and sink. A sink change needs an explicit replay/backfill plan; deleting progress
does not recover history that was already acknowledged. A stalled stream retains TLog
history, so monitor progress and repeated failures.

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
