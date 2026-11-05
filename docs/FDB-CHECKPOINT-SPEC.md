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
registration. Kafka transactions add atomic visibility for `read_committed` consumers;
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

Default directory path: `("fdb-kafka-bridge", "checkpoints")`.
Key: `directory.pack((raw_stream_name,))`.
Value: tuple-packed `(1, stream_id, last_consumed_version, kafka_cluster_id, topic)`.

`SinkIdentity` uses Kafka's broker-reported cluster ID, obtained through
`MutationProducer.cluster_id(timeout)`. Bootstrap addresses are not an identity:
several addresses may name the same cluster. Missing cluster metadata fails startup.
Load and save reject a different cluster or topic before any progress can be acknowledged.
Unknown formats, malformed values and the old experimental three-field values are rejected;
there is no automatic migration that guesses which sink received earlier records.

Every save is an optimistic FDB transaction that reads the stored value before writing:

| Stored state | Result |
|---|---|
| No checkpoint | Write |
| Different Kafka cluster/topic | `CheckpointSinkMismatchError` |
| Different stream registration ID | `CheckpointStreamMismatchError` |
| Newer stored version | `CheckpointRegressionError` |
| Equal cursor | No-op, including retry after an uncertain commit |
| Older stored version | Advance |

Transactions have configurable total timeout and retry limit, defaulting to five seconds
and ten retries. Exhausted native errors become `CheckpointWriteError` and prevent ack.
Cursors resume as-is, without adding one. The listener waits for the read version and
re-acknowledges the saved cursor before consuming.

## Startup guards

Before any stream registration or checkpoint allocation:

1. Read current stream metadata and an existing checkpoint directory without writes.
2. Validate that a saved cursor's stream exists and its registration ID matches.
3. Validate a supplied key range against the registered range.
4. Reject overlap between the captured range and either the checkpoint directory's
   complete prefix range or Directory Layer metadata `[0xfe, 0xff)`.
5. Require an explicit range if the stream does not exist.

Only then may the directory be allocated and a missing stream registered. Allocation
and its overlap check are in the same transaction, so an unsafe allocation rolls back.
An existing stream with no checkpoint logs a warning and uses cluster `min_version`.
No startup check removes or re-registers an existing stream.

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
| `--shutdown-timeout-s` | `STREAMLINE_SHUTDOWN_TIMEOUT_S` | 30 |
| `--max-record-bytes` | `STREAMLINE_MAX_RECORD_BYTES` | 900000 |

Prefixes are UTF-8 text or `0x`-prefixed hexadecimal bytes. A path is slash-separated
non-empty directory components. The envelope default leaves room below Kafka's message
limit for framing and the key; broker limits must still accommodate the configured size.

The CLI runs `Bridge.run()` in a worker thread. SIGINT/SIGTERM request shutdown; the main
thread calls `Bridge.stop()` and restores previous signal handlers. That allows the
existing stop logic to unblock an idle native consume and resolve outstanding commits.
`shutdown_timeout_seconds` bounds individual waits, not the whole process teardown;
allow for the step-lock waits, producer close and worker join in supervisor timeouts.
The daemon never removes streams or deletes checkpoints on shutdown.

Exit statuses: 0 for clean shutdown, 1 for runtime/dependency failure, 2 for invalid
configuration, 3 for unsafe startup or corrupt/foreign checkpoints.

## Scope and verification

Exactly one process may own a stream. There are no leases, heartbeats or multi-pod
failover in this backend. All deployments of a stream must use the same checkpoint
path and sink. A sink change needs an explicit replay/backfill plan; deleting progress
does not recover history that was already acknowledged. A stalled stream retains TLog
history, so monitor progress and repeated failures.

Unit tests cover backend wiring, sink changes during rebuild, retry-in-place, CLI
precedence and signal cleanup. Live tests use real FDB transactions for monotonic saves,
sink mismatches, corruption, overlap rollback, retry exhaustion and restart crash windows.
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
