# Daemon module spec

Contract for `src/bridge/` — CLI configuration and the CDC → Kafka run loop.
Trello: [Daemon Entrypoint & CLI Configuration Runner](https://trello.com/c/5t718fA3).

## 1. Entrypoints

| Command | Module |
|---------|--------|
| `streamline-daemon` | `src.bridge.cli:main` ([project.scripts](../pyproject.toml)) |
| `python -m src.bridge` | `src.bridge.__main__` |
| `python -m src.main` | `src.main` → same `main()` |

Exit codes: `0` on clean shutdown, `2` on config/usage errors, `1` on runtime failure.

## 2. Configuration

Flags override environment variables. Defaults:

| Flag | Env | Default |
|------|-----|---------|
| `--cluster-file` | `FDB_CLUSTER_FILE` | `/etc/foundationdb/fdb.cluster` |
| `--kafka-bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `--subspace-prefix` | `STREAMLINE_SUBSPACE_PREFIX` | unset (no stream registration; consume existing stream) |
| `--topic` | `KAFKA_TOPIC` | `fdb-cdc` |
| `--batch-size` | `STREAMLINE_BATCH_SIZE` | `100` |
| `--poll-interval-ms` | `STREAMLINE_POLL_INTERVAL_MS` | `100` |
| `--stream-name` | `STREAMLINE_STREAM_NAME` | same as `--topic` when unset |
| `--metrics-host` | `STREAMLINE_METRICS_HOST` | `127.0.0.1` |
| `--metrics-port` | `STREAMLINE_METRICS_PORT` | `9102` |

`--subspace-prefix` accepts `0x`-prefixed hex, even-length hex, or a UTF-8 string
and becomes `bytes` for `FDBMutationListener(subspace=…)`.

`--batch-size` is validated (`>= 1`) and retained for future Kafka batching; the
native CDC path still processes one consume reply per poll.

## 3. Run loop

1. Configure JSON logging; start observability HTTP (`/metrics`, `/healthz`, `/readyz`).
2. Open `FDBClient`, `FDBMutationListener.start()`, construct `MutationProducer`.
3. Until stop: `poll_records` → if records, `produce` then `acknowledge`; else sleep
   `poll_interval_ms`.
4. SIGINT / SIGTERM set a stop flag (no exit in the handler). Do not start a new
   poll after stop. If produce succeeded for the current reply, still
   `acknowledge()` before teardown.
5. Close producer, close listener (and FDB client), stop HTTP.

Durable FDB commit-version checkpointing is out of scope here (separate Trello
card). This process's "commit offsets" step is the CDC `acknowledge()` after a
successful produce (stub flush is synchronous today).

## 4. Public API

Import `BridgeConfig`, `load_config`, `BridgeDaemon` from `src.bridge`, or run via
the entrypoints above.
