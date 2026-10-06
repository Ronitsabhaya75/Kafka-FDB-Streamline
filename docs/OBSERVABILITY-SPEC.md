# Observability module spec

Contract for `src/observability/` and the Kafka producer stub in `src/kafka/`.
Trello:
[End-to-End Pipeline Telemetry, Healthz & Structured Audit Logging](https://trello.com/c/JENLbMhs).

## 1. Metrics (`GET /metrics` on `:9102`)

| Metric | Type | Source |
|--------|------|--------|
| `fdb_mutations_polled_total{opcode}` | Counter | `FDBMutationListener.poll_records` |
| `fdb_cdc_latest_read_version` | Gauge | CDC reply watermark |
| `fdb_cdc_lag_versions` | Gauge | cached `get_read_version() - watermark` |
| `fdb_serializer_bytes_out_total` | Counter | Serialized record sizes |
| `fdb_serializer_errors_total` | Counter | `SerializationError` on serialize (then re-raise) |
| `kafka_records_published_total` | Counter | real producer only (registered, unused by stub) |
| `kafka_publish_latency_seconds` | Histogram | real producer only (registered, unused by stub) |
| `kafka_version_end_markers_total` | Counter | real producer only (registered, unused by stub) |

Opcode labels come from `MutationType(type_code).name`, or `TYPE_<n>` for unknown codes.

When `get_read_version()` fails, lag is **left unchanged** (not forced to 0) so an
outage does not look caught up. The listener caches GRV for a few seconds.

## 2. Health endpoints (`:9102`)

Background daemon thread (`start_http_server`), default bind `0.0.0.0:9102`:

- `GET /healthz` → 200 if a heartbeat occurred within the liveness window
  (default 120s). Heartbeat runs **after** `consume()` returns and **after** ack —
  not before the long poll.
- `GET /readyz` → 200 only when FDB readiness (refcount > 0) **and** Kafka
  readiness are true.
- `GET /metrics` → Prometheus text from the observability registry.

`FDBClient.open()` acquires one FDB readiness owner only after a successful
`get_read_version()`. `close()` releases one owner. Kafka readiness stays false
until a real producer can set it after a successful metadata fetch; the stub does
not touch it.

HTTP server start and `configure_logging()` are wired by the daemon entrypoint
(see stacked PR), not by importing this package.

## 3. Structured logging

`configure_logging()` configures `structlog` JSON. VersionEnd audit lines use
`audit_version_end(...)` with `fdb_version`, `mutation_count`, and
`duration_seconds` (serialize-only wall time after consume returns). Audits emit
**after** durable ack. Idle watermarks (`mutation_count == 0`) log at DEBUG;
non-zero VersionEnds at INFO. Topic / partition fields wait on the real producer.

## 4. Public API

Import from `src.observability` and `src.kafka.MutationProducer`. Package
`__all__` is the bridge contract (heartbeat, readiness setters, metric recorders,
audit). Tests import HTTP / registry / configure helpers from submodules.
