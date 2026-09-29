# Observability module spec

Contract for `src/observability/` and the instrumented Kafka stub in
`src/kafka/`. Trello:
[End-to-End Pipeline Telemetry, Healthz & Structured Audit Logging](https://trello.com/c/JENLbMhs).

## 1. Metrics (`GET /metrics` on `:9102`)

| Metric | Type | Source |
|--------|------|--------|
| `fdb_mutations_polled_total{opcode}` | Counter | `FDBMutationListener.poll_records` |
| `fdb_cdc_latest_read_version` | Gauge | CDC reply watermark |
| `fdb_cdc_lag_versions` | Gauge | `get_read_version() - watermark` |
| `fdb_serializer_bytes_out_total` | Counter | Serialized record sizes |
| `fdb_serializer_errors_total` | Counter | Serialize failures (then re-raise) |
| `kafka_records_published_total` | Counter | `MutationProducer.produce` |
| `kafka_publish_latency_seconds` | Histogram | `MutationProducer.produce` |
| `kafka_version_end_markers_total` | Counter | `produce` on VersionEnd records |

Opcode labels use declared names (`SET_VALUE`, `CLEAR_RANGE`, …) or `TYPE_<n>`.

## 2. Health endpoints (`:9102`)

Background daemon thread (`start_http_server`):

- `GET /healthz` → 200 if the process heartbeat is within the liveness window
  (updated on each `poll_records`).
- `GET /readyz` → 200 only when FDB readiness **and** Kafka readiness are true.
- `GET /metrics` → Prometheus text from the observability registry.

`FDBClient.open()` / `close()` toggle FDB readiness. Constructing
`MutationProducer` marks Kafka ready; `close()` clears it. The stub producer does
not contact a broker.

## 3. Structured logging

`configure_logging()` configures `structlog` JSON. VersionEnd audit lines use
`audit_version_end(...)` with `fdb_version`, `mutation_count`, and
`duration_seconds` (optional `topic` / `partition`).

## 4. Public API

Import from `src.observability` and `src.kafka.MutationProducer`.
