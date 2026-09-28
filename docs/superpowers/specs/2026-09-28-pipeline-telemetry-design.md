# Pipeline Telemetry, Healthz & Structured Audit Logging — Design

**Date:** 2026-09-28  
**Trello:** [End-to-End Pipeline Telemetry, Healthz & Structured Audit Logging](https://trello.com/c/JENLbMhs)  
**Branch:** `dev/pipeline-telemetry` (cut from `dev/fdb-mutations` / PR #13)  
**Status:** Approved approach (observability package + thin CDC hooks; Kafka stubbed)

## Goal

Give the bridge daemon runtime visibility before v0.1.0: Prometheus metrics,
liveness/readiness HTTP endpoints, and structured JSON audit logs — without
waiting for a real Kafka producer module.

## Non-goals

- Implementing `MutationProducer` or the full bridge daemon loop.
- Live FDB/Kafka smoke verification in CI (manual / follow-up).
- Merging this branch or opening a PR until Meet asks.

## Architecture

```
src/observability/
  __init__.py          # public API (__all__)
  metrics.py           # Prometheus metric definitions + record_* helpers
  health.py            # liveness + FDB/Kafka readiness state
  http.py              # background daemon thread on :9102
  logging.py           # JSON structured logging setup + VersionEnd audit helper

src/cdc/listener.py    # thin hooks into poll_records / serialize paths
```

Callers import from `src.observability`. CDC calls metric/audit helpers; Kafka
calls `record_kafka_publish(...)` later when the producer exists.

### Metrics (`:9102/metrics`)

| Name | Type | Notes |
|------|------|--------|
| `fdb_mutations_polled_total` | Counter | Label `opcode` (`SET_VALUE`, `CLEAR_RANGE`, …) |
| `fdb_cdc_latest_read_version` | Gauge | Latest processed / reply watermark |
| `fdb_cdc_lag_versions` | Gauge | `cluster_read_version - cursor_version` (0 if unknown) |
| `fdb_serializer_bytes_out_total` | Counter | Sum of serialized record byte lengths |
| `fdb_serializer_errors_total` | Counter | Serialization failures in the poll path |
| `kafka_records_published_total` | Counter | Stub helper; unused until producer |
| `kafka_publish_latency_seconds` | Histogram | Stub helper |
| `kafka_version_end_markers_total` | Counter | Stub helper (or incremented when CDC emits VersionEnd records if we treat them as “markers prepared for Kafka”) |

**Decision for VersionEnd markers:** Increment `kafka_version_end_markers_total`
when the listener emits a `VersionEnd` record (watermark prepared for publish).
True broker-ack counting stays in the Kafka stub until the producer lands.

### HTTP (`:9102`)

Background daemon thread (stdlib `http.server` or prometheus WSGI + thin
handlers — prefer `prometheus_client` exposition + small custom handlers for
health):

- `GET /metrics` — Prometheus text exposition.
- `GET /healthz` — **200** if the process loop is marked alive (heartbeat /
  last-poll timestamp within a configurable window, default generous for tests).
- `GET /readyz` — **200** only when FDB readiness is true **and** Kafka readiness
  is true. Until a producer exists, Kafka readiness defaults to **False** unless
  a test/caller marks it ready via `health.set_kafka_ready(True)` (explicit stub).

Bind address/port configurable (default `127.0.0.1:9102`).

### Structured logging

Use **`structlog`** configured for JSON on stdout (stdlib fallback only if we
must; card allows either — pick structlog for context binding).

Shared keys when known: `timestamp`, `level`, `fdb_version`, `topic`,
`partition`, `mutation_count`. Missing keys omitted rather than forced nulls.

On every emitted `VersionEnd` from the listener: one `INFO` audit event with
`mutation_count` (group total or `0` for idle watermark), `fdb_version`, and
`duration_seconds` for that poll cycle.

### CDC instrumentation points

In `FDBMutationListener.poll_records`:

1. After a successful native consume, update latest version / lag via
   `client.get_read_version()` when available.
2. For each serialized mutation (or from returned protobuf records), increment
   opcode counters.
3. Add serialized byte lengths to `fdb_serializer_bytes_out_total`.
4. On serialization errors, increment `fdb_serializer_errors_total` (then
   re-raise — existing failure semantics unchanged).
5. Emit VersionEnd audit log + marker counter when VersionEnd records are
   produced.
6. Touch liveness heartbeat so `/healthz` stays green while polling.

Do **not** change consume → checkpoint → ack ordering.

### Dependencies

- Runtime: `prometheus_client`, `structlog` in `pyproject.toml` `[project].dependencies`.
- Pin versions in `requirements-dev.txt` for reproducible CI.

### Docs & tests

- `docs/OBSERVABILITY-SPEC.md` — public contract (metrics names, endpoints, log
  keys, Kafka stub rules). Row in `docs/INDEX.md`.
- Unit tests under `tests/observability/` and CDC tests asserting metric
  increments / audit fields with mocks (no live FDB/Kafka).

## Acceptance mapping

| Card checklist | This design |
|----------------|-------------|
| Add `prometheus_client` | Yes (+ structlog) |
| Instrument FDB poll loop | Yes on `FDBMutationListener` |
| Instrument `MutationProducer.produce()` | Stub API only |
| Expose `:9102/metrics` and `/healthz` | Yes (+ `/readyz`) |
| Live smoke suite verification | Follow-up / manual |

## Risks

- Branch depends on unmerged #13; rebases may be needed when #13 updates.
- `/readyz` will fail until Kafka readiness is marked — document that so ops
  do not expect ready without a producer.
