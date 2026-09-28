# Pipeline Telemetry, Healthz & Structured Audit Logging — Design

**Date:** 2026-09-28  
**Trello:** [End-to-End Pipeline Telemetry, Healthz & Structured Audit Logging](https://trello.com/c/JENLbMhs)  
**Branch:** `dev/pipeline-telemetry` (cut from `dev/fdb-mutations` / PR #13)  
**Status:** Approved — full Trello card coverage on top of #13

## Goal

Complete runtime visibility for the bridge before v0.1.0: Prometheus metrics,
`/healthz` + `/readyz`, and structured JSON audit logs — covering every item on
the Trello card.

## Non-goals

- Real librdkafka / broker I/O (producer is a metrics-instrumented stub until the
  Kafka card lands).
- Merging this branch or opening a PR until Meet asks.

## Architecture

```
src/observability/     metrics, health, HTTP :9102, JSON logging
src/cdc/               hooks in FDBClient.open + FDBMutationListener.poll_records
src/kafka/             MutationProducer.produce() with Kafka metrics
```

### Metrics (`GET :9102/metrics`) — all card names

| Name | Type | Wired by |
|------|------|----------|
| `fdb_mutations_polled_total{opcode=…}` | Counter | `FDBMutationListener.poll_records` |
| `fdb_cdc_latest_read_version` | Gauge | listener (reply watermark) |
| `fdb_cdc_lag_versions` | Gauge | listener vs `FDBClient.get_read_version()` |
| `fdb_serializer_bytes_out_total` | Counter | listener after serialize |
| `fdb_serializer_errors_total` | Counter | listener on serialize failure |
| `kafka_records_published_total` | Counter | `MutationProducer.produce()` |
| `kafka_publish_latency_seconds` | Histogram | `MutationProducer.produce()` |
| `kafka_version_end_markers_total` | Counter | `MutationProducer.produce()` on VersionEnd |

### HTTP (`:9102`)

Background daemon thread:

- `GET /metrics` — Prometheus text
- `GET /healthz` — 200 if process loop heartbeat is fresh
- `GET /readyz` — 200 only when FDB **and** Kafka readiness are true

`FDBClient.open()` marks FDB ready. `MutationProducer` marks Kafka ready on
construction (stub is always “handle established”; real broker health replaces
this later).

### Structured JSON logging

`structlog` JSON: `timestamp`, `level`, `fdb_version`, `topic`, `partition`,
`mutation_count` when known. Every VersionEnd from the listener → `INFO` audit
with `mutation_count`, `fdb_version`, `duration_seconds`.

### Trello acceptance checklist

| Checklist item | Coverage |
|----------------|----------|
| Add `prometheus_client` to `pyproject.toml` | Yes (+ `structlog`) |
| Instrument FDBClient / listener polling | Yes |
| Instrument `MutationProducer.produce()` | Yes (`src/kafka/producer.py` stub) |
| Expose `:9102/metrics` and `/healthz` via background thread | Yes (+ `/readyz`) |
| Verify metrics in smoke suite | Unit/smoke tests with mocks; opt-in live marker documented |

## Risks

- Depends on unmerged #13; rebase if that branch moves.
- Stub producer does not talk to a broker; `/readyz` Kafka bit means “handle
  constructed,” not broker round-trip.
