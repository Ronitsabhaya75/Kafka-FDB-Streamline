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

- Real librdkafka / broker I/O (Kafka metrics stay registered but untouched until
  the Kafka card lands; stub does not set `/readyz`).
- Starting `:9102` / JSON logging from this package alone — the daemon entrypoint
  owns process wiring.

## Architecture

```
src/observability/     metrics, HealthState, HTTP :9102, JSON logging
src/cdc/               hooks in FDBClient.open + listener poll/ack
src/kafka/             MutationProducer stub (no metrics / readiness)
```

### Metrics (`GET :9102/metrics`) — all card names

| Name | Type | Wired by |
|------|------|----------|
| `fdb_mutations_polled_total{opcode=…}` | Counter | `FDBMutationListener.poll_records` |
| `fdb_cdc_latest_read_version` | Gauge | listener (reply watermark) |
| `fdb_cdc_lag_versions` | Gauge | listener vs cached GRV (unchanged on GRV failure) |
| `fdb_serializer_bytes_out_total` | Counter | listener after serialize |
| `fdb_serializer_errors_total` | Counter | listener on `SerializationError` |
| `kafka_records_published_total` | Counter | registered; real producer only |
| `kafka_publish_latency_seconds` | Histogram | registered; real producer only |
| `kafka_version_end_markers_total` | Counter | registered; real producer only |

### HTTP (`:9102`, bind `0.0.0.0`)

Background daemon thread:

- `GET /metrics` — Prometheus text
- `GET /healthz` — 200 if process loop heartbeat is fresh (post-consume / post-ack;
  default timeout 120s so idle long-polls do not flap)
- `GET /readyz` — 200 only when FDB (refcount) **and** Kafka readiness are true

`FDBClient.open()` marks FDB ready after a successful `get_read_version()`.
Kafka readiness stays false until the real producer can prove broker metadata.

### Structured JSON logging

`structlog` JSON: `timestamp`, `level`, `fdb_version`, `mutation_count`,
`duration_seconds`. VersionEnd audits fire **after ack**; idle watermarks at
DEBUG; non-zero at INFO. Topic / partition wait on the real producer.

### Trello acceptance checklist

| Checklist item | Coverage |
|----------------|----------|
| Add `prometheus_client` to `pyproject.toml` | Yes (+ `structlog`) |
| Instrument FDBClient / listener polling | Yes |
| Instrument `MutationProducer.produce()` | Deferred — metrics registered, stub does not touch them |
| Expose `:9102/metrics` and `/healthz` via background thread | Yes (+ `/readyz`); process start is daemon entrypoint |
| Verify metrics in smoke suite | Unit tests always; live CDC path under `FDB_CDC_INTEGRATION=1` |

## Risks

- Depends on unmerged #13; rebase if that branch moves.
- Stub producer does not talk to a broker; `/readyz` stays false for Kafka until
  the real producer lands.
