# Pipeline Telemetry, Healthz & Structured Audit Logging — Design

**Date:** 2026-09-28  
**Trello:** [End-to-End Pipeline Telemetry, Healthz & Structured Audit Logging](https://trello.com/c/JENLbMhs)  
**Branch:** `dev/pipeline-telemetry` (rebased onto `main` after #13 / #18)  
**Status:** Approved — full Trello card coverage on top of main

## Goal

Complete runtime visibility for the bridge before v0.1.0: Prometheus metrics,
`/healthz` + `/readyz`, and structured JSON audit logs — covering every item on
the Trello card.

## Non-goals

- Wiring Kafka publish metrics / `/readyz` into `MutationProducer` delivery
  callbacks (metrics stay registered; producer does not set them yet).
- Starting `:9102` / JSON logging from this package alone — the daemon entrypoint
  owns process wiring.

## Architecture

```
src/observability/     metrics, HealthState, HTTP :9102, JSON logging
src/cdc/               hooks in FDBClient.open + listener poll/ack
src/kafka/             MutationProducer (real client; metrics/readyz not wired yet)
```

### Metrics (`GET :9102/metrics`) — all card names

| Name | Type | Wired by |
|------|------|----------|
| `fdb_mutations_polled_total{opcode=…}` | Counter | `FDBMutationListener.poll_records` |
| `fdb_cdc_latest_read_version` | Gauge | listener (reply watermark) |
| `fdb_cdc_lag_versions` | Gauge | listener vs cached GRV (unchanged on GRV failure) |
| `fdb_serializer_bytes_out_total` | Counter | listener after serialize |
| `fdb_serializer_errors_total` | Counter | listener on `SerializationError` |
| `kafka_records_published_total` | Counter | registered; wire after broker ack |
| `kafka_publish_latency_seconds` | Histogram | registered; wire after broker ack |
| `kafka_version_end_markers_total` | Counter | registered; wire after broker ack |

### HTTP (`:9102`, bind `0.0.0.0`)

Background daemon thread:

- `GET /metrics` — Prometheus text
- `GET /healthz` — 200 if process loop heartbeat is fresh (post-consume / post-ack;
  default timeout 120s so idle long-polls do not flap)
- `GET /readyz` — 200 only when FDB (refcount) **and** Kafka readiness are true

`FDBClient.open()` marks FDB ready after a successful `get_read_version()`.
Kafka readiness stays false until `MutationProducer` proves broker metadata.

### Structured JSON logging

`structlog` JSON: `timestamp`, `level`, `fdb_version`, `mutation_count`,
`duration_seconds`. VersionEnd audits fire **after ack**; idle watermarks at
DEBUG; non-zero at INFO. Topic / partition wait on the real producer.

### Trello acceptance checklist

| Checklist item | Coverage |
|----------------|----------|
| Add `prometheus_client` to `pyproject.toml` | Yes (+ `structlog`) |
| Instrument FDBClient / listener polling | Yes |
| Instrument `MutationProducer.produce()` | Deferred — metrics registered, not yet on delivery path |
| Expose `:9102/metrics` and `/healthz` via background thread | Yes (+ `/readyz`); process start is daemon entrypoint |
| Verify metrics in smoke suite | Unit tests always; live CDC path under `FDB_CDC_INTEGRATION=1` |

## Risks

- Kafka `/readyz` and publish metrics stay false/zero until delivery callbacks
  call into `src.observability`.
