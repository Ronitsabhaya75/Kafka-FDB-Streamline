# Daemon Entrypoint & CLI Configuration Runner — design

Trello: [Daemon Entrypoint & CLI Configuration Runner](https://trello.com/c/5t718fA3).

## Goal

Ship a single process entrypoint that loads config (CLI + env), runs the CDC →
Kafka poll loop, exposes `:9102` health/metrics, and shuts down cleanly on
SIGINT/SIGTERM.

## Approach

Put the runnable surface in `src/bridge/` (layout from `docs/conventions.md`):

- `config.py` — `BridgeConfig` from argparse/env (flags win)
- `daemon.py` — `BridgeDaemon` lifecycle + loop
- `cli.py` / `__main__.py` / `src/main.py` — entrypoints
- Console script `streamline-daemon`

Reuse existing `FDBMutationListener`, stub `MutationProducer`, and
`start_http_server`. Durable FDB checkpoint writes stay on the checkpoint card;
this daemon finishes in-flight produce then calls CDC `acknowledge()` before
closing handles.

## Acceptance mapping

| Card criterion | Where |
|----------------|-------|
| CLI flags + env for cluster, Kafka, subspace, topic, batch, poll | `BridgeConfig` / `cli.py` |
| SIGINT/SIGTERM finish in-flight, flush (stub produce), commit offsets (CDC ack) | `BridgeDaemon.request_stop` / `run` / `close` |
| `streamline-daemon` / `python -m src.main` | `pyproject.toml` scripts + `src/main.py` |
