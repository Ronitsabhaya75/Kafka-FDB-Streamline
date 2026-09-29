# Docs index

Map of the project's docs. Check here before reading docs wholesale, then open only what the task needs. Adding or substantially changing a doc means updating its row.

| Doc | Read it for |
|-----|-------------|
| [`../Proposal.md`](../Proposal.md) | Project scope, goals, architecture, sponsor requirements |
| [`conventions.md`](conventions.md) | Repo layout, branching, code style, local setup (incl. Trello MCP), PR rules |
| [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md) | FDB native CDC: stream lifecycle, ack/resume semantics, Python API (#13925), limitations, API version 800 gate, Kafka key/partitioning, #13925/#13971 ABI risk. Its TL;DR table links each section. |
| [`SERIALIZATION-SPEC.md`](SERIALIZATION-SPEC.md) | `src/serialization/` contract: public API, wire mapping, input rules, errors and decode failures, reader contract for a version group |
| [`CDC-CONSUMER-SPEC.md`](CDC-CONSUMER-SPEC.md) | `src/cdc/` contract: FDBClient, stream lifecycle, bounded record polling, safe resume and acknowledgement |
| [`OBSERVABILITY-SPEC.md`](OBSERVABILITY-SPEC.md) | Metrics, `:9102` health endpoints, structured audit logging, Kafka produce instrumentation |
| [`DAEMON-SPEC.md`](DAEMON-SPEC.md) | `src/bridge/` CLI config, run loop, SIGINT/SIGTERM shutdown, entrypoints |
| [`superpowers/specs/2026-09-28-pipeline-telemetry-design.md`](superpowers/specs/2026-09-28-pipeline-telemetry-design.md) | Design notes for the telemetry card (approach + acceptance mapping) |
| [`superpowers/specs/2026-09-29-daemon-entrypoint-design.md`](superpowers/specs/2026-09-29-daemon-entrypoint-design.md) | Design notes for the daemon entrypoint card |
