# Docs index

Map of the project's docs. Check here before reading docs wholesale, then open only what the task needs. Adding or substantially changing a doc means updating its row.

| Doc | Read it for |
|-----|-------------|
| [`../Proposal.md`](../Proposal.md) | Project scope, goals, architecture, sponsor requirements |
| [`conventions.md`](conventions.md) | Repo layout, branching, code style, local setup (incl. Trello MCP), PR rules |
| [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md) | FDB native CDC: stream lifecycle, ack/resume semantics, Python API (#13925), limitations, API version 800 gate, Kafka key/partitioning, #13925/#13971 ABI risk. Its TL;DR table links each section. |
| [`kafka-delivery-guarantees.md`](kafka-delivery-guarantees.md) | Kafka delivery semantics (acks, idempotence), exactly-once transaction strategies, error handling, and Python client (`confluent-kafka` vs `kafka-python`) comparison. |
| [`SERIALIZATION-SPEC.md`](SERIALIZATION-SPEC.md) | `src/serialization/` contract: public API, wire mapping, input rules, errors and decode failures, reader contract for a version group |
| [`CDC-CONSUMER-SPEC.md`](CDC-CONSUMER-SPEC.md) | `src/cdc/` contract: FDBClient, stream lifecycle, bounded record polling, safe resume and acknowledgement |
| [`CHECKPOINT-SPEC.md`](CHECKPOINT-SPEC.md) | FDB cursor checkpointing as an identity/integrity guard (position falls back to the cluster ack watermark), the failover flow (restart → startup resolution → run loop), crash-point outcomes, exit codes, deployment requirements, storage layout, write guards, store API, test plan, acceptance criteria |
