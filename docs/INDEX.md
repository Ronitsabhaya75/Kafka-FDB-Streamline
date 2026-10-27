# Docs index

Map of the project's docs. Check here before reading docs wholesale, then open only what the task needs. Adding or substantially changing a doc means updating its row.

| Doc | Read it for |
|-----|-------------|
| [`../Proposal.md`](../Proposal.md) | Project scope, goals, architecture, sponsor requirements |
| [`conventions.md`](conventions.md) | Repo layout, branching, code style, local setup (incl. Trello MCP), PR rules |
| [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md) | FDB native CDC: stream lifecycle, ack/resume semantics, Python API (#13925), limitations, API version 800 gate, Kafka key/partitioning, #13925/#13971 ABI risk. Its TL;DR table links each section. |
| [`kafka-delivery-guarantees.md`](kafka-delivery-guarantees.md) | Kafka delivery semantics (acks, idempotence), exactly-once transaction strategies, error handling, and Python client (`confluent-kafka` vs `kafka-python`) comparison. |
| [`KAFKA-PRODUCER-SPEC.md`](KAFKA-PRODUCER-SPEC.md) | `src.kafka.MutationProducer` contract: mandatory durability settings, transactional mode, sticky errors, fatal and closed states, close behaviour (purge and release), flush_or_raise, and local Kafka verification commands. |
| [`SERIALIZATION-SPEC.md`](SERIALIZATION-SPEC.md) | `src/serialization/` contract: public API, wire mapping, input rules, errors and decode failures, reader contract for a version group |
| [`CDC-CONSUMER-SPEC.md`](CDC-CONSUMER-SPEC.md) | `src/cdc/` contract: FDBClient, stream lifecycle, bounded record polling, safe resume and acknowledgement |
| [`CHECKPOINT-SPEC.md`](CHECKPOINT-SPEC.md) | FDB cursor checkpointing as an identity/integrity guard (position falls back to the cluster ack watermark), storage layout incl. sink identity and per-stream owner/heartbeat keys, write guards incl. lease fencing and sink mismatch, pool failover (one leader per stream, 3 s takeover, lease transactions, timings, proxy knob dependency, stream spreading), startup checks and blocked streams, run loop, crash points, exit codes, deployment requirements (Deployment with N ≥ 2), store and lease API, test plan, open questions, acceptance criteria |
