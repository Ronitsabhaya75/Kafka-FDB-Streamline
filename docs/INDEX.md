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
| [`BRIDGE-PIPELINE-SPEC.md`](BRIDGE-PIPELINE-SPEC.md) | `src.bridge` contract: `BridgePipeline` single-step coordinator and `Bridge` autonomous daemon (configuration, durable cursor persistence, recovery, and metrics). |
| [`CDC-CONSUMER-SPEC.md`](CDC-CONSUMER-SPEC.md) | `src/cdc/` contract: FDBClient, stream lifecycle, bounded record polling, safe resume and acknowledgement |
| [`../demo/README.md`](../demo/README.md) | Live 10,000-mutation bridge demo: prerequisites, Docker Compose setup, CLI flags. |
