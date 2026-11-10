# Docs index

Map of the project's docs. Check here before reading docs wholesale, then open only what the task needs. Adding or substantially changing a doc means updating its row.

**Status:** *Normative* docs define what the code must do; code and tests must match them. *Research* and *Historical* docs record investigation or original intent and may lag the code; where they disagree, the normative doc wins. See [Specs](conventions.md#specs).

## Component specs

| Doc | Status | Read it for |
|-----|--------|-------------|
| [`CDC-CONSUMER-SPEC.md`](CDC-CONSUMER-SPEC.md) | Normative | `fdb_kafka_bridge.cdc`: FDB client, CDC stream lifecycle, polling, resume and acknowledgement, opt-in idle-reply ack coalescing. |
| [`SERIALIZATION-SPEC.md`](SERIALIZATION-SPEC.md) | Normative | `fdb_kafka_bridge.serialization`: public API, wire mapping, errors, version-group reader contract. |
| [`KAFKA-PRODUCER-SPEC.md`](KAFKA-PRODUCER-SPEC.md) | Normative | `fdb_kafka_bridge.kafka.MutationProducer`: producer contract and local Kafka verification. |
| [`BRIDGE-PIPELINE-SPEC.md`](BRIDGE-PIPELINE-SPEC.md) | Normative | `fdb_kafka_bridge.bridge`: `BridgePipeline`, `Bridge` daemon, config, cursor, recovery and shutdown. |

## Project and process

| Doc | Status | Read it for |
|-----|--------|-------------|
| [`../AGENTS.md`](../AGENTS.md) | Normative | Agent guidance: pre-finish checks, comment rules, review checklist. `CLAUDE.md` and `.cursor/rules/` load it. |
| [`conventions.md`](conventions.md) | Normative | Repo layout, branching, specs, code style, local setup (incl. Trello MCP), PRs, CI checks. |
| [PR review skill](../.agents/skills/review-streamline-pr/SKILL.md) | Normative | Agent workflow for reviewing a PR and drafting source-linked feedback. |
| [Doc sync skill](../.agents/skills/doc-sync/SKILL.md) | Normative | Agent workflow for syncing docs with a branch diff before a PR: owning-doc updates, new specs, verifying touched claims against the implementation, code/spec contradictions. |
| [`../demo/README.md`](../demo/README.md) | Normative | Running the live 10,000-mutation FDB → Kafka demo. |
| [`../Proposal.md`](../Proposal.md) | Historical | Sponsor proposal: scope, goals, original architecture; its as-built banner lists what changed. |

## Research

| Doc | Status | Read it for |
|-----|--------|-------------|
| [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md) | Research | FDB native CDC internals: stream lifecycle, ack/resume semantics, Python API, limitations, ABI risks. |
| [`kafka-delivery-guarantees.md`](kafka-delivery-guarantees.md) | Research | Kafka delivery semantics, exactly-once strategies, error handling, Python client comparison. |
| [`protobuf_schema_validation.md`](protobuf_schema_validation.md) | Historical | Pre-implementation protobuf schema review and evolution rules; superseded by `mutations.proto` and the serialization spec. |
| [`FDB-DIRECTORY-LAYER-KEY-RANGE-MAPPING.md`](FDB-DIRECTORY-LAYER-KEY-RANGE-MAPPING.md) | Research | FDB directory layer, subspaces and key ranges, and how they map onto bridge key ranges and topics. |
| [`../benchmarks/0.1.0v/README.md`](../benchmarks/0.1.0v/README.md) | Research | v0.1.0 end-to-end benchmarks: 10-min sustained run, unthrottled throughput, checkpoint durability, CDC ordering checks. |
