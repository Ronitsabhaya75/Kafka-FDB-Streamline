# Docs index

Map of the project's docs. Check here before reading docs wholesale, then open only what the task needs. Adding or substantially changing a doc means updating its row.

| Doc | Read it for |
|-----|-------------|
| [`../Proposal.md`](../Proposal.md) | Project scope, goals, architecture, sponsor requirements |
| [`conventions.md`](conventions.md) | Repo layout, branching, code style, local setup (incl. Trello MCP), PR rules |
| [`CDC-DEEP-DIVE.md`](CDC-DEEP-DIVE.md) | FDB native CDC: stream lifecycle, ack/resume semantics, Python API (#13925), limitations, API version 800 gate, Kafka key/partitioning, #13925/#13971 ABI risk. Its TL;DR table links each section. |
| [`SERIALIZATION-SPEC.md`](SERIALIZATION-SPEC.md) | `src/serialization/` contract: public API, wire mapping, input rules, errors and decode failures, reader contract for a version group |
