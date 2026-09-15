# Kafka-FDB-Streamline

Streams changes from a FoundationDB directory to a Kafka topic using FDB's native CDC. See [`Proposal.md`](Proposal.md) for scope and design.

## Getting started

```bash
make setup && source .venv/bin/activate
make lint
```

Requires Python 3.12. Commits must be signed — see [Signed commits](docs/conventions.md#signed-commits).

## Contributing

Repo layout, branching, code style and PR workflow: [`docs/conventions.md`](docs/conventions.md). Coding-agent guidance: [`AGENTS.md`](AGENTS.md).
