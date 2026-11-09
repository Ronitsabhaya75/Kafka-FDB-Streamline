# Kafka-FDB-Streamline

Streams changes in a FoundationDB key range (configured as hex `begin_key`/`end_key`, e.g. a directory's prefix range) to a Kafka topic using FDB's native CDC. See [`Proposal.md`](Proposal.md) for scope; the as-built design is in the specs listed in [`docs/INDEX.md`](docs/INDEX.md).

## Getting started

```bash
make setup && source .venv/bin/activate
make lint
make test        # unit tests; FDB and Kafka integration tests skip without a live stack
```

Requires Python 3.12. Commits must be signed — see [Signed commits](docs/conventions.md#signed-commits).

Docs map: [`docs/INDEX.md`](docs/INDEX.md). Live end-to-end demo against the Docker
Compose FDB + Kafka stack: [`demo/README.md`](demo/README.md).

## Install

```bash
pip install .
```

Installs the `fdb-kafka-bridge` distribution, which provides the import packages `fdb_kafka_bridge` (subpackages `bridge`, `cdc`, `kafka`, `serialization`) and `fdbkafka` (generated Protobuf).

`libfdb_c` is a system requirement that pip does not install. The `foundationdb` Python package depends on it at runtime: `import fdb` succeeds without it, but `fdb.api_version()` raises `Unable to locate the FoundationDB API shared library!`. Install the client library from the [FoundationDB release](https://github.com/apple/foundationdb/releases) that matches `FDB_VERSION` in the [`Dockerfile`](Dockerfile), which shows the Linux setup.

## Contributing

Repo layout, branching, code style and PR workflow: [`docs/conventions.md`](docs/conventions.md). Coding-agent guidance: [`AGENTS.md`](AGENTS.md).
