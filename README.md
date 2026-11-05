# Kafka-FDB-Streamline

Streams changes from a FoundationDB directory to a Kafka topic using FDB's native CDC. See [`Proposal.md`](Proposal.md) for scope and design.

## Getting started

```bash
make setup && source .venv/bin/activate
make lint
```

Requires Python 3.12. Commits must be signed — see [Signed commits](docs/conventions.md#signed-commits).

Kafka producer behavior and local verification are documented in the
[`Kafka producer specification`](docs/KAFKA-PRODUCER-SPEC.md).

## Install

```bash
pip install .
```

Installs the `fdb-kafka-bridge` distribution, which provides the import packages `fdb_kafka_bridge` (subpackages `bridge`, `cdc`, `kafka`, `serialization`) and `fdbkafka` (generated Protobuf).

`libfdb_c` is a system requirement that pip does not install. The `foundationdb` Python package depends on it at runtime: `import fdb` succeeds without it, but `fdb.api_version()` raises `Unable to locate the FoundationDB API shared library!`. Install the client library from the [FoundationDB release](https://github.com/apple/foundationdb/releases) that matches `FDB_VERSION` in the [`Dockerfile`](Dockerfile), which shows the Linux setup.

## Run the durable bridge

```bash
streamline-daemon --stream-name orders --subspace-prefix orders: \
  --kafka-bootstrap-servers localhost:9092
```

The CLI publishes to `fdb.cdc.orders` and saves a sink-checked FDB cursor before
acknowledging each delivered reply. Configuration, recovery and live tests:
[`FDB checkpoints and CLI`](docs/FDB-CHECKPOINT-SPEC.md).

## Contributing

Repo layout, branching, code style and PR workflow: [`docs/conventions.md`](docs/conventions.md). Coding-agent guidance: [`AGENTS.md`](AGENTS.md).
