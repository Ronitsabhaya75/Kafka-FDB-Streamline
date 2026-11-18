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

## Watching a Directory Layer tree

The bridge can resolve an existing Directory Layer path, optionally with every descendant, into one multi-range CDC stream on FoundationDB 8.0:

```python
from fdb_kafka_bridge.cdc import DirectoryCDCResolver, FDBClient, FDBMutationListener

client = FDBClient()
resolved = DirectoryCDCResolver(client.open(), ("my_application",), recursive=True).resolve()
listener = FDBMutationListener(client, "my-application-cdc", key_ranges=resolved.key_ranges)
resolved.path_for_key(raw_key)  # logical directory owning a mutation key, or None
```

As a daemon: `python -m fdb_kafka_bridge.bridge.cli --cdc-stream-name my-application-cdc --cdc-directory-path '["my_application"]' --cdc-directory-recursive --cdc-cursor-file cursor.json`. The tree is read once at startup. The daemon requires a cursor file (or a supplied `CheckpointStore`), and every checkpoint carries a fingerprint that refuses to resume after the tree changes. Details: [CDC spec §2.5](docs/CDC-CONSUMER-SPEC.md) and [Bridge spec, Directory mode](docs/BRIDGE-PIPELINE-SPEC.md#directory-mode).

## Install

```bash
pip install .
```

Installs the `fdb-kafka-bridge` distribution, which provides the import packages `fdb_kafka_bridge` (subpackages `bridge`, `cdc`, `kafka`, `serialization`) and `fdbkafka` (generated Protobuf).

`libfdb_c` is a system requirement that pip does not install. The `foundationdb` Python package depends on it at runtime: `import fdb` succeeds without it, but `fdb.api_version()` raises `Unable to locate the FoundationDB API shared library!`. Install the client library from the [FoundationDB release](https://github.com/apple/foundationdb/releases) that matches `FDB_VERSION` in the [`Dockerfile`](Dockerfile), which shows the Linux setup.

## Contributing

Repo layout, branching, code style and PR workflow: [`docs/conventions.md`](docs/conventions.md). Coding-agent guidance: [`AGENTS.md`](AGENTS.md).
