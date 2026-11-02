# Kafka-FDB-Streamline (`fdb-kafka-bridge`)

A high-performance, durable change data capture (CDC) bridge connecting **FoundationDB 8.0** to **Apache Kafka**. It continuously captures live mutations from FoundationDB directory subspaces, serializes mutation records and version boundaries into Protocol Buffers envelopes, and publishes them to Kafka topics with strict delivery, ordering, and retention guarantees.

---

## Architecture

The bridge operates as an autonomous streaming pipeline. It coordinates between FoundationDB's native transaction log (TLog) CDC streaming subsystem, a pure zero-copy Protobuf serialization layer, and a hardened librdkafka producer.

```mermaid
flowchart TD
    subgraph FDB ["FoundationDB 8.0 Cluster (API 800)"]
        Dir["Directory Subspace<br/><code>[rawPrefix, strinc(rawPrefix))</code>"]
        TLog["Native CDC Stream<br/><code>(TLog Mutation Log)</code>"]
        Dir -->|Key-range commits| TLog
    end

    subgraph Bridge ["fdb-kafka-bridge Daemon"]
        Listener["<b>FDBMutationListener</b><br/><code>poll_records()</code><br/>API 800 Gated • Cursor Reconcile"]
        Serializer["<b>Serialization Engine</b><br/><code>serialize_version_group()</code><br/>Batch Slicing • Varint Bound Check"]
        Producer["<b>MutationProducer</b><br/><code>confluent-kafka</code><br/>acks=all • Idempotence • Gapless"]
        Cursor["<b>Atomic Cursor Store</b><br/><code>_PersistedCursor.save()</code><br/>SHA-256 Verified • fsync replace"]

        Listener -->|Native CDC reply| Serializer
        Serializer -->|FDBMutationRecord stream| Producer
        Producer -->|Confirmed broker delivery| Cursor
        Cursor -->|fsync complete| Listener
    end

    subgraph Kafka ["Apache Kafka Cluster"]
        Topic["Directory Topic<br/><code>Key = stream_name</code><br/>Partition-Ordered"]
    end

    subgraph Downstream ["Downstream Consumers"]
        ConsumerApp["Stream Processors (Flink / Spark)<br/>Search Indexes (OpenSearch)<br/>OLAP Data Warehouses (ClickHouse)"]
    end

    TLog -->|consume()| Listener
    Producer -->|Kafka produce (acks=all / txn commit)| Topic
    Listener -.->|acknowledge() advances watermark| TLog
    Topic --> ConsumerApp
```

### Pipeline Execution Step

Every step processed by [`BridgePipeline.step`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/bridge/pipeline.py#L89) follows an invariant, fail-safe execution order:

```
[1. FDB CDC Poll] ──> [2. Protobuf Slicing] ──> [3. Kafka Produce & Flush] 
                  ──> [4. Atomic Cursor fsync] ──> [5. FDB Retention Acknowledge]
```

1. **Poll CDC**: [`FDBMutationListener`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/cdc/listener.py#L65) fetches the next mutation reply from FoundationDB.
2. **Serialize & Batch**: [`serialize_version_group`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/serialization/version_group.py#L32) slices mutations into bounded [`FDBMutationBatch`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/protobuf/proto/fdbkafka/cdc/v1/mutations.proto#L86) records up to `max_record_bytes`, terminating each version with a [`VersionEnd`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/protobuf/proto/fdbkafka/cdc/v1/mutations.proto#L21) audit marker.
3. **Publish to Kafka**: [`MutationProducer`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/kafka/producer.py#L87) produces all records using the stream name as the partition key. It either blocks on `flush_or_raise()` (idempotent mode) or completes a 2-phase commit transaction (transactional mode).
4. **Checkpoint Cursor**: [`_PersistedCursor`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/bridge/pipeline.py#L393) writes the current `last_consumed_version` to a temporary file, fsyncs the file, atomically renames it via `os.replace`, and fsyncs the parent directory.
5. **Acknowledge FDB**: The listener calls native `acknowledge()`, moving FDB's cumulative retention watermark forward so the cluster can safely discard obsolete TLog history.

---

## Quickstart: Docker-Based Demo

A complete demonstration is included in [`demo/run_demo.py`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/demo/run_demo.py). It runs against real, containerized FoundationDB 8.0 (native CDC enabled) and Apache Kafka 3.9.0 instances—no browser, mocks, or simulated network calls required.

### 1. Start the Environment

```bash
# Launch FoundationDB 8.0 + native CDC and Apache Kafka 3.9.0 (KRaft)
docker compose -f .devcontainer/docker-compose.yml up -d --build

# Install the bridge distribution in editable mode
docker compose -f .devcontainer/docker-compose.yml exec app \
  pip3 install --break-system-packages -e .
```

### 2. Run the 10,000-Mutation End-to-End Demo

```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
  env PYTHONPATH=.:src:protobuf/gen python3 demo/run_demo.py --count 10000
```

### What the Demo Does:
1. **Connects to FoundationDB** with native CDC API version 800.
2. **Initializes the Bridge Daemon** with transactional Kafka publishing and local disk cursor checkpointing.
3. **Generates Upstream Workload**: Commits 10,000 representative writes and atomic mutations across 16 FDB operations (`SET_VALUE`, `CLEAR_RANGE`, `ADD`, `BIT_AND`, `APPEND_IF_FITS`, `MAX`, `MIN`, `SET_VERSIONSTAMPED_KEY`, etc.).
4. **Streams Through the Bridge**: Captures CDC records, encodes them to Protobuf wire format, flushes to Kafka broker with `acks=all`, checkpoints cursor, and advances FDB watermarks.
5. **Consumes from Kafka**: Uses a standard Kafka consumer to read the emitted topic records.
6. **Byte-for-Byte Verification**: Audits every mutation, key, value, sequence number, and version boundary against the committed upstream dataset.
7. **Clean Termination**: Drains remaining queues, safely halts the bridge daemon, and displays operational throughput and latency statistics.

---

## Delivery Guarantees & Consistency Model

The bridge provides **at-least-once durable delivery** with strict total ordering per directory stream.

### 1. Invariant Producer Durability Settings

[`MutationProducer`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/kafka/producer.py#L87) enforces non-negotiable configuration settings that cannot be overridden by callers:

```python
acks = "all"                                   # Requires full in-sync replica acknowledgement
enable.idempotence = True                      # Prevents duplicate broker writes from retries
max.in.flight.requests.per.connection = 5     # Supported by librdkafka with idempotence enabled
enable.gapless.guarantee = True                # Halts rather than delivering messages out-of-order
message.timeout.ms = 0                         # Infinite timeout; prevents gaps during network outages
```

### 2. Publishing Modes

* **Idempotent Mode**: Delivers records in batches. Any delivery failure triggers a **sticky error state**—the producer permanently raises on subsequent `produce()` and `flush()` attempts until replaced, ensuring out-of-order records can never be enqueued.
* **Transactional Mode**: When configured with `transactional_id`, batches are published inside a Kafka transaction. Downstream consumers configured with `isolation.level=read_committed` observe each FDB commit version atomically.

### 3. Failure Matrix & Recovery Semantics

| Failure Scenario | System State at Failure | Recovery Behavior | Guarantee |
| :--- | :--- | :--- | :--- |
| **Kafka broker drop during produce/flush** | Producer fails or transaction aborts. FDB is **never acknowledged**. | Bridge closes producer and resets listener. On retry, reads CDC stream from prior acknowledged cursor. | **At-least-once**: Zero data loss; uncommitted messages are re-sent. |
| **Crash after Kafka ACK, before cursor disk sync** | Kafka holds records; disk cursor is not updated; FDB unacknowledged. | Bridge reboots from previous durable cursor. Re-polls and re-produces the batch to Kafka. | **At-least-once**: Consumers may observe duplicate records. Duplicates are identified by `(fdb_version, sequence_no)`. |
| **Crash after cursor disk sync, before FDB ACK** | Kafka holds records; cursor file is updated; FDB retention watermark uncommitted. | On restart, [`FDBMutationListener`](file:///Users/ronitsabhaya/Desktop/new_app/Kafka-FDB-Streamline/src/fdb_kafka_bridge/cdc/listener.py#L158) restores the cursor, waits for cluster read version, and re-acknowledges FDB. | **At-least-once / Restored Watermark**: FDB watermark catches up without duplicate transmission. |
| **Downstream Deduplication** | Downstream consumer restart. | Every mutation envelope contains monotonic coordinates: `fdb_version` (uint64) and `sequence_no` (uint32). | Downstream stores can achieve **exactly-once state updates** via versioned upserts. |

---

## Benchmark Results (v0.1.0)

End-to-end benchmarks were executed against live containerized FoundationDB 8.0 and Apache Kafka 3.9.0 stacks using [`benchmarks/0.1.0v/bench_e2e.py`](benchmarks/0.1.0v/bench_e2e.py).

### Performance Summary

| Metric | Result | Engineering Significance |
| :--- | :--- | :--- |
| **Peak Unthrottled Throughput** | **56,683 mut/s** (8.00 MB/s wire) | Single-threaded writer saturated without pipeline queuing (backlog slope: -52 mut/s). |
| **10-Minute Sustained Ingestion** | **9,450,500 mutations** (15,000 mut/s) | Steady-state window with **`+0.008 mut/s`** backlog slope and **0.02s** post-test drain. |
| **Commit-to-Ack Latency** | **p50 = 11.8 ms**, **p90 = 15.2 ms**, **p99 = 46.3 ms** | Wall-clock time from FDB `tr.commit()` to Kafka broker ACK (`acks=all`). |
| **CDC Read-Head Lag** | **861 versions (~0.86 ms)** mean | Polling loop tracks within milliseconds of FoundationDB's live commit head. |
| **Full Bridge Checkpoint Overhead** | **15,917 disk fsyncs** | Autonomous daemon maintained 15k mut/s with disk fsync per batch with **0 errors**. |
| **Stream Integrity Audit (`--verify`)** | **0 duplicates, 0 missing, 0 order errors** | Monotonic ordering verified across 1,050,500 consecutive mutations. |

### Stage Breakdown Latencies (15,000 mut/s Ingestion)

Linear stage durations within the core bridge loop:

$$\text{Stage Duration: } \text{Poll} \approx 0.72\text{ ms} \quad|\quad \text{Produce} \approx 0.02\text{ ms} \quad|\quad \text{Broker Flush} \approx 0.17\text{ ms} \quad|\quad \text{FDB Ack} \approx 1.86\text{ ms}$$

### How to Reproduce Benchmarks

All benchmarks run inside the project container environment:

```bash
# 1. 10-Minute Sustained Load Test (15,000 mut/s)
docker compose -f .devcontainer/docker-compose.yml exec app \
  python3 benchmarks/0.1.0v/bench_e2e.py \
    --kafka-bootstrap-servers kafka:19092 \
    --duration 600 --warmup 30 --target-rate 15000

# 2. Autonomous Bridge Test with Atomic Disk Checkpointing
docker compose -f .devcontainer/docker-compose.yml exec app \
  python3 benchmarks/0.1.0v/bench_e2e.py \
    --use-bridge \
    --kafka-bootstrap-servers kafka:19092 \
    --duration 60 --warmup 10 --target-rate 15000

# 3. Stream Ordering & Integrity Verification Test
docker compose -f .devcontainer/docker-compose.yml exec app \
  python3 benchmarks/0.1.0v/bench_e2e.py \
    --kafka-bootstrap-servers kafka:19092 \
    --duration 60 --warmup 10 --target-rate 15000 \
    --key-pool 0 --verify

# 4. Unthrottled Saturation Test
docker compose -f .devcontainer/docker-compose.yml exec app \
  python3 benchmarks/0.1.0v/bench_e2e.py \
    --kafka-bootstrap-servers kafka:19092 \
    --duration 30 --warmup 5 --target-rate 0
```

Full benchmark documentation and raw JSON run results are available in [`benchmarks/0.1.0v/README.md`](benchmarks/0.1.0v/README.md).

---

## Limitations & Operational Considerations

While Kafka-FDB-Streamline provides high throughput and rigorous durability, operators should consider several technical constraints:

### 1. Experimental FoundationDB Native CDC Support
* **Unreleased Upstream FDB Version**: Native CDC is a new feature introduced in **FoundationDB 8.0**, which is currently an unreleased development branch.
* **API Version 800 Gate**: CDC requires initializing FoundationDB with `fdb.api_version(800)`. Selecting API version 800 hard-gates the cluster and disables specific legacy features, including Tenants, Metaclusters, Blob Granules, Storage Cache Servers, and Dynamic Knobs.
* **Python Bindings ABI Risk**: Native CDC Python bindings depend on Apple FoundationDB pull requests [#13925](https://github.com/apple/foundationdb/pull/13925) and [#13971](https://github.com/apple/foundationdb/pull/13971). Both PRs redefine CDC symbols at API version 800, introducing possible ABI instability prior to official upstream merge.

### 2. Transaction Log (TLog) Disk Retention Pressure
* **Indefinite Retention**: FoundationDB CDC streams do not employ time-based log expiration. The cluster's Transaction Logs retain every mutation until the bridge explicitly issues an `acknowledge()`.
* **Cluster Stalling Hazard**: If a bridge daemon crashes or falls behind indefinitely without an active consumer advancing the watermark, transaction logs will consume available disk space. When TLog disks reach **5% free space**, FoundationDB halts all incoming transactions cluster-wide. Production deployments **must** configure alerts for consumer lag and TLog disk utilization.

### 3. Oversized Commit Versions (`server_overloaded` 1211)
* FDB's native CDC stream transfers data through internal ring buffers. If a single commit version contains a transaction so massive that it exceeds the CDC buffer limit, the FDB proxy raises error `1211: server_overloaded`.
* Native CDC does not currently provide an API mechanism to skip an unprocessable commit version. An oversized commit version will permanently stall the CDC consumer stream until manually cleared or re-registered.

### 4. Partitioning & Atomicity Boundaries
* **Per-Stream Ordering**: Ordering is strictly preserved within an individual registered CDC stream (typically one FoundationDB directory subspace).
* **Single Partition Routing**: Batches for a stream default to using the stream name as the Kafka record partition key, ensuring all mutations land on the same Kafka partition. Splitting a directory stream across multiple partitions increases horizontal throughput but sacrifices global linear ordering. Cross-directory transactions cannot be observed atomically without multi-stream consumer synchronization.

---

## Local Development & Testing

### Installation

Requires **Python 3.12** and the FoundationDB native client library (`libfdb_c.so` on Linux or `libfdb_c.dylib` on macOS):

```bash
# Set up virtual environment and install dependencies
make setup
source .venv/bin/activate

# Run code formatters and linters (ruff + black)
make lint

# Run the test suite (639 unit and hypothesis property tests)
make test
```

### Packaging

The project ships as distribution `fdb-kafka-bridge`, providing two import packages:
* `fdb_kafka_bridge`: Core bridge engine (`bridge`, `cdc`, `kafka`, `serialization`).
* `fdbkafka`: Protobuf gencode compiled via Buf (`fdbkafka.cdc.v1.mutations_pb2`).

```bash
pip install .
```

To test wheel distribution packaging in an isolated virtual environment:

```bash
make test-packaging
```

---

## Contributing & References

* **Project Context & Proposal**: [`Proposal.md`](Proposal.md)
* **Documentation Map**: [`docs/INDEX.md`](docs/INDEX.md)
* **Code Conventions & Signing**: [`docs/conventions.md`](docs/conventions.md)
* **Agent Guidelines**: [`AGENTS.md`](AGENTS.md)
* **PR Review Skill**: [`.agents/skills/review-streamline-pr/SKILL.md`](.agents/skills/review-streamline-pr/SKILL.md)
