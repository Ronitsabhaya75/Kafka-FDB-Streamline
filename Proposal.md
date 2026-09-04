# FoundationDB-Kafka Connector (Kafka-FDB-Streamline)

A robust change data capture (CDC) bridge connecting **FoundationDB** with **Apache Kafka**, enabling real-time stream consumption of FDB mutations with strong consistency guarantees.

---

## Table of Contents

- [Overview](#overview)
- [Background & Motivation](#background--motivation)
- [Proposal](#proposal)
  - [1. Directory Layer CDC Stream](#1-directory-layer-cdc-stream)
  - [2. Kafka Bridge Architecture](#2-kafka-bridge-architecture)
  - [3. Core Protobuf Schema](#3-core-protobuf-schema)
  - [4. Bridge Consumption Pattern](#4-bridge-consumption-pattern)
- [Extensions](#extensions)
  - [Domain-Specific Schemas](#domain-specific-schemas)
  - [Record Layer Integration](#record-layer-integration)
  - [Improved Directory-Topic Mapping](#improved-directory-topic-mapping)
- [Design Challenges](#design-challenges)
- [Downstream Applications](#downstream-applications)
- [References](#references)

---

## Overview

FoundationDB serves as a highly scalable, distributed, ACID-compliant transactional key-value store. While FDB excels as a primary source of truth, modern architectures often require feeding live mutation streams into analytics engines, data lakes, search indexes, and event-driven notification pipelines.

This project proposes an end-to-end bridge that streams change data capture (CDC) records from FoundationDB directories directly to Apache Kafka topics with durable delivery guarantees.

---

## Background & Motivation

FoundationDB 8.0 introduced [support for CDC](https://apple.github.io/foundationdb/), allowing consumers to efficiently consume mutations to tracked key-ranges. However:

- CDC is currently exposed primarily through C bindings (`fdb_c`).
- Raw versioned FDB mutations require application-level parsing, decoding, and version management.
- Applications often operate in higher-level languages (Python, Java, Go) and rely on abstractions like the [FDB Directory Layer](https://apple.github.io/foundationdb/developer-guide.html#directories).
- Building bespoke CDC ingestion logic per application requires substantial, repetitive engineering effort.

In contrast, consuming data streams from **Apache Kafka** is a mature, well-solved paradigm supported by a vast ecosystem of battle-tested client libraries, connectors, and stream-processing engines (e.g., Apache Flink, Kafka Streams, Spark Streaming). Bridging FDB CDC to Kafka makes FDB commit logs readily accessible to downstream consumers across any technology stack.

---

## Proposal

### 1. Directory Layer CDC Stream

The first step is adding support to the Directory Layer for creating CDC streams tailored to specific directories, extending native language bindings beyond C. 

In Python, creating and consuming directory change streams would look like:

```python
import fdb
import fdb.directory

fdb.api_version(730)
db = fdb.open()

# Open the target directory
dir = fdb.directory.open(db, ("my_data",))

# Open CDC stream for the directory
cdc_stream = await fdb.open_cdc_stream(db, dir, stream_name="my_stream")

# Continuously consume and acknowledge mutations
while True:
    mutations = await cdc_stream.consume()
    # Process mutations...
    await cdc_stream.ack()
```

---

### 2. Kafka Bridge Architecture

On top of the Directory Layer CDC, a Kafka bridge library establishes a mapping between FoundationDB directories and Kafka topics:

1. **Consume**: Continuously reads mutations from a directory's FDB CDC stream.
2. **Transform / Serialize**: Encodes versioned mutations into standardized records (e.g., Protobuf).
3. **Publish**: Writes batches to Kafka topics.
4. **Acknowledge**: Only commits progress / acknowledges (`ack()`) the FDB CDC stream **after** records have been durably acknowledged by Kafka (e.g., `acks=all`).

```
┌────────────────────────┐
│ FoundationDB Cluster   │
│  ┌──────────────────┐  │
│  │ Directory Layer  │  │
│  └────────┬─────────┘  │
└───────────┼────────────┘
            │ FDB CDC Stream
            ▼
┌────────────────────────┐
│ FDB-Kafka Bridge       │
│  - Stream Consumer     │
│  - Protobuf Serializer │
│  - Durable Publisher   │
└───────────┬────────────┘
            │ Kafka Producer (acks=all)
            ▼
┌────────────────────────┐
│ Apache Kafka           │
│  ┌──────────────────┐  │
│  │ Directory Topic  │  │
│  └────────┬─────────┘  │
└───────────┼────────────┘
            ▼
Downstream Consumers (Flink / Microservices / Analytics)
```

---

### 3. Core Protobuf Schema

Kafka records can be encoded using Protocol Buffers. Below is the general schema representing versioned FDB mutations and stream lifecycle boundaries:

```protobuf
syntax = "proto3";

package fdb.kafka.cdc;

// Tracks commit version and mutation sequence within that version
message FDBVersionIndex {
    uint64 fdb_version = 1;
    uint32 sequence_no = 2; // Orders FDB mutations at a given (stream, version)
}

// Closes a version boundary for a particular stream
message VersionEnd {
    uint64 fdb_version = 1;
}

// Represents single key mutations (SET, ADD, and atomic mutations)
message FDBSingleKeyMutation {
    enum MutationType {
        SET = 0;
        ADD = 1;
        // See https://github.com/apple/foundationdb/blob/ad61b1f40941da1633488c17bae792186625c88/bindings/c/foundationdb/fdb_c.h#L194
        // for the full list of atomic mutations supported in FDB
    }

    bytes key = 1;
    bytes value = 2;
    MutationType mutation_type = 3;
}

// Represents range clear operations
message FDBClearRange {
    bytes begin_key = 1;
    bytes end_key = 2;
}

// Union of mutation types associated with a version index
message FDBMutation {
    FDBVersionIndex version_index = 1;
    oneof mutation {
        FDBSingleKeyMutation single_key_mutation = 2;
        FDBClearRange clear_range = 3;
    }
}

// Envelope published to Kafka
message FDBMutationRecord {
    string stream_name = 1;
    oneof record {
        FDBMutation mutation = 2;
        VersionEnd version_end = 3;
    }
}
```

> **Note**: This schema is intentionally modular and can be extended to support batching multiple mutations into a single Kafka record payload for higher throughput.

---

### 4. Bridge Consumption Pattern

Using the bridge library, streaming mutations to Kafka follows a straightforward pipeline:

```python
import fdb
import fdb.directory

dir = fdb.directory.open(fdb_conn, ("my_data",))

bridge = await open_directory_cdc(
    fdb_conn, dir, stream_name="my-data-to-kafka"
)

while True:
    batch = await bridge.consume()
    await publish_to_kafka(batch)
    await bridge.ack()
```

Downstream Kafka consumers can then subscribe to the directory-associated topic and process updates reliably.

---

## Extensions

### Domain-Specific Schemas

In the generic schema, keys and values are raw byte strings. In real-world applications, values stored in FDB are frequently serialized Protobuf messages. If atomic operations are not used, the bridge library can deserialize raw values directly and emit domain-specific Kafka records.

For example, given an employee directory where keys encode employee IDs and values represent an `Employee`:

```protobuf
syntax = "proto3";

message Employee {
    string name = 1;
    string department = 2;
    uint32 salary = 3;
}
```

If only upserts and single-employee deletions are required, the bridge can emit simplified, strongly-typed records:

```protobuf
syntax = "proto3";

message EmployeeMutation {
    FDBVersionIndex fdb_version_index = 1;
    uint32 employee_id = 2;
    optional Employee new_data = 3; // Present on upsert, absent/null on deletion
}

message EmployeeMutationRecord {
    string stream_name = 1;
    oneof record {
        EmployeeMutation mutation = 2;
        VersionEnd version_end = 3;
    }
}
```

This drastically reduces decoding boilerplate for downstream consumers. Alternative formats (e.g., Avro, JSON Schema) can be evaluated similarly.

---

### Record Layer Integration

For applications leveraging the [FDB Record Layer](https://github.com/FoundationDB/fdb-record-layer) (a schema and secondary index abstraction built on top of the directory layer, currently supported in Java), the bridge can be extended to consume directly from Record Layer databases rather than raw directory byte ranges.

---

### Improved Directory-Topic Mapping

- **Commit-Version Ordering**: Updates for a single directory can be published in FDB commit-version order.
- **Cross-Directory Atomicity**: Consumers reading across multiple directory topics will not automatically observe cross-directory transactions atomically or share a unified commit order. Guaranteeing cross-directory consistency requires coordinated stream merging and explicit commit version boundaries (`VersionEnd`).
- **Configurable Partitioning & Topics**: Users may configure multiple Kafka topics or topic partitions within a single directory (e.g., partitioned by specific serialized fields), trading strict total ordering for higher horizontal write throughput.

---

## Design Challenges

| Challenge | Key Considerations |
| :--- | :--- |
| **Exactly-Once Delivery** | How to guarantee end-to-end exactly-once semantics between FDB and Kafka? What happens if a Kafka ACK is dropped due to a network glitch, or if the bridge process crashes mid-batch? Requires idempotent producers or transactional Kafka writes aligned with FDB CDC ACK boundaries. |
| **High Write Throughput** | If mutation throughput to a single FDB directory exceeds the ingestion capacity of a single bridge process, how can work be parallelized without sacrificing order? |
| **Concurrent Stream Consumers** | What happens when multiple bridge processes attempt to consume the same FDB CDC stream simultaneously? Requires leader election, partition assignments, or cooperative rebalancing. |
| **Arbitrary Schema Evolution** | How can diverse application-level schemas, serialization formats, and schema evolution (e.g., schema registries) be accommodated without adding disproportionate complexity to the core bridge library? |

---

## Downstream Applications

With the FDB-Kafka bridge in place, numerous downstream workflows become possible:

- **Batch Analytics & Data Warehousing**:
  - Combined with an initial snapshot/backfill, ingest the CDC stream into analytical columnar stores (e.g., ClickHouse, Snowflake, BigQuery) to query consistent snapshots with high-performance OLAP queries without putting load on FDB.
- **Streaming Analytics**:
  - Connect Apache Flink or Kafka Streams to process real-time event streams, compute sliding-window aggregations, and detect patterns prior to landing data into analytical stores.
- **Event-Driven Notifications & Services**:
  - Trigger downstream microservices, webhooks, or push notifications immediately when specific entities or fields mutate in FoundationDB.
- **Search Indexing**:
  - Continuously update search engines (e.g., Elasticsearch, OpenSearch) in near-real-time directly from FDB updates.

Any use case in the Kafka ecosystem becomes immediately available with FoundationDB as the authoritative, strongly-consistent source of truth.

---

## References

- [FoundationDB Documentation](https://apple.github.io/foundationdb/)
- [FoundationDB Directory Layer](https://apple.github.io/foundationdb/developer-guide.html#directories)
- [FoundationDB Atomic Mutations (`fdb_c.h`)](https://github.com/apple/foundationdb/blob/ad61b1f40941da1633488c17bae792186625c88/bindings/c/foundationdb/fdb_c.h#L194)
- [FoundationDB Record Layer](https://github.com/FoundationDB/fdb-record-layer)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
