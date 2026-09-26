# Kafka Delivery Guarantees, Producer Configuration, and Exactly-Once Semantics
## Research for the FoundationDB CDC → Apache Kafka Bridge

**Project:** Kafka-FDB Streamline  
**Focus:** Kafka producer durability, ordering, transactions, exactly-once semantics, Python client selection, batching, error handling, and Protobuf serialization.

---

# 1. Why This Research Matters

Our bridge moves committed FoundationDB (FDB) CDC mutations into Apache Kafka.

```text
FoundationDB
    ↓
Native CDC
    ↓
Bridge
    ↓
Serialize mutation as Protobuf
    ↓
Kafka Producer
    ↓
Kafka Topic
    ↓
Downstream Consumers
```

The difficult part is not simply sending a message to Kafka.

The key question is:

> **When is it safe to tell FoundationDB that a CDC mutation has been processed?**

The required order is:

```text
1. Read mutations from FDB CDC
        ↓
2. Serialize them
        ↓
3. Publish them to Kafka
        ↓
4. Commit the Kafka transaction
        ↓
5. Confirm Kafka committed successfully
        ↓
6. Only then acknowledge progress back to FDB
```

The bridge must never acknowledge FDB before Kafka has committed the corresponding records, because a crash between those two steps could permanently lose mutations.

---

# 2. Kafka Delivery Semantics

Kafka delivery behavior is commonly described as:

```text
At-most-once
At-least-once
Exactly-once
```

## At-most-once

A record is delivered zero or one times.

Possible result:

```text
A
B
D
```

Record `C` may have been lost.

This is not acceptable for our CDC bridge.

## At-least-once

A record is delivered one or more times.

Possible result:

```text
A
B
C
C
D
```

No data is lost, but duplicates may occur.

FDB CDC itself behaves like an at-least-once source because unacknowledged mutations may be replayed after restart.

## Exactly-once

The desired consumer-visible result is:

```text
A
B
C
D
```

even if retries or crashes occur.

Kafka's exactly-once building blocks include:

- idempotent producers,
- transactions,
- stable `transactional.id` values,
- transaction commit/abort,
- and consumers using `isolation.level=read_committed`.

Exactly-once is not achieved by `acks=all` alone.

---

# 3. Understanding `acks`

The producer property:

```text
acks
```

controls how many broker acknowledgements are required before a produce request is considered successful.

Important values:

```text
acks=0
acks=1
acks=all
```

---

# 4. `acks=0`

With:

```text
acks=0
```

the producer does not wait for any broker acknowledgement.

```text
Producer
   ↓
send bytes
   ↓
consider request sent
```

### Advantages

- lowest latency,
- highest possible throughput,
- minimal acknowledgement overhead.

### Disadvantages

- records can be silently lost,
- broker failures may not be detected,
- the producer cannot safely infer durable receipt.

### Project suitability

**Not acceptable.**

The bridge cannot safely acknowledge an FDB cursor based on a fire-and-forget Kafka send.

---

# 5. `acks=1`

With:

```text
acks=1
```

the partition leader writes the record to its local log and responds without waiting for all required in-sync replicas.

```text
Producer
    ↓
Leader stores record
    ↓
ACK
    ↓
Followers replicate afterward
```

A failure can occur in this window:

```text
Leader stores A
    ↓
Leader ACKs producer
    ↓
Leader crashes
    ↓
Follower had not replicated A
```

The producer already received success, but the record can still disappear.

### Project suitability

**Not strong enough** for our required durability.

---

# 6. `acks=all`

With:

```text
acks=all
```

the leader waits for the required in-sync replicas before returning success.

```text
                 ┌→ Replica 1
Producer → Leader├→ Replica 2
                 └→ Replica 3
                       ↓
             replication requirement met
                       ↓
                      ACK
```

This is Kafka's strongest normal producer acknowledgement mode.

### Recommendation

Use:

```text
acks=all
```

for the FDB → Kafka bridge.

---

# 7. `acks=all` Is Not Exactly-Once

`acks=all` answers:

> How durable must this produce request be before Kafka calls it successful?

It does not solve:

- duplicate retries,
- process restarts,
- replay from FDB,
- atomic publication of several records,
- stale producer instances.

Therefore it is necessary, but not sufficient, for exactly-once behavior.

---

# 8. The Retry Problem

Suppose:

```text
Producer sends A
      ↓
Kafka stores A
      ↓
Kafka sends ACK
      X
network response is lost
```

The producer does not know whether A was stored.

A simple retry can result in:

```text
A
A
```

Kafka idempotence addresses this producer-retry problem.

---

# 9. Idempotent Producers

Kafka supports:

```text
enable.idempotence=true
```

An idempotent producer prevents duplicates caused by producer-level retries.

Conceptually:

```text
ProducerID=42
Sequence=17
Record=A
```

If the same producer retry sends the same sequence again, Kafka can detect that it was already accepted.

Instead of:

```text
A
A
```

Kafka keeps:

```text
A
```

---

# 10. Idempotence Configuration Requirements

Kafka requires compatible settings:

```text
enable.idempotence=true
acks=all
retries > 0
max.in.flight.requests.per.connection <= 5
```

For a conservative initial configuration:

```text
max.in.flight.requests.per.connection=1
```

This is very easy to reason about.

Later, with idempotence enabled, values up to 5 can preserve ordering while increasing throughput.

---

# 11. Idempotence Does Not Solve FDB Replay by Itself

Consider:

```text
FDB mutation A
      ↓
Bridge
      ↓
Kafka stores A
      ↓
Bridge crashes
      ↓
FDB was never acknowledged
```

After restart:

```text
FDB replays A
      ↓
Bridge may publish A again
```

Producer idempotence is mainly about retries within the Kafka producer protocol/session.

It does not automatically make an external FDB replay exactly-once.

For that, our bridge needs Kafka transactions plus durable cursor coordination.

---

# 12. Kafka Transactions

Kafka supports transactional producers using:

```text
transactional.id
```

Example:

```text
transactional.id=fdb-cdc-users
```

A producer can then execute:

```text
begin_transaction()

produce(record 1)
produce(record 2)
produce(record 3)

commit_transaction()
```

The records become visible atomically to consumers configured with:

```text
isolation.level=read_committed
```

If the transaction fails:

```text
abort_transaction()
```

and `read_committed` consumers do not treat those records as committed output.

---

# 13. Why `transactional.id` Matters

A stable `transactional.id` identifies the logical producer across restarts.

It also supports **fencing** stale producers.

Example:

```text
Bridge A
transactional.id = "fdb-users"
```

Bridge A becomes disconnected.

A standby starts:

```text
Bridge B
transactional.id = "fdb-users"
```

Kafka can fence the older producer instance so both do not successfully commit for the same stream.

That aligns directly with our single-writer / failover requirements.

---

# 14. Transaction Lifecycle with `confluent-kafka`

Conceptually:

```python
producer = Producer({
    "bootstrap.servers": "...",
    "transactional.id": "fdb-users",
    "enable.idempotence": True,
    "acks": "all",
})

producer.init_transactions()
producer.begin_transaction()

producer.produce(...)

producer.commit_transaction()
```

If a transaction must be aborted:

```python
producer.abort_transaction()
```

The client exposes transaction-aware error handling that helps distinguish:

```text
retriable
abortable
fatal
```

conditions.

---

# 15. What Kafka Exactly-Once Semantics Mean

Kafka transactions make a set of records atomically visible.

Example:

```text
Transaction 1:
A
B
C
COMMIT
```

A `read_committed` consumer sees:

```text
A
B
C
```

If:

```text
Transaction 2:
D
E
ABORT
```

a `read_committed` consumer does not expose D and E as committed results.

Exactly-once therefore depends on both the producer and consumer contract.

---

# 16. Exactly-Once for Our FDB → Kafka Bridge

FDB and Kafka do not share one distributed transaction.

Our bridge therefore needs a protocol.

The requirements propose committing:

```text
CDC mutation records
+
CDC cursor/progress record
```

inside the same Kafka transaction.

```text
begin Kafka transaction

publish:
  mutation A
  mutation B
  mutation C

publish:
  cursor = V123

commit Kafka transaction
```

Only after:

```text
commit_transaction() succeeds
```

should the bridge:

```text
acknowledge FDB cursor
```

This creates a durable Kafka-side record of what FDB progress has already been committed.

---

# 17. Important Crash Case

Consider:

```text
Kafka transaction COMMIT succeeds
        ↓
Bridge crashes
        ↓
FDB ACK never happens
```

FDB may replay the mutations after restart.

Because Kafka already contains a committed progress/cursor record, the bridge can determine what has already been committed and avoid making the replay visible as a duplicate.

This is why the cursor must be coordinated with the Kafka transaction.

---

# 18. `read_committed` Consumers

The downstream consumer contract must use:

```text
isolation.level=read_committed
```

Otherwise consumers may observe records from aborted transactions.

The EOS contract is therefore:

```text
transactional producer
        +
successful Kafka commit
        +
read_committed consumer
```

---

# 19. Ordering

Kafka preserves ordering within a partition.

If one FDB CDC stream requires a total order:

```text
one FDB stream
      ↓
one Kafka partition
```

is the simplest design.

Producer ordering also depends on retry behavior.

With idempotence enabled, Kafka supports:

```text
max.in.flight.requests.per.connection <= 5
```

while preserving the relevant producer ordering guarantees.

For the first implementation:

```text
max.in.flight.requests.per.connection=1
```

is easiest to reason about.

---

# 20. Batching

Kafka producers batch records for efficiency.

Instead of:

```text
one record
   ↓
one network request
```

the producer can send:

```text
A
B
C
D
 ↓
one batch
```

This reduces:

- network overhead,
- protocol overhead,
- system calls,
- and can improve compression.

Important producer settings:

```text
linger.ms
batch.size
```

---

# 21. `linger.ms`

`linger.ms` tells the producer how long it may wait for more records before sending a partially filled batch.

```text
Record A arrives
     ↓
wait up to linger.ms
     ↓
B, C, D arrive
     ↓
send [A B C D]
```

Trade-off:

```text
larger linger.ms
      ↓
better batching
      ↓
potentially more latency
```

A small value such as a few milliseconds is a reasonable starting point for benchmarking.

---

# 22. `batch.size`

`batch.size` limits the amount of data grouped into a producer batch for a partition.

It is a byte-oriented limit, not a mutation count.

Conceptually:

```text
Partition batch

[A]
[A B]
[A B C]
...
batch threshold
     ↓
send
```

Because `confluent-kafka` uses librdkafka, we should use librdkafka's actual configuration behavior and defaults rather than assuming the Java client has identical defaults.

---

# 23. Batching vs Transactions

These solve different problems.

```text
Batching
    ↓
performance optimization
```

```text
Transactions
    ↓
atomic visibility / EOS mechanism
```

One transaction may contain multiple producer batches.

---

# 24. Error Handling

Reliable transactional code should distinguish:

```text
Retriable errors
Abortable transaction errors
Fatal errors
```

## Retriable errors

Temporary failures such as transport problems may be retriable.

`confluent-kafka` exposes:

```python
error.retriable()
```

Many producer-level retries are already handled by the client.

The application should avoid blindly resending individual records in ways that defeat idempotence.

## Abortable transaction errors

Some errors mean the current transaction cannot safely commit.

`confluent-kafka` exposes:

```python
error.txn_requires_abort()
```

Correct pattern:

```text
transaction error
       ↓
abort transaction
       ↓
retry the logical transaction
```

## Fatal errors

A fatal producer error means the producer should not continue as though correctness is still guaranteed.

Conceptually:

```text
fatal producer state
      ↓
log + alert
      ↓
stop / recreate according to design
```

---

# 25. Delivery Reports

`confluent-kafka` is asynchronous.

Calling:

```python
producer.produce(...)
```

does not mean the record has already been durably committed.

Delivery callbacks can report record delivery outcomes.

However, in transactional mode, the final bridge correctness boundary is:

```text
commit_transaction() succeeds
```

not simply an individual record delivery callback.

Only after transaction commit should the bridge advance its FDB acknowledgement.

---

# 26. Protobuf Serialization

Kafka stores keys and values as bytes.

Our bridge can use Protobuf as:

```text
FDBMutationRecord
      ↓
SerializeToString()
      ↓
bytes
      ↓
Kafka value
```

Example:

```python
payload = mutation_record.SerializeToString()

producer.produce(
    topic=topic,
    value=payload,
)
```

Downstream consumers use the same `.proto` contract:

```text
Kafka bytes
    ↓
ParseFromString()
    ↓
FDBMutationRecord
```

Kafka itself does not need to understand the schema.

---

# 27. Kafka Record Key vs Value

The Protobuf event can be stored as the Kafka:

```text
value
```

The Kafka:

```text
key
```

is a separate partitioning decision.

Possible keys include:

- stream ID,
- directory ID,
- raw FDB key,
- or null.

Because the current architecture uses one partition per ordered stream, entity-level partitioning may not be necessary initially.

---

# 28. Python Kafka Client Evaluation

The main candidates researched were:

```text
confluent-kafka
kafka-python
```

## Important update

Older comparisons often say:

```text
kafka-python does not support transactions
```

That statement is outdated.

Current `kafka-python` versions support idempotent producers, transactional producers, and `read_committed` consumers.

Transaction support appeared in the 2.2.x line, and the project remains actively maintained with 3.x releases in 2026.

---

# 29. `confluent-kafka`

`confluent-kafka` is Confluent's Python client built on native:

```text
librdkafka
```

Relevant features:

- asynchronous producer,
- idempotent production,
- transactional producer API,
- `init_transactions()`,
- `begin_transaction()`,
- `commit_transaction()`,
- `abort_transaction()`,
- stable `transactional.id`,
- producer fencing,
- transaction-aware error classification,
- delivery reports,
- high-performance batching.

Its transactional API maps very directly to our bridge requirements.

---

# 30. `kafka-python`

`kafka-python` is primarily a Python implementation.

Current versions support:

- idempotent production,
- transactional production,
- read-committed consumers,
- transactional improvements,
- active maintenance.

It is therefore a legitimate option and should not be rejected using outdated information.

However, its transaction support is newer relative to the long-established librdkafka transaction path.

---

# 31. Client Comparison

| Area | `confluent-kafka` | `kafka-python` |
|---|---|---|
| Implementation | Python bindings over native `librdkafka` | Primarily Python |
| Idempotent producer | Yes | Yes |
| Transactional producer | Yes | Yes in current releases |
| `read_committed` consumer | Yes | Yes |
| Producer fencing | Yes | Supported |
| Transaction API maturity | Strong / long-established | Newer |
| Error classification | Explicit retriable / abortable / fatal model | Available, different ergonomics |
| Performance | Strong native implementation | Improved, but Python-heavy |
| Maintenance | Active | Active |
| Fit for this bridge | **Recommended** | Viable alternative |

---

# 32. Recommendation

Use:

```text
confluent-kafka
```

for the FDB → Kafka bridge.

## Why

1. Mature transactional API.
2. librdkafka-based performance.
3. Idempotence support.
4. Clear retriable/abortable/fatal error handling.
5. Stable `transactional.id` fencing.
6. Strong fit for high-throughput producer workloads.
7. It already matches the project's requirements document.

`kafka-python` is no longer disqualified by missing transactions, but `confluent-kafka` remains the stronger engineering fit.

---

# 33. Proposed Initial Producer Configuration

```python
producer_config = {
    "bootstrap.servers": "localhost:9092",

    "acks": "all",

    "enable.idempotence": True,

    "max.in.flight.requests.per.connection": 1,

    "transactional.id": "fdb-cdc-users",

    "linger.ms": 5,
}
```

Notes:

- `transactional.id` should be stable for the same logical FDB stream.
- Different independent streams should have different IDs.
- `transactional.id` implies idempotence in Kafka/librdkafka.
- Explicit settings are still useful for clarity.
- Batching values should be benchmarked rather than guessed.

---

# 34. Recommended Bridge Transaction Pattern

Conceptual pseudocode:

```python
producer.init_transactions()

while running:

    mutations, cursor = read_from_fdb()

    producer.begin_transaction()

    try:
        for mutation in mutations:
            payload = serialize_protobuf(mutation)

            producer.produce(
                topic=topic,
                value=payload,
            )

        producer.produce(
            topic=cursor_topic,
            key=stream_id,
            value=serialize_cursor(cursor),
        )

        producer.commit_transaction()

    except KafkaException as exc:

        error = exc.args[0]

        if error.txn_requires_abort():
            producer.abort_transaction()

        elif error.retriable():
            ...

        else:
            raise

    acknowledge_fdb(cursor)
```

This is conceptual rather than final production code.

The critical ordering is:

```text
Kafka commit
    BEFORE
FDB acknowledgement
```

---

# 35. Failure Scenarios

## Crash before Kafka commit

```text
FDB read
   ↓
begin transaction
   ↓
produce
   ↓
CRASH
```

Expected:

- transaction does not become committed output,
- FDB was not acknowledged,
- data can be replayed.

## Transaction abort

```text
produce
   ↓
transaction error
   ↓
abort
```

Expected:

- `read_committed` consumers do not see aborted data,
- FDB is not acknowledged,
- bridge retries the logical unit.

## Commit succeeds, crash before FDB ACK

```text
Kafka COMMIT
    ↓
CRASH
    ↓
FDB not ACKed
```

Expected:

- FDB may replay,
- Kafka's committed cursor/progress identifies already committed progress,
- bridge avoids a visible duplicate.

## Stale producer after failover

```text
Old producer: transactional.id=stream-users
New producer: transactional.id=stream-users
```

Kafka fences the stale producer.

---

# 36. What "Durable Receipt" Means

The phrase:

> only acknowledge back to FDB after Kafka confirms durable receipt

needs a precise interpretation.

With plain idempotent production, a successful delivery report under `acks=all` is strong evidence of durable broker acceptance.

With our transactional design, the stronger correctness boundary is:

```text
commit_transaction() succeeds
```

because individual records may already be present at brokers while still belonging to a transaction that later aborts.

Therefore:

```text
record delivery != final bridge commit
```

FDB should be acknowledged after the Kafka transaction commits.

---

# 37. Suggested Development Phases

## Phase 1 — Basic producer

```text
Python
  ↓
confluent-kafka
  ↓
local Kafka
```

## Phase 2 — Protobuf

```text
FDBMutationRecord
      ↓
SerializeToString()
      ↓
Kafka
```

## Phase 3 — Durable producer settings

```text
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=1
```

## Phase 4 — Transactions

```text
transactional.id
init_transactions
begin_transaction
commit_transaction
abort_transaction
```

## Phase 5 — Cursor transaction

Commit:

```text
mutation records
+
CDC cursor
```

atomically in Kafka.

## Phase 6 — Failure testing

Test:

```text
bridge kill before commit
bridge kill after commit
Kafka restart
network failure
producer fencing
```

Verify:

```text
zero loss
zero visible duplicates
correct ordering
correct restart cursor
```

---

# 38. Research Conclusions

1. `acks=all` provides the strongest producer acknowledgement durability but does not alone provide exactly-once semantics.

2. `enable.idempotence=true` prevents duplicates caused by Kafka producer retries and preserves ordering with compatible configuration.

3. `transactional.id` enables transactional publishing across producer sessions and supports stale-producer fencing.

4. Kafka EOS requires transactional publishing plus `read_committed` downstream consumption.

5. Because FDB and Kafka do not share one atomic transaction, our bridge should commit mutation records and CDC progress together in Kafka, then acknowledge FDB only after that transaction succeeds.

6. `confluent-kafka` is the recommended Python client because its mature librdkafka transactional API, performance, and explicit transaction error model align well with our requirements.

7. `kafka-python` now supports transactions and is actively maintained, so older claims that it has no transaction support are outdated.

8. `linger.ms` and `batch.size` improve throughput but must be balanced against latency.

9. Protobuf payloads can be serialized directly to bytes and placed in the Kafka record value.

10. For our bridge, the primary success boundary is successful Kafka transaction commit, not merely `produce()` returning.

---

# 39. Recommended Initial Configuration Summary

```text
Python client:
    confluent-kafka

Durability:
    acks=all

Idempotence:
    enable.idempotence=true

Ordering:
    max.in.flight.requests.per.connection=1 initially
    benchmark up to 5 later

Transactions:
    transactional.id=<stable ID per FDB stream>

Consumer contract:
    isolation.level=read_committed

Serialization:
    Protobuf SerializeToString() → Kafka value bytes

Batching:
    start with small linger.ms
    benchmark batch.size / linger.ms

FDB acknowledgement:
    only after commit_transaction() succeeds
```

---

# 40. Official Documentation and References

## Apache Kafka Producer Configuration

https://kafka.apache.org/40/configuration/producer-configs/

Useful for:

- `acks`
- `enable.idempotence`
- `transactional.id`
- `max.in.flight.requests.per.connection`
- `linger.ms`
- `batch.size`

## Apache Kafka Documentation

https://kafka.apache.org/documentation/

## Confluent Kafka Python API

https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

Useful for:

- Producer API
- transactional API
- `init_transactions()`
- `begin_transaction()`
- `commit_transaction()`
- `abort_transaction()`
- retriable errors
- abortable errors
- fatal errors

## librdkafka Configuration

https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html

Useful for:

- `enable.idempotence`
- `transactional.id`
- `linger.ms`
- `batch.size`
- retries
- delivery reports

## Kafka Delivery Semantics Overview

https://docs.confluent.io/kafka/design/delivery-semantics.html

## confluent-kafka-python Repository

https://github.com/confluentinc/confluent-kafka-python

## kafka-python Repository

https://github.com/dpkp/kafka-python

## kafka-python Releases

https://github.com/dpkp/kafka-python/releases

## kafka-python Changelog

https://github.com/dpkp/kafka-python/blob/master/docs/changelog.rst

The changelog is important because recent releases added and expanded idempotent and transactional producer support, making older "no transaction support" comparisons outdated.

---

# 41. Recommended Team Discussion Questions

1. What should one Kafka transaction contain?
   - one FDB version group,
   - one CDC consume reply,
   - or multiple version groups?

2. Where should the committed CDC cursor live?
   - same topic,
   - separate compacted metadata topic?

3. What is the exact `transactional.id` naming scheme?

4. How is the same stable `transactional.id` preserved during failover?

5. Should the first implementation use:

```text
max.in.flight.requests.per.connection=1
```

and later benchmark 5?

6. What starting values should we benchmark for:

```text
linger.ms
batch.size
```

7. Which Kafka errors should cause:
   - retry,
   - transaction abort,
   - producer shutdown,
   - operator alert?

8. What is the maximum amount of FDB data we allow in one Kafka transaction?

9. How will integration tests prove:
   - zero lost records,
   - zero visible duplicates,
   - ascending FDB version order,
   - correct crash recovery?

---

# 42. One-Sentence Summary

> **Use a librdkafka-backed transactional `confluent-kafka` producer with `acks=all`, idempotence, a stable `transactional.id`, and `read_committed` consumers; commit mutations and CDC progress atomically in Kafka, and acknowledge FoundationDB only after the Kafka transaction successfully commits.**
