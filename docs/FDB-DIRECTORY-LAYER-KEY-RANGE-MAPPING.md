# FoundationDB Directory Layer & Key Range Mapping
## A Practical Guide for the FoundationDB → Kafka CDC Bridge

This document explains the FoundationDB (FDB) **Directory Layer**, **subspaces**, **key ranges**, and **key range mapping** in simple terms, with a focus on how these concepts affect our **FoundationDB CDC → Kafka bridge**.

---

# 1. Why This Matters for Our Project

Our project is building a bridge that:

```text
FoundationDB
    ↓
CDC mutation stream
    ↓
Bridge
    ↓
Serialization
    ↓
Kafka
    ↓
Consumers / downstream applications
```

FoundationDB CDC gives us changes to raw FDB keys.

Kafka, however, needs us to decide how those changes should be:

- represented,
- routed,
- partitioned,
- grouped,
- and understood by downstream consumers.

To make good decisions about that, we need to understand how FoundationDB organizes data internally.

The core chain to remember is:

```text
Logical Directory
      ↓
Directory Layer
      ↓
Subspace Prefix
      ↓
Physical FDB Key Range
      ↓
CDC Watches That Range
      ↓
Bridge Receives Mutations
      ↓
Kafka
```

That is the key relationship between the Directory Layer and our Kafka work.

---

# 2. FoundationDB Has One Giant Ordered Keyspace

FoundationDB is fundamentally a **distributed ordered key-value database**.

Instead of thinking in SQL tables such as:

```sql
SELECT * FROM users;
```

think of FoundationDB as one huge ordered collection of keys:

```text
Key                         Value
------------------------------------------------
users/1/name                Alice
users/1/email               alice@email.com
users/2/name                Bob
orders/100/status           shipped
orders/101/status           pending
products/55/price           24.99
```

At the lowest level, FDB keys are just **byte strings**.

FoundationDB stores them in **lexicographic order**.

Conceptually:

```text
users/1/...
users/2/...
users/3/...
-------------------
orders/100/...
orders/101/...
-------------------
products/...
```

Because keys are ordered, FoundationDB can efficiently operate on contiguous sections of the keyspace.

These sections are called **key ranges**.

---

# 3. What Is a Key Range?

A key range is a continuous interval of keys:

```text
[start_key, end_key)
```

The start key is included.

The end key is excluded.

For example:

```text
["users/", "users0")
```

could represent a raw prefix-style range containing keys beginning with `users/`.

Conceptually:

```text
users/1/name
users/1/email
users/2/name
users/2/email
```

all belong to the same logical range.

Key ranges are extremely important in FoundationDB because many operations work over ranges rather than individual keys.

For our project, CDC also cares about ranges because a CDC stream watches mutations affecting one or more key ranges.

---

# 4. The Problem with Managing Raw Prefixes Manually

Imagine several applications share the same FoundationDB database:

```text
User Service
Order Service
Inventory Service
```

We could manually assign byte prefixes:

```text
\x01 → users
\x02 → orders
\x03 → inventory
```

Then real keys might look like:

```text
\x01 + encoded user data
\x02 + encoded order data
\x03 + encoded inventory data
```

This works, but now somebody has to manage those prefixes.

We would have to make sure:

- applications do not accidentally choose the same prefix,
- prefixes do not overlap,
- we know what every prefix means,
- directories can be reorganized safely,
- multiple teams can share the database without collisions.

FoundationDB's **Directory Layer** exists to solve that problem.

---

# 5. What Is the Directory Layer?

The Directory Layer gives human-readable names to regions of the FoundationDB keyspace.

Think of it like a filesystem-style namespace:

```text
/users
/orders
/products
```

An application may request a directory such as:

```python
users = fdb.directory.create_or_open(db, ("users",))
```

Conceptually, we asked FoundationDB for:

```text
/users
```

The Directory Layer then assigns that logical directory a short binary prefix.

For example:

```text
/users
    ↓
0x1537
```

The exact prefix is managed automatically.

The important idea is:

```text
Human-readable directory path
          ↓
Directory Layer lookup
          ↓
Binary prefix
```

The Directory Layer is therefore a **level of indirection**.

The directory name is logical.

The prefix is physical.

---

# 6. A Directory Name Is Not the Literal Stored Prefix

This is a very important distinction.

If we create:

```python
users = fdb.directory.create_or_open(db, ("users",))
```

FoundationDB does not necessarily store keys as:

```text
users/Alice
users/Bob
users/Charlie
```

Instead, it may allocate a binary prefix:

```text
/users
   ↓
0x1537
```

Actual stored keys might then start with:

```text
0x1537...
```

So:

```text
Logical view:

/users

        ↓

Directory Layer

        ↓

Physical view:

0x1537...
```

For our CDC bridge, this matters because CDC mutations may expose raw physical FDB keys, not friendly directory paths.

---

# 7. What Is a Subspace?

A **subspace** is a prefix that represents part of the FoundationDB keyspace.

If:

```text
users prefix = 0x1537
```

then all keys beginning with that prefix belong to the users subspace.

For example:

```text
0x1537 + Alice
0x1537 + Bob
0x1537 + Charlie
```

Visually:

```text
FDB KEYSPACE

00...
01...
...
15 36...
==========================
15 37 Alice
15 37 Bob
15 37 Charlie    ← users subspace
==========================
15 38...
...
FF...
```

A directory typically gives us a `DirectorySubspace`.

That object behaves both like:

- a directory in the hierarchy,
- and a subspace representing part of the actual keyspace.

A good mental model is:

```text
Directory = logical name

Subspace = physical prefix
```

---

# 8. Directory Hierarchies

Directories can be nested.

For example:

```text
/store
/store/users
/store/orders
/store/products
```

Conceptually:

```python
store = fdb.directory.create_or_open(db, ("store",))

users = store.create_or_open(db, ("users",))
orders = store.create_or_open(db, ("orders",))
```

This gives applications a familiar hierarchy.

However, do not assume that the physical bytes must literally be:

```text
store_prefix + users_prefix
```

The Directory Layer manages that mapping internally.

What matters to applications is:

```text
directory path
      ↓
subspace prefix
```

---

# 9. Directory → Key Range Mapping

This is the part most relevant to our CDC project.

Suppose we have:

```text
/users
/orders
/products
```

and FoundationDB maps them to:

```text
/users      → prefix A
/orders     → prefix B
/products   → prefix C
```

Each prefix corresponds to a region of the keyspace.

Conceptually:

```text
Users:
[prefix_A, end_of_prefix_A)

Orders:
[prefix_B, end_of_prefix_B)

Products:
[prefix_C, end_of_prefix_C)
```

Visually:

```text
FDB KEYSPACE

+----------------------------------------+
| USERS RANGE                            |
|                                        |
| user 1                                 |
| user 2                                 |
| user 3                                 |
+----------------------------------------+

+----------------------------------------+
| ORDERS RANGE                           |
|                                        |
| order 100                              |
| order 101                              |
+----------------------------------------+

+----------------------------------------+
| PRODUCTS RANGE                         |
+----------------------------------------+
```

The prefix identifies the beginning of the region.

A corresponding prefix-end value identifies where that region stops.

So the mapping chain is:

```text
/users
   ↓
Directory Layer
   ↓
prefix A
   ↓
[prefix A, prefix_end(prefix A))
```

That final result is a physical FDB key range.

---

# 10. Why CDC Cares About Key Ranges

CDC means **Change Data Capture**.

A CDC stream watches database mutations that occur within one or more FDB key ranges.

Suppose the users directory maps to:

```text
prefix = 0x1537
```

Then the CDC stream might conceptually watch:

```text
[0x1537, prefix_end(0x1537))
```

That means:

```text
"Tell me whenever something changes
inside the users subspace."
```

So:

```text
/users
   ↓
Directory Layer
   ↓
0x1537
   ↓
key range
   ↓
CDC watches that range
```

This is the connection we care about.

---

# 11. Concrete Example

Suppose:

```text
Directory:
/users

Physical prefix:
0x1537
```

Our application stores:

```text
user Alice
user Bob
user Charlie
```

The physical keys may look conceptually like:

```text
0x1537 + ("alice", "email")
0x1537 + ("bob", "email")
0x1537 + ("charlie", "email")
```

All of them begin with:

```text
0x1537
```

Therefore the full users range is approximately:

```text
[0x1537, prefix_end(0x1537))
```

Now imagine Alice's email changes.

CDC sees a mutation affecting a raw key such as:

```text
0x1537...
```

Our bridge receives that mutation.

The raw key itself may not say:

```text
/users
```

Instead, we may only see:

```text
0x1537...
```

That creates an important design question:

> Do we care about converting that physical key/range back into a logical directory before sending the event to Kafka?

---

# 12. The Tuple Layer

The Tuple Layer is another FoundationDB concept that helps explain how actual keys are built.

FDB keys are bytes.

Applications usually want structured keys such as:

```text
("users", 123, "email")
```

The Tuple Layer converts structured values into binary representations that preserve useful ordering properties.

Conceptually:

```text
("users", 123, "email")
        ↓
Tuple Layer
        ↓
encoded bytes
```

A subspace can then prepend its prefix.

So a complete key may conceptually be:

```text
directory prefix
       +
tuple-encoded application key
       ↓
complete FDB key
```

For example:

```text
Directory prefix:
P

Tuple:
(123, "email")

Final key:
P + encoded(123, "email")
```

The full hierarchy is:

```text
Directory Layer
      ↓
DirectorySubspace
      ↓
Subspace prefix
      ↓
Tuple Layer
      ↓
Structured application key
      ↓
FINAL FDB KEY
```

---

# 13. Example with an Application Directory

Suppose we create:

```python
app = fdb.directory.create_or_open(
    db,
    ("my_app",)
)
```

FDB assigns:

```text
prefix = P
```

Inside that application we create a logical users area.

Conceptually:

```text
/my_app/users
```

Then we build a key:

```python
key = users.pack((123, "email"))
```

Conceptually:

```text
P
+
encoded("users")
+
encoded(123)
+
encoded("email")
```

The final physical FDB key is just bytes.

But to the application, it represents:

```text
/my_app/users/123/email
```

This separation is useful because applications can work with meaningful logical structures while FoundationDB manages the actual key layout.

---

# 14. Directory Moves and Renames

One useful property of the Directory Layer is that a directory path can move without requiring the underlying subspace prefix to change.

Imagine:

```text
BEFORE

/users
   ↓
0x1537
```

Then the directory is moved to:

```text
AFTER

/store/users
      ↓
0x1537
```

The logical name changed.

The physical prefix stayed the same.

This is possible because the Directory Layer adds indirection:

```text
directory path
      ↓
metadata lookup
      ↓
physical prefix
```

This has an important CDC implication.

If CDC is watching:

```text
[0x1537, prefix_end(0x1537))
```

and the directory is renamed, CDC may still be watching the exact same underlying data.

Therefore our Kafka bridge should not assume:

```text
physical prefix = directory name
```

They are separate concepts.

---

# 15. Why This Matters to the Kafka Layer

Our bridge eventually receives CDC mutations.

Suppose we receive:

```text
FDB key:
0x1537...
```

If the Directory Layer mapping tells us:

```text
0x1537
   ↓
/users
```

we now have a choice about how much information to expose to Kafka.

This may affect:

- Kafka topics,
- Kafka message keys,
- Kafka partitions,
- Protobuf fields,
- downstream consumer usability.

---

# 16. Possible Architecture: Raw FDB Events

The simplest design is:

```text
CDC
 ↓
raw FDB mutation
 ↓
Kafka
```

Kafka event:

```text
version: 938429
mutation_type: SET
key_bytes: 0x1537...
value_bytes: ...
```

Advantages:

- simple,
- generic,
- closest representation to FoundationDB,
- minimal metadata lookup.

Disadvantages:

- downstream consumers must understand FDB key encoding,
- raw prefixes are not very human-readable,
- consumers may need extra knowledge about Directory Layer mappings.

---

# 17. Possible Architecture: Directory-Enriched Events

Another design is:

```text
CDC
 ↓
raw FDB mutation
 ↓
directory / range lookup
 ↓
enriched event
 ↓
Kafka
```

Kafka event could contain:

```text
directory: "/users"
version: 938429
mutation_type: SET
fdb_key: ...
value: ...
```

Advantages:

- easier for downstream consumers,
- useful debugging information,
- event semantics are more understandable.

Disadvantages:

- bridge must maintain or query directory metadata,
- directory moves/renames introduce semantic questions,
- more complexity in the producer pipeline.

---

# 18. Possible Architecture: Directory Controls Kafka Topics

We could map logical FDB areas into Kafka topics.

For example:

```text
/users
     ↓
fdb.users

/orders
     ↓
fdb.orders

/products
     ↓
fdb.products
```

Then:

```text
FDB CDC
   ↓
directory/range mapping
   ↓
topic selection
   ↓
Kafka
```

Advantages:

- consumers can subscribe only to data they care about,
- topics become semantically meaningful.

Disadvantages:

- more Kafka topics,
- directory changes may affect routing behavior,
- configuration becomes more complicated,
- topic naming becomes coupled to application structure.

---

# 19. Possible Architecture: Directory Influences Kafka Partitioning

Another option is to keep one Kafka topic but use FDB structure to determine Kafka partitioning.

Example:

```text
Kafka Topic:
foundationdb-cdc
```

Then:

```text
/users/...    → partition based on users key
/orders/...   → partition based on orders key
/products/... → partition based on products key
```

This could potentially help preserve useful ordering relationships.

However, Kafka only guarantees ordering **within a partition**.

Therefore our partitioning strategy must match whatever ordering guarantees the bridge promises.

This connects Directory Layer research directly to Kafka design.

---

# 20. Important Distinction: Two Meanings of "Key Range Mapping"

The phrase **key range mapping** can refer to two different things in FoundationDB.

## Meaning 1: Application-Level Directory → Key Range

This is what we have mainly discussed:

```text
/users
   ↓
Directory Layer
   ↓
subspace prefix
   ↓
FDB key range
```

This is highly relevant to our CDC bridge.

---

## Meaning 2: Physical FDB Key Range → Storage Servers

FoundationDB is distributed.

Internally, different physical parts of the global keyspace may live on different storage servers.

Conceptually:

```text
[a, f) → Storage Server 1
[f, m) → Storage Server 2
[m, z) → Storage Server 3
```

FDB tracks this mapping internally.

Clients use key range location information to know where requests should go.

This is a lower-level distributed-systems concept.

For our current Kafka/CDC work, the first meaning is likely more directly relevant:

```text
logical directory
      ↓
physical CDC range
```

But we should be aware that both uses of the phrase exist.

---

# 21. The Most Important Architecture Question for Our Team

The main question is:

> When a CDC mutation gives us a raw FoundationDB key, do we need to resolve that key back to a Directory Layer path before publishing it to Kafka?

There is no automatic single answer.

It depends on what guarantees and usability we want.

Possible strategies:

```text
Option A:
raw FDB key → Kafka

Option B:
raw FDB key
   ↓
resolve directory
   ↓
directory-enriched Kafka event

Option C:
directory / key range
   ↓
select Kafka topic

Option D:
directory / key range
   ↓
select Kafka partition key
```

This should probably be an explicit architecture decision.

---

# 22. Questions We Should Bring to the Team

## CDC registration

1. Are CDC streams registered using:
   - raw key ranges,
   - Directory Layer subspaces,
   - or both?

2. Who is responsible for translating a logical directory into a CDC range?

---

## Event representation

3. Will Kafka events contain:
   - raw FoundationDB keys only,
   - decoded tuple data,
   - logical directory paths,
   - or some combination?

4. Should the Protobuf schema contain directory metadata?

For example:

```protobuf
message CdcEvent {
    string directory_path = 1;
    bytes raw_key = 2;
    uint64 version = 3;
    MutationType mutation_type = 4;
}
```

---

## Kafka topics

5. Will we use:

```text
one global topic
```

such as:

```text
foundationdb-cdc
```

or:

```text
one topic per CDC stream
```

or:

```text
one topic per logical directory
```

such as:

```text
fdb.users
fdb.orders
fdb.products
```

---

## Kafka partitioning

6. Does the FDB directory or key range influence the Kafka message key?

7. What ordering guarantee do we need?

For example:

```text
all mutations globally ordered
```

versus:

```text
mutations ordered per directory
```

versus:

```text
mutations ordered per logical entity/key
```

That decision strongly affects Kafka partitioning.

---

## Directory changes

8. What should happen if:

```text
/users
```

becomes:

```text
/store/users
```

but its physical prefix stays the same?

Should Kafka consumers see:

```text
/users
```

for old messages and:

```text
/store/users
```

for new ones?

Or should the bridge treat the physical stream identity as stable?

This should be defined.

---

## Mapping configuration

9. Do we need explicit configuration like:

```text
Directory: /users
FDB range: [...]
Kafka topic: users-cdc
Kafka partition strategy: by user ID
```

and:

```text
Directory: /orders
FDB range: [...]
Kafka topic: orders-cdc
Kafka partition strategy: by order ID
```

If so, this mapping may become a real component of the bridge.

---

# 23. Example End-to-End Flow

Suppose we have:

```text
Directory:
/users
```

Directory Layer resolves it to:

```text
Prefix:
0x1537
```

The corresponding FDB key range is:

```text
[0x1537, prefix_end(0x1537))
```

CDC watches that range.

A user email changes.

FoundationDB produces a mutation:

```text
version: 8349238
type: SET_VALUE
key: 0x1537...
value: new@email.com
```

Our bridge receives it.

We may resolve:

```text
0x1537
   ↓
/users
```

Then serialize:

```text
CdcEvent {
    directory: "/users"
    version: 8349238
    mutation_type: SET_VALUE
    raw_key: ...
    value: ...
}
```

Then route to:

```text
Kafka topic:
foundationdb-cdc
```

or perhaps:

```text
Kafka topic:
fdb.users
```

The Kafka producer sends the event.

A downstream consumer reads it.

So the full pipeline becomes:

```text
/users
   ↓
Directory Layer
   ↓
0x1537
   ↓
FDB Key Range
   ↓
CDC
   ↓
Mutation
   ↓
Directory / Range Resolution
   ↓
Protobuf
   ↓
Kafka Producer
   ↓
Kafka Topic / Partition
   ↓
Consumer
```

---

# 24. Simple Definitions

## Keyspace

FoundationDB's globally ordered collection of binary keys.

```text
key1
key2
key3
...
```

---

## Key Range

A continuous section of the ordered keyspace.

```text
[begin, end)
```

---

## Prefix

A byte sequence shared by a group of related keys.

```text
prefix + key1
prefix + key2
prefix + key3
```

---

## Subspace

A logical region of FoundationDB's keyspace identified by a prefix.

```text
prefix
  ↓
all keys starting with that prefix
```

---

## Directory Layer

A mapping system that lets applications use human-readable hierarchical names instead of manually managing raw byte prefixes.

```text
/users
   ↓
Directory Layer
   ↓
binary subspace prefix
```

---

## Tuple Layer

A structured encoding mechanism that converts values such as:

```text
(123, "email")
```

into correctly ordered FDB key bytes.

---

## Directory → Key Range Mapping

The process:

```text
logical path
     ↓
Directory Layer
     ↓
subspace prefix
     ↓
physical FDB key range
```

---

## CDC Range

The physical FDB key range whose mutations a CDC stream monitors.

---

# 25. The Mental Model to Remember

If we remember only one diagram, it should be this:

```text
                LOGICAL WORLD

                    /users
                       |
                       |
                       v

                DIRECTORY LAYER

                       |
                       |
                       v

                SUBSPACE PREFIX

                    0x1537
                       |
                       |
                       v

                  KEY RANGE

        [0x1537, prefix_end(0x1537))
                       |
                       |
                       v

                     CDC

                  mutation
                       |
                       |
                       v

                    BRIDGE

                serialize / map
                       |
                       |
                       v

                    KAFKA

              topic + partition
                       |
                       |
                       v

                   CONSUMER
```

For our project, this relationship is the important part:

> The Directory Layer gives logical meaning to portions of the FoundationDB keyspace, while CDC operates on the physical key ranges underneath those directories. Our bridge must decide how much of that logical meaning should be preserved, enriched, or used for routing when publishing CDC events to Kafka.

---

# 26. How the Directory Layer Allocates Prefixes: The High Contention Allocator

The document above states that the Directory Layer assigns short binary prefixes to directories.

But how does it actually do that?

The answer is the **High Contention Allocator (HCA)**.

## The Problem

A naive approach would be:

```text
global counter = 0
new directory → counter++
prefix = encode(counter)
```

But this creates a severe bottleneck because every directory creation transaction would read and increment the exact same key. Under concurrent clients, this causes constant transaction conflicts.

## The Solution: Sliding Window Allocation

The HCA avoids single-point-of-contention by using a **sliding window** algorithm:

```text
1. Maintain a "window" of candidate prefix IDs.
2. A client needing a new prefix randomly selects
   a candidate from the current window.
3. It verifies (transactionally) that the candidate
   is not already in use.
4. If a collision occurs, the transaction retries
   with a new candidate.
5. As contention increases, the window size grows
   dynamically to allow more concurrent, non-conflicting
   allocations.
```

Conceptually:

```text
Window: [100, 200)

Client A picks: 137  ← claims it
Client B picks: 162  ← claims it (no conflict with A)
Client C picks: 137  ← conflict → retry → picks 189
```

## Why This Matters for Our CDC Bridge

The HCA means:

- Directory prefixes are **short** (usually a few bytes).
- Prefixes are **not** sequential or predictable.
- Two directories created at the same time may get **unrelated** prefixes.
- The prefix tells you nothing about the directory's logical position in the hierarchy.

For our bridge, this reinforces the point that **raw CDC keys cannot be intuitively mapped back to directory paths** without an explicit lookup.

---

# 27. Tuple Layer Encoding Specification

Section 12 described the Tuple Layer conceptually.

Here are the concrete encoding details from the official specification.

## Type Codes

The Tuple Layer uses a system of **type code** bytes to identify what kind of value follows:

```text
Type Code    Type               Encoding
─────────    ────               ────────
0x00         Null               Zero-length. Represents nil/null.
0x01         Byte String        0x01 + value (0x00 escaped as 0x00 0xFF) + 0x00
0x02         Unicode String     0x02 + UTF-8 value (0x00 escaped as 0x00 0xFF) + 0x00
0x05         Nested Tuple       0x05 + encoded elements (nulls become 0x00 0xFF) + 0x00
0x0C–0x1C    Integers           19 type codes covering negative and positive integers
0x14         Integer Zero       Represents the integer 0
0x20         Float (32-bit)     IEEE big-endian with sign bit adjustments
0x21         Double (64-bit)    IEEE big-endian with sign bit adjustments
0x26         Boolean False      0x26
0x27         Boolean True       0x27
0x30         UUID               0x30 + 16 bytes (big-endian)
0x33         Versionstamp       0x33 + 12 bytes (10 byte version + 2 byte user)
```

## Null-Byte Escaping

The `0x00` byte is special because it acts as the **terminator** for variable-length values.

Any literal `0x00` inside a byte string or unicode string is escaped as `0x00 0xFF`:

```text
Original:   hello\x00world
Encoded:    0x01 hello 0x00 0xFF world 0x00
                  ↑                    ↑
            escaped null          terminator
```

## Order Preservation

The encoding is designed so that **lexicographic comparison of the encoded bytes** matches the **semantic comparison of the original values**:

```text
pack(("alice",)) < pack(("bob",))
pack((1,))       < pack((2,))
pack((1, "a"))   < pack((1, "b"))
pack((1, "a"))   < pack((2, "a"))
```

This is critical because FoundationDB stores keys in lexicographic byte order.

## Integer Encoding Detail

Integers use a **variable-length** encoding with different type codes for different byte widths:

```text
Type Code Range    Meaning
0x0C               Negative integer, 8 bytes
0x0D               Negative integer, 7 bytes
...
0x13               Negative integer, 1 byte
0x14               Zero
0x15               Positive integer, 1 byte
0x16               Positive integer, 2 bytes
...
0x1C               Positive integer, 8 bytes
```

Positive integers use big-endian encoding.

Signed integers use two's-complement with the sign bit inverted.

## CDC Implication

When our bridge receives a raw CDC key, the bytes after the directory prefix are **tuple-encoded**.

To extract meaningful information (user IDs, record types, etc.), the bridge would need to:

```text
raw key bytes
    ↓
strip directory prefix
    ↓
tuple-layer unpack
    ↓
structured values: ("users", 123, "email")
```

This decoding step is optional but adds significant usability to Kafka events.

---

# 28. Directory Partitions

The existing document did not mention **directory partitions**, which are an important Directory Layer feature.

## The Problem with Standard Directories

Under normal operation, a directory does **not** share a common key prefix with its subdirectories.

Sibling directories receive **independently allocated** prefixes:

```text
/store             → prefix 0x15 0x37
/store/users       → prefix 0x15 0x26    ← NOT a child of 0x15 0x37
/store/orders      → prefix 0x15 0x42    ← NOT a child of 0x15 0x37
```

This means you **cannot** use a single range read to scan all data under `/store` and its children.

## Directory Partitions Solve This

A **partition** is a special directory whose prefix is **prepended** to all its descendants' prefixes:

```python
partition = fdb.directory.create(db, ('p1',), layer=b'partition')
users = partition.create_or_open(db, ('users',))
```

Now:

```text
partition /p1      → prefix P
/p1/users          → prefix P + allocated_suffix
/p1/orders         → prefix P + allocated_suffix
```

All data under the partition shares the prefix `P`, enabling:

```text
range_read(P, prefix_end(P))
    → returns ALL data across ALL subdirectories of /p1
```

## Partition Drawbacks

The official docs explicitly warn about these:

1. **Directories cannot be moved between different partitions.**
2. **Longer prefixes** — partition directories have longer keys than non-partition counterparts, reducing performance.
3. **Nesting partitions** compounds the prefix length problem.
4. **The root directory of a partition cannot pack/unpack keys** — you must create at least one subdirectory to store content.

## CDC Implication

Directory partitions create an interesting opportunity for our bridge:

```text
If CDC watches the partition prefix range:

[P, prefix_end(P))

it captures mutations across ALL subdirectories
of that partition in a single CDC stream.
```

This could simplify CDC registration for hierarchical data:

```text
Without partitions:
  watch /store/users   → range A
  watch /store/orders  → range B
  watch /store/products → range C

With a partition at /store:
  watch /store         → range P (covers everything)
```

The tradeoff is that the bridge must then **demultiplex** mutations from the single stream into logical categories.

---

# 29. Tenants

**Tenants** are a FoundationDB feature (available since FDB 7.1) that provides a formal mechanism for **key-space isolation**.

## What Tenants Are

A tenant is a named **transaction domain** within the FDB cluster:

```text
Tenant: "tenant_A"
    → assigned prefix: 0xAA...
    → all transactions scoped to keys under 0xAA...

Tenant: "tenant_B"
    → assigned prefix: 0xBB...
    → all transactions scoped to keys under 0xBB...
```

Key properties:

- Each tenant has a unique, non-overlapping prefix assigned by FDB.
- **Tenant transactions cannot read or write outside their tenant boundaries.**
- This is enforced at the FDB client level.

## Tenants vs Directories

```text
Directories:
  logical namespace → short prefix
  NO enforcement of isolation
  applications can cross directory boundaries freely

Tenants:
  named transaction domain → enforced prefix
  STRICT isolation between tenants
  transactions CANNOT cross tenant boundaries
```

Directories organize data.

Tenants **isolate** data.

## Tenants and the Directory Layer

The official docs recommend:

> It is **not** recommended to use a global directory layer shared between tenants.

Instead, each tenant should have its **own** directory layer:

```text
Tenant A:
  directory layer A
    /users
    /orders

Tenant B:
  directory layer B
    /users    ← completely independent from Tenant A's /users
    /orders
```

Using the directory layer within a tenant transaction works normally — the paths and prefixes are scoped to the tenant's keyspace.

## CDC Implication

For a multi-tenant FDB deployment:

```text
Question:
  Does our CDC bridge need to be tenant-aware?

If CDC watches a global key range:
  → mutations from ALL tenants appear in the same stream
  → bridge must filter/route by tenant

If CDC watches per-tenant ranges:
  → each tenant's mutations are isolated
  → natural mapping to per-tenant Kafka topics
```

This is an important architecture question if the target FDB deployment uses tenants.

---

# 30. Directory Layer Internal Metadata Storage

The Directory Layer stores its own mapping data **inside FoundationDB itself**.

## The `\xFE` Prefix

By default, directory metadata lives under the byte prefix `\xFE` (254):

```text
FDB KEYSPACE

0x00...                ← user data starts here
...
0xFE...                ← DIRECTORY LAYER METADATA
  0xFE + path nodes    ← maps directory paths to prefixes
  0xFE + HCA state     ← high contention allocator state
  0xFE + version info  ← directory layer version
...
0xFF...                ← SYSTEM KEYS (reserved by FDB)
```

## Two Internal Subspaces

The `DirectoryLayer` class uses two subspaces internally:

```text
node_subspace (default: \xFE)
  Stores:
  - directory path → prefix mappings
  - HCA state
  - version metadata

content_subspace (default: empty prefix)
  Where actual application data is stored.
  Can use the entire keyspace except
  the node_subspace range.
```

You can customize these when creating your own `DirectoryLayer`:

```python
dir_layer = DirectoryLayer(
    node_subspace=Subspace(rawPrefix=b'\xFE'),
    content_subspace=Subspace(rawPrefix=b'\x01')
)
```

## CDC Implication

This has a critical implication:

```text
If CDC is configured to watch a broad key range
(e.g., the entire keyspace), it will also capture
mutations to the Directory Layer's own metadata.
```

Our bridge should be aware that:

- Keys under `\xFE` are **directory metadata**, not application data.
- Changes to directory metadata (creating/moving/deleting directories) will appear as CDC mutations.
- The bridge should probably **filter out** `\xFE`-prefixed mutations unless we specifically want to track directory structure changes.

Similarly:

- Keys under `\xFF` are **system keys** (reserved by FDB internally).
- These should also be filtered.

---

# 31. DirectorySubspace: The Dual Interface

When you create or open a directory, the returned object is a `DirectorySubspace`.

This object implements **two interfaces simultaneously**:

```text
DirectorySubspace
    ├── DirectoryLayer interface
    │     create()
    │     open()
    │     create_or_open()
    │     move()
    │     move_to()
    │     list()
    │     remove()
    │     exists()
    │
    └── Subspace interface
          pack(tuple)        → full FDB key
          unpack(key)        → tuple
          range()            → (begin_key, end_key)
          key()              → raw prefix bytes
          contains(key)      → bool
```

This means the same object can be used to:

1. **Manage subdirectories** (as a directory):

```python
users = fdb.directory.create_or_open(db, ('users',))
inactive = users.create(db, ('inactive',))       # subdirectory
```

2. **Store data** (as a subspace):

```python
db[users.pack((123, 'email'))] = b'alice@email.com'
db[users['Smith']] = b''                          # index notation
```

## CDC Implication

When our bridge resolves a raw key to a directory, the `DirectorySubspace.unpack()` method can decode the **application-level tuple** from the key:

```python
raw_key = b'\x15\x37...'     # from CDC mutation
stripped = users.unpack(raw_key)
# → (123, 'email')
```

This gives us structured data to include in Kafka events.

---

# 32. Full Directory Layer API Surface

The existing document mentioned `create_or_open` and `move` but did not document the full API.

Here is the complete set of operations:

## Creating and Opening

```python
# Create a new directory (error if exists)
users = fdb.directory.create(db, ('users',))

# Open an existing directory (error if not exists)
users = fdb.directory.open(db, ('users',))

# Create or open (idempotent — most common)
users = fdb.directory.create_or_open(db, ('users',))
```

## Moving and Renaming

```python
# Move via the directory layer (old_path → new_path)
users = fdb.directory.move(db, ('users',), ('store', 'users'))

# Move via the directory subspace itself
orders = orders.move_to(db, ('store', 'orders'))
```

Key property: **moving changes the logical path but NOT the physical prefix**.

## Listing

```python
# List top-level directory names
fdb.directory.list(db)
# → ['store']

# List subdirectories of a directory
store.list(db)
# → ['orders', 'products', 'users']

# List with a sub-path
store.list(db, ('orders',))
# → ['cancelled']
```

Note: `list` returns **directory names** (strings), not subspaces or their contents.

## Removing

```python
# Remove a directory (error if not exists)
users.remove(db)

# Remove if exists (no error)
fdb.directory.remove_if_exists(db, ('store', 'temp'))
```

**Removing a directory deletes all data in its subspace** plus all its subdirectories and their data.

## Existence Check

```python
users.exists(db)
# → True / False

store.exists(db, ('products',))
# → True / False
```

## The `layer` Parameter

Directories support an optional `layer` parameter — a byte string tag:

```python
users = fdb.directory.create(db, ('users',), layer=b'my_app_v2')
```

This can be used to:

- Tag directories with application-specific metadata.
- Enforce that a directory is opened with the correct layer string.
- Create **partitions** (by using `layer=b'partition'`).

The layer value is stored in directory metadata and can be retrieved via:

```python
users.layer
# → b'my_app_v2'
```

## CDC Implication

The `list()` method is particularly relevant for our bridge because it allows us to:

```text
1. Enumerate all directories in the FDB database.
2. Resolve each directory to its subspace prefix.
3. Automatically register CDC watches for each.
```

This could enable **automatic discovery** of CDC targets:

```text
for dir_name in fdb.directory.list(db):
    dir = fdb.directory.open(db, (dir_name,))
    prefix = dir.key()
    register_cdc_watch(prefix, prefix_end(prefix))
    map_to_kafka_topic(dir_name)
```

---

# 33. Subdirectories vs Nested Subspaces

The official docs make an important distinction between two ways to organize data under a directory.

## Subdirectories

```python
inactive = users.create(db, ('inactive',))
```

Properties:

- Gets its own **independently allocated** prefix.
- Prefix is **unrelated** to the parent's prefix.
- Can be **moved** or **renamed** cheaply.
- **Cannot** range-read across parent and subdirectory in one operation.

```text
/users          → prefix 0x15 0x37
/users/inactive → prefix 0x15 0x26   ← different prefix!

range_read(0x15 0x37, prefix_end(0x15 0x37))
    → returns ONLY /users data
    → does NOT include /users/inactive data
```

## Nested Subspaces

```python
# Use the directory as a subspace to store structured data
db[users.pack(('ID', user_id, 'lastname', 'Smith'))] = b''
db[users.pack(('ID', user_id, 'email', 'alice@test.com'))] = b''
```

Properties:

- All data shares the **same prefix** as the parent directory.
- Data is **physically contiguous** in the keyspace.
- **Can** range-read all data under the directory at once.
- **Cannot** be moved independently of the parent.

```text
/users prefix: 0x15 0x37

All keys:
  0x15 0x37 + pack(('ID', 1, 'lastname', ...))
  0x15 0x37 + pack(('ID', 1, 'email', ...))
  0x15 0x37 + pack(('ID', 2, 'lastname', ...))

range_read(0x15 0x37, prefix_end(0x15 0x37))
    → returns ALL user data in one scan
```

## When to Use Which

```text
Use subdirectories when:
  - Data sets are logically separate.
  - You need to move/rename sections independently.
  - You want independent CDC streams per section.

Use nested subspaces when:
  - Data is logically part of one collection.
  - You want to range-read the entire collection.
  - You want a single CDC stream for the entire collection.
```

## CDC Implication

This choice directly affects CDC stream design:

```text
Subdirectories:
  /users           → CDC stream A
  /users/inactive  → CDC stream B (separate!)

Nested subspaces:
  /users (all data nested) → CDC stream A (captures everything)
```

For our bridge, knowing whether the application uses subdirectories or nested subspaces determines:

- How many CDC streams we need.
- Whether a single Kafka topic covers all of a directory's data.
- Whether we need to combine data from multiple CDC streams for a complete picture.

---

# 34. System Key Reservation (`\xFF`)

All keys starting with the byte `\xFF` (255) are **reserved by FoundationDB** for internal system use.

```text
FDB KEYSPACE

0x00...    ← application data begins
...
0xFE...    ← directory layer metadata (by convention)
0xFF...    ← RESERVED SYSTEM KEYS
  0xFF /conf/...         ← cluster configuration
  0xFF /coordinators     ← coordinator list
  0xFF /serverList/...   ← storage server list
  0xFF /keyServers/...   ← key-to-server mapping
  0xFF 0xFF ...          ← additional system ranges
```

These keys:

- **Cannot be read or written** in normal transactions.
- Require the `ACCESS_SYSTEM_KEYS` transaction option to access.
- Are managed entirely by FoundationDB internals.

## CDC Implication

If our CDC stream is configured to watch very broad ranges, the bridge must:

```text
Filter out:
  0xFE... → directory layer metadata
  0xFF... → system keys

Keep:
  0x00... through 0xFD... → actual application data
```

A simple prefix check is sufficient:

```python
if raw_key[0] >= 0xFE:
    # skip — this is metadata or system data
    continue
```

---

# 35. Cross-Directory Range Read Limitation

This is an important constraint that affects CDC design.

## The Limitation

Because sibling directories receive **independently allocated** prefixes, you **cannot** perform a single range read across multiple sibling directories:

```text
/users    → prefix 0x15 0x37
/orders   → prefix 0x15 0x26
/products → prefix 0x15 0x42
```

There is no single range `[begin, end)` that covers exactly these three directories and nothing else.

## Workarounds

1. **Multiple range reads** — one per directory.
2. **Directory partitions** — put related directories under a partition so they share a common prefix (see Section 28).
3. **Single directory with nested subspaces** — store all data under one directory.

## CDC Implication

This limitation means:

```text
To watch /users, /orders, and /products with CDC:

Option A: Three separate CDC streams (one per directory)
Option B: One broad CDC stream covering the entire keyspace
Option C: Use a partition to group them under a shared prefix
```

For our Kafka bridge, Option A gives the cleanest mapping (one directory → one Kafka topic), but requires managing more CDC streams.

---

# 36. Directory Layer Caching and Performance

The official docs and community guidance highlight performance considerations around the Directory Layer.

## Directory Lookups Require Database Reads

Every call to `open()`, `create_or_open()`, `list()`, etc. performs **actual FDB transactions** to read directory metadata:

```text
fdb.directory.open(db, ('users',))
    ↓
reads \xFE-prefixed metadata keys
    ↓
resolves path → prefix
    ↓
returns DirectorySubspace
```

This means directory operations are **not free**.

## Caching Recommendation

Applications should **cache** directory-to-prefix mappings:

```python
# Do this ONCE at startup
users = fdb.directory.open(db, ('users',))
orders = fdb.directory.open(db, ('orders',))

# Reuse the cached DirectorySubspace objects
# in all subsequent transactions
db[users.pack((123, 'email'))] = b'new@email.com'
```

Do **not** open directories inside hot transaction loops.

## Metadata Hotspot Risk

Because all directory metadata lives under `\xFE`, heavy directory operations can create a **hot key** on the storage server responsible for that range.

This is usually not a problem because:

- Directories are created/modified infrequently.
- Data operations use the directory's prefix, not `\xFE`.

But it could matter during bulk directory creation or migration.

## CDC Bridge Implication

Our bridge should:

```text
1. Open/resolve all directories ONCE at startup.
2. Cache the prefix → directory path mappings.
3. Use the cached map for CDC key resolution.
4. Periodically refresh the cache (or watch for
   \xFE mutations indicating directory changes).
```

This avoids per-mutation directory lookups which would be prohibitively expensive at high throughput.

---

# 37. The `layer` Parameter: Tagging Directories

Each directory can optionally store a `layer` byte string as metadata:

```python
users = fdb.directory.create(db, ('users',), layer=b'user_data_v2')
```

The layer value is:

- Stored in the directory metadata.
- Returned when the directory is opened.
- Used to **type-check** directories — if you open with a layer that doesn't match, it raises an error.

## Special Layer Values

```text
b'partition'   → creates a directory partition (Section 28)
b''            → default, no special behavior
b'<anything>'  → application-defined tag
```

## CDC Implication

The layer value could be useful for our bridge:

```text
Directory: /users
Layer: b'user_records'

Bridge sees a mutation in the /users prefix.
Bridge reads the layer value.
Bridge uses it to determine:
  - which Protobuf schema to use for serialization
  - which Kafka topic to route to
  - what downstream processing to apply
```

This provides a mechanism for **application-level metadata** to influence CDC event handling without hardcoding directory-to-schema mappings.

---

# 38. Updated Questions for the Team

Based on the additional research above, these questions should be added to the team discussion from Section 22:

## High Contention Allocator

10. Should the bridge cache directory-to-prefix mappings at startup, or re-resolve them periodically?

11. If directories are created dynamically at runtime, how does the bridge learn about new directories?

---

## Partitions

12. Does the application use directory partitions?

13. If so, should we watch the partition-level range (one stream) or individual subdirectory ranges (multiple streams)?

---

## Tenants

14. Is the FDB deployment multi-tenant?

15. If yes, should we produce per-tenant Kafka topics?

16. Should CDC streams be tenant-scoped or global?

---

## Metadata Filtering

17. Should the bridge filter out `\xFE` (directory metadata) and `\xFF` (system key) mutations?

18. Or should directory structure changes (create/move/delete) be published as separate Kafka events?

---

## Tuple Decoding

19. Should the bridge decode tuple-encoded keys into structured data before publishing to Kafka?

20. If so, how does the bridge know the expected tuple schema for each directory?

---

## Layer Parameter

21. Should the directory `layer` value be included in Kafka event metadata?

22. Can the `layer` value drive Protobuf schema selection?

---

# 39. References

This document draws from the following official FoundationDB sources:

- [Developer Guide — FoundationDB 7.4.7](https://apple.github.io/foundationdb/developer-guide.html)
  - Namespace Management
  - Subspaces
  - Directories (Usage, Subdirectories and Nested Subspaces, Directory Partitions)
  - Tenants (Tenants and Directories)
- [Data Modeling — FoundationDB](https://apple.github.io/foundationdb/data-modeling.html)
  - Encoding Data Types
  - Tuples
- [Python API Reference — FoundationDB](https://apple.github.io/foundationdb/api-python.html)
  - DirectoryLayer class
  - DirectorySubspace class
  - Subspace class
  - Tuple Layer
- [Tenants — FoundationDB](https://apple.github.io/foundationdb/tenants.html)
- [FoundationDB Tuple Layer Typecodes Specification](https://github.com/apple/foundationdb/blob/main/design/tuple.md)
