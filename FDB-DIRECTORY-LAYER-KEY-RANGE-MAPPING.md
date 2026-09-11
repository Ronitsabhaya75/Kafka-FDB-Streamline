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
