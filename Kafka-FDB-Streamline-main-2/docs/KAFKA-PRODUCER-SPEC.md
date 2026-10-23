# Kafka producer specification

`src.kafka.MutationProducer` is the publishing boundary for serialized CDC
records. It accepts an `FDBMutationRecord`, serializes it to Protobuf wire bytes,
and enqueues it with `confluent-kafka`.

## Delivery contract

The wrapper always applies:

```text
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
```

In non-transactional mode the gapless guarantee is also enforced:

```text
enable.gapless.guarantee=true
```

Callers cannot weaken these settings through `extra_config`. Five in-flight
requests preserves ordering because idempotence is enabled. The gapless guarantee
(non-transactional only) makes librdkafka raise a fatal error rather than commit
later messages past a permanently failed one. Performance settings, including
`linger.ms` and `compression.type`, remain configurable.

### Sticky errors

Delivery errors are **sticky**. Once any message permanently fails, every
subsequent `produce()` and `flush()` on the same instance raises the original
error. A flush timeout also poisons the producer. The caller must discard the
instance and create a new one, resuming from the last acknowledged FDB version.
This fail-stop behaviour ensures the bridge never silently drops records or
continues producing past a gap.

### Flush contract

`flush()` blocks until all queued messages are broker-acknowledged. It raises
`KafkaException` on:

- **Delivery failure**: a message was permanently rejected by the broker.
- **Timeout**: not all messages were acknowledged within the deadline.

In both cases the producer is poisoned. The bridge must not acknowledge the FDB
CDC cursor unless `flush()` returns without raising.

## Transactional mode

When a `transactional_id` is supplied at construction, the producer acquires a
transactional context. The caller drives the lifecycle:

1. `init_transactions()` — one-time handshake with the transaction coordinator.
2. `begin_transaction()` — open a new transaction.
3. `produce()` — enqueue records inside the transaction.
4. `commit_transaction()` — atomically make all records visible to
   `read_committed` consumers.
5. `abort_transaction()` — discard all records in the current transaction.

Gapless guarantee is omitted in transactional mode because the transaction
commit/abort lifecycle already provides atomicity: either every record in the
transaction is visible or none are.

Producer **fencing** is automatic: when a new producer instance calls
`init_transactions()` with the same `transactional_id`, any in-flight
transactions from a zombie predecessor are aborted by the broker.

### Error classification

On `commit_transaction()` failure, the `KafkaError` object exposes:

- `txn_requires_abort()` → `True`: abort the transaction, then retry.
- `retriable()` → `True`: abort the transaction, back off, then retry.
- Neither: the error is **fatal**; the producer is unrecoverable.

In all three cases the pipeline must **not** acknowledge the FDB CDC cursor.

## Close

`close()` attempts a best-effort flush and releases the underlying producer.
It does **not** clear the sticky error state: a caller who retains a reference
after close still sees the producer as poisoned.

## Tests

Unit tests mock librdkafka and run as part of the ordinary test suite without a
broker. Live round-trip tests are opt-in:

```bash
KAFKA_INTEGRATION=1 \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
pytest tests/kafka/test_producer_integration.py -m integration -v
```

With the development Kafka service running, the smoke script publishes a sample
Protobuf message and invokes Kafka's console consumer inside the container to
verify its key and that its value decodes back to the record sent:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d kafka
PYTHONPATH=.:protobuf/gen \
  .venv/bin/python scripts/kafka_producer_smoke.py
```

The script uses a unique topic by default. Pass `--topic`,
`--bootstrap-servers`, or `--console-bootstrap-servers` to override its local
defaults.
