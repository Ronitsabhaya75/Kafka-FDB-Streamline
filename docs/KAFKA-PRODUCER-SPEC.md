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
enable.gapless.guarantee=true
message.timeout.ms=0
```

Callers cannot weaken these settings through `extra_config`. Five in-flight
requests preserves ordering because idempotence is enabled. The gapless guarantee
makes librdkafka raise a fatal error rather than commit later messages past a
permanently failed one. Setting `message.timeout.ms=0` (infinite timeout) is
required because librdkafka's gapless guarantee does not cover messages that time
out locally; infinite local timeout ensures temporary broker outages cannot cause
silent gaps in message sequence delivery. Strict total ordering across broker
outages and producer restarts is reinforced by transactional mode. Performance
settings, including `linger.ms` and `compression.type`, remain configurable.
Durability keys and `transactional.id` in `extra_config` are rejected with
`ValueError` at construction time.

### Nontransactional mode

This configuration provides durable, ordered, idempotent delivery within one
producer session. It is not end-to-end exactly-once delivery: that requires the
transaction and cursor protocol described in
[`kafka-delivery-guarantees.md`](kafka-delivery-guarantees.md). Delivery errors are
**sticky**: the first delivery-callback error is preserved and re-raised on every
subsequent `produce()` and `flush()` call, so a caller cannot re-send version V
after V+1 has been queued. The CDC bridge must not acknowledge an FDB version
until `flush()` returns zero (or `flush_or_raise()` completes without error).
The producer must be replaced after a permanent delivery failure; recover by
discarding the producer and resuming from the last acknowledged FDB version.

### Transactional mode

When `transactional_id` is supplied, the underlying librdkafka producer is
configured for Kafka transactions. The caller must invoke `init_transactions()`
exactly once before the first `begin_transaction()`; calling it again raises
`RuntimeError`. Transaction methods guard on transactional mode, initialization
state, and open transaction state.

`produce()` in transactional mode requires an open transaction (between
`begin_transaction()` and `commit_transaction()` or `abort_transaction()`);
calling `produce()` outside an open transaction raises `RuntimeError`.

Unlike nontransactional mode, delivery errors do not block subsequent `produce()`
calls within an open transaction; `flush()` continues to raise if any delivery
errors occurred, until the transaction is resolved via `commit_transaction()` or
`abort_transaction()`.

Transaction-scoped delivery errors are cleared after a successful
`commit_transaction()` or `abort_transaction()`. If abort itself fails, delivery
errors are preserved and the producer is marked fatal.

Transactional mode provides Kafka-side atomicity: `read_committed` consumers
observe each batch as all-or-nothing. It does **not** provide end-to-end
exactly-once between FDB and Kafka because the CDC cursor acknowledgement is
outside the Kafka transaction boundary.

## Transactional ID validation

- `transactional_id=None` (default): nontransactional mode.
- `transactional_id="some-id"`: transactional mode.
- `transactional_id=""`: rejected with `ValueError`.
- `extra_config={"transactional.id": ...}`: rejected with `ValueError`.
  Use the explicit `transactional_id` parameter instead.

## Transaction lifecycle

```
init_transactions()    → required once before any transaction
begin_transaction()    → start a new transaction
produce() ...          → enqueue records (inside the transaction)
commit_transaction()   → atomically commit all produced records
  OR
abort_transaction()    → discard all produced records
```

### Error classification

- **Retriable** (`retriable()`): the commit may succeed on retry. Callers should
  retry `commit_transaction()` while `retriable()` is true. Calling `abort_transaction()`
  during a retriable commit error can cause state conflicts or duplicate writes.
- **Abort-required** (`txn_requires_abort()`): the transaction cannot be committed
  and must be aborted with `abort_transaction()` before a new transaction can begin.
- **Fatal** (`fatal()`): the producer is permanently broken and must be
  replaced. Any error reported via `error_cb` where `err.fatal()` is true, or any
  fatal `KafkaException`, marks the producer fatal.

### Fatal and closed states

After `close()`, operational methods (`produce()`, `flush()`, `flush_or_raise()`,
`__len__()`, and transaction lifecycle methods) raise `RuntimeError("Producer is closed")`.
After `mark_fatal()` (or encountering an unrecoverable fatal Kafka error), these methods
raise `RuntimeError("Producer is in a fatal error state")`. The `transactional` property
remains accessible for configuration inspection.

## Flushing and close behaviour

- `flush(timeout)` blocks until all queued messages are delivered or timeout
  expires, returning the count of remaining messages.
- `flush_or_raise(timeout)` blocks until all messages are delivered; if any
  messages remain queued after timeout, it raises `TimeoutError`.
- `close(timeout=10.0)` aborts any open transaction, then flushes remaining
  messages; `timeout` bounds the abort and each flush. If messages remain
  undelivered (or the flush raises), a warning with the remaining count is logged
  and the queue is purged: their `on_delivery` callbacks receive
  `KafkaError._PURGE_QUEUE` or `KafkaError._PURGE_INFLIGHT`. A purged in-flight
  message may still have reached the broker, so its outcome is unknown and the
  caller must replay it. The underlying librdkafka producer is then released and
  the wrapper marked closed. `close()` logs but does not propagate exceptions, and
  is idempotent.
- Without the purge, pending delivery callbacks would keep the wrapper (and its
  librdkafka threads) alive indefinitely, retrying forever under
  `message.timeout.ms=0`.

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
