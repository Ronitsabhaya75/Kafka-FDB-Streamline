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
```

Callers cannot weaken these settings through `extra_config`. Five in-flight
requests preserves ordering because idempotence is enabled. The gapless guarantee
makes librdkafka raise a fatal error rather than commit later messages past a
permanently failed one. Performance settings, including `linger.ms` and
`compression.type`, remain configurable. Durability keys and `transactional.id`
in `extra_config` are rejected with `ValueError` at construction time.

### Nontransactional mode

This configuration provides durable, ordered, idempotent delivery within one
producer session. Delivery errors are **sticky**: the first delivery-callback
error is preserved and re-raised on every subsequent `produce()` and `flush()`
call. The producer must be replaced after a permanent delivery failure. The CDC
bridge must not acknowledge an FDB version until `flush()` returns zero.

### Transactional mode

When `transactional_id` is supplied, the underlying librdkafka producer is
configured for Kafka transactions. The caller must invoke `init_transactions()`
exactly once before the first `begin_transaction()`. Transaction methods guard
on both transactional mode and initialization state.

Transaction-scoped delivery errors are cleared after a successful
`commit_transaction()` or `abort_transaction()`. If abort itself fails, delivery
errors are preserved and the producer should be treated as fatal.

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

- **Retriable**: the commit may succeed on retry.  The bridge aborts and
  replays instead, since the CDC batch can be reprocessed.
- **Abort-required** (`txn_requires_abort()`): the transaction must be aborted
  before a new one can begin.
- **Fatal** (`fatal()`): the producer is permanently broken and must be
  replaced.

### Fatal state

After `mark_fatal()` or `close()`, all operations except `close()` raise
`RuntimeError`. The caller must create a new `MutationProducer`.

## Close behaviour

`close()` attempts to abort any open transaction (transactional mode), then
flushes remaining messages. It logs but does not propagate exceptions. After
close, the producer is marked fatal. If undelivered messages remain after the
final flush, a warning is logged.

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
