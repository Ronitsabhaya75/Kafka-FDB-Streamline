# Kafka producer specification

`fdb_kafka_bridge.kafka.MutationProducer` is the publishing boundary for serialized CDC
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

These settings are applied after `extra_config`, and `extra_config` may not
contain these exact keys. Their librdkafka aliases (`request.required.acks`,
`max.in.flight`, `delivery.timeout.ms`) are not rejected and could weaken them,
so callers must not pass them. Five
in-flight requests preserves ordering because idempotence is enabled. The
gapless guarantee makes librdkafka raise a fatal error rather than commit later messages past a
permanently failed one. Setting `message.timeout.ms=0` (infinite timeout) is
required because librdkafka's gapless guarantee does not cover messages that time
out locally; infinite local timeout ensures temporary broker outages cannot cause
silent gaps in message sequence delivery. These settings order delivery within
one producer session only; ordering across a producer restart comes from the
caller resuming at its last checkpoint, not from the producer. Performance
settings, including `linger.ms` and `compression.type`, remain configurable.
Durability keys and `transactional.id` in `extra_config` are rejected with
`ValueError` at construction time.

A user `error_cb` in `extra_config` is chained after the wrapper's own handler;
exceptions from it, or from a per-message `on_delivery` callback, are logged
and never propagate into librdkafka. A delivery error is recorded before the
user callback runs, so a raising callback cannot hide it.

### Nontransactional mode

This configuration provides at-least-once, ordered delivery within one producer
session: idempotence removes duplicates from internal retries, but a record
replayed after a restart is written again. It is not end-to-end exactly-once
delivery (see [`kafka-delivery-guarantees.md`](kafka-delivery-guarantees.md),
a research write-up). Delivery errors are **sticky**: the first
delivery-callback error is preserved and re-raised on every subsequent
`produce()` (before enqueueing) and `flush()` (after flushing) call, so a caller
cannot re-send version V after V+1 has been queued. Sticky errors never clear
and do **not** set `fatal`. The CDC bridge must not acknowledge an FDB version
until `flush()` returns zero (or `flush_or_raise()` completes without error).

The caller must replace the producer if it is `closed`, `fatal`, or has raised a
sticky delivery error; recover by discarding it and resuming from the last
checkpoint.

### Transactional mode

When `transactional_id` is supplied, the underlying librdkafka producer is
configured for Kafka transactions. The caller must invoke `init_transactions()`
exactly once before the first `begin_transaction()`; calling it again after a
success raises `RuntimeError`, while a call that raised leaves the producer
uninitialised. Transaction methods guard on transactional mode, initialization
state, and open transaction state.

`produce()` in transactional mode requires an open transaction (between
`begin_transaction()` and `commit_transaction()` or `abort_transaction()`);
calling `produce()` outside an open transaction raises `RuntimeError`.

Unlike nontransactional mode, delivery errors do not block subsequent `produce()`
calls within an open transaction; `flush()` continues to raise if any delivery
errors occurred, until the transaction is resolved via `commit_transaction()` or
`abort_transaction()`.

Transaction-scoped delivery errors are cleared only after a successful
`commit_transaction()` or `abort_transaction()`. Purge reports for messages
still queued at an abort do not surface as errors in the next transaction
(checked only by the integration suite). A commit that fails with a non-fatal error leaves the
transaction open, the producer non-fatal, and the errors in place; the caller
retries the commit or aborts (see [Error classification](#error-classification)).
If `abort_transaction()` itself fails, for any reason, the delivery errors are
preserved, the transaction stays open, and the producer becomes fatal (see
[Fatal and closed states](#fatal-and-closed-states)): the producer is **not**
reusable after a failed abort.

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

The wrapper does not enforce this classification on a failed commit; it only
keeps the transaction open (unless the error is fatal) and leaves the choice to
the caller.

- **Retriable** (`retriable()`): the commit may succeed on retry. Retry
  `commit_transaction()` while `retriable()` is true, or give up and call
  `abort_transaction()`.
- **Abort-required** (`txn_requires_abort()`): the transaction cannot be committed
  and must be aborted with `abort_transaction()` before a new transaction can begin.
- **Fatal** (`fatal()`): the producer is permanently broken and must be
  replaced.

### Fatal and closed states

The producer becomes fatal on exactly these triggers:

- `error_cb` receives an error whose `fatal()` is true (a non-fatal one is only
  logged);
- `init_transactions()`, `begin_transaction()` or `commit_transaction()` raises a
  `KafkaException` whose error is fatal;
- `abort_transaction()` raises any exception, fatal or not;
- `mark_fatal()`.

`produce()` and `flush()` do not inspect `KafkaException.fatal()`; a fatal
librdkafka error there surfaces through `error_cb`. Marking the producer fatal on
a retriable abort failure is stricter than librdkafka requires, but safe: the
transaction state is unknown, so the wrapper refuses further use. `fatal` does not
record which trigger fired, so the caller cannot tell a fenced producer from one
whose abort merely failed; either way it must be replaced.

Public read-only properties:

- `transactional` — whether a `transactional_id` was supplied.
- `closed` — whether `close()` has run.
- `fatal` — whether any trigger above has fired.

All remain readable after close or fatal. `closed` is checked before `fatal`, so
a producer that is both reports closed. Blocked methods raise `RuntimeError`:

| Method | Closed | Fatal |
|--------|--------|-------|
| `produce()`, `flush()`, `flush_or_raise()`, `__len__()` | `Producer is closed` | `Producer is in a fatal error state` |
| `init_transactions()`, `begin_transaction()`, `commit_transaction()`, `abort_transaction()` | `<method> cannot proceed: producer is closed` | `<method> cannot proceed: producer is in a fatal error state` |

Transaction methods additionally raise `<method> requires a transactional_id`
on a nontransactional producer and `<method> requires init_transactions() to be
called first` before initialisation.

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
  the wrapper marked closed. `close()` logs but does not propagate exceptions
  (a failed abort is logged at DEBUG), and is idempotent.
- Without the purge, pending delivery callbacks would keep the wrapper (and its
  librdkafka threads) alive indefinitely, retrying forever under
  `message.timeout.ms=0`.

## Tests

All tests below except the integration tests run in the ordinary suite without a
broker. `test_producer.py` mocks librdkafka. `test_producer_close_release.py`
uses real librdkafka against the unreachable address `127.0.0.1:1`.

Live round-trip tests are opt-in. From the host, or a container started with
`--network host` (as CI does), the broker is `localhost:9092`:

```bash
KAFKA_INTEGRATION=1 \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
pytest tests/kafka/test_producer_integration.py -m integration -v
```

Inside the devcontainer the broker is `kafka:19092`; the `app` service already
exports `KAFKA_BOOTSTRAP_SERVERS=kafka:19092`.

The smoke script runs on the host, since it shells out to `docker compose exec`.
With the development Kafka service running, it publishes a sample Protobuf
message to `localhost:9092`, then runs Kafka's console consumer inside the
`kafka` container (against `kafka:19092`) to verify its key and that its value
decodes back to the record sent:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d kafka
PYTHONPATH=.:src:protobuf/gen \
  .venv/bin/python scripts/kafka_producer_smoke.py
```

The script uses a unique topic by default. Pass `--topic`,
`--bootstrap-servers`, or `--console-bootstrap-servers` to override its local
defaults.

## Record size

librdkafka rejects a message at `produce()` with `MSG_SIZE_TOO_LARGE` when
`len(key) + len(value)` plus a 36-byte per-message overhead exceeds
`message.max.bytes` (default 1,000,000). The CDC listener's default
`max_record_bytes` is also 1,000,000, so it leaves no room for the key or the
overhead: a record near that size passes the listener but is rejected by
`produce()`. A caller that needs every listener-accepted record to be producible
must keep `max_record_bytes + len(key) + 36 <= message.max.bytes` (and within the
broker and topic `max.message.bytes`).
