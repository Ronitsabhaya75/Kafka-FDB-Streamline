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
contain them or their librdkafka aliases (`request.required.acks`,
`max.in.flight`, `delivery.timeout.ms`), so callers cannot weaken them. Five
in-flight requests preserves ordering because idempotence is enabled. The
gapless guarantee makes librdkafka raise a fatal error rather than commit later messages past a
permanently failed one. Setting `message.timeout.ms=0` (infinite timeout) is
required because librdkafka's gapless guarantee does not cover messages that time
out locally; infinite local timeout ensures temporary broker outages cannot cause
silent gaps in message sequence delivery. These settings order delivery within
one producer session only; ordering across a producer restart comes from the
caller resuming at its last checkpoint, not from the producer. Performance
settings, including `linger.ms` and `compression.type`, remain configurable.
Durability keys, their aliases and `transactional.id` in `extra_config` are
rejected with `ValueError` at construction time.

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

Transaction-scoped delivery errors are cleared only after a successful
`commit_transaction()` or `abort_transaction()`. A commit that fails with a
non-fatal error leaves the transaction open and the errors in place; the caller
retries the commit or aborts. If `abort_transaction()` itself fails, for any
reason, the delivery errors are preserved, the transaction stays open, and the
producer becomes fatal (see [Fatal and closed states](#fatal-and-closed-states)):
the producer is **not** reusable after a failed abort.

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
  replaced.

### Fatal and closed states

The producer becomes fatal on exactly these triggers:

| Trigger | `fatal_cause` |
|---------|---------------|
| `error_cb` receives an error whose `fatal()` is true | `FatalCause.KAFKA` |
| `init_transactions()`, `begin_transaction()` or `commit_transaction()` raises a `KafkaException` whose error is fatal | `FatalCause.KAFKA` |
| `abort_transaction()` raises a fatal `KafkaException` | `FatalCause.KAFKA` |
| `abort_transaction()` raises anything else (non-fatal `KafkaException`, or any other exception) | `FatalCause.ABORT_FAILED` |
| `mark_fatal()` | `FatalCause.KAFKA` |

`produce()` and `flush()` do not inspect `KafkaException.fatal()`; a fatal
librdkafka error there surfaces through `error_cb`. Marking the producer fatal on
a retriable abort failure is stricter than librdkafka requires, but safe: the
transaction state is unknown, so the wrapper refuses further use.

`KAFKA` takes precedence: it overrides an earlier `ABORT_FAILED`, and
`ABORT_FAILED` never replaces `KAFKA`. `fatal_cause` lets the caller choose a
recovery: `ABORT_FAILED` means only this producer instance is unusable, so a
replacement may succeed; `KAFKA` covers fencing and other conditions a
replacement is not expected to fix.

Public read-only properties:

- `transactional` — whether a `transactional_id` was supplied.
- `closed` — whether `close()` has run.
- `fatal` — `fatal_cause is not None`.
- `fatal_cause` — `FatalCause | None` (exported from `fdb_kafka_bridge.kafka`).

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
and `test_producer_record_size.py` use real librdkafka against the unreachable
address `127.0.0.1:1`.

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
`max_record_bytes` is therefore 999,000, leaving room for a stream-name key of
up to 964 bytes. A caller raising `max_record_bytes` must keep
`max_record_bytes + len(key) + 36 <= message.max.bytes` (and within the broker
and topic `max.message.bytes`).

## Invariants

- <a id="inv-kafka-1"></a>**INV-KAFKA-1.** The mandatory durability config is
  applied after `extra_config`; `extra_config` containing a mandatory key, a
  librdkafka alias of one, or `transactional.id` is rejected with `ValueError`.
  Enforced by: `producer.MutationProducer.__init__`, `producer._FORBIDDEN_EXTRA_KEYS`.
  Pinned by: `tests/kafka/test_producer.py::test_enforces_durability_config`,
  `tests/kafka/test_producer.py::test_mandatory_keys_rejected_in_extra_config`,
  `tests/kafka/test_producer.py::test_librdkafka_alias_keys_rejected_in_extra_config`,
  `tests/kafka/test_producer.py::test_transactional_id_in_extra_config_rejected`.
- <a id="inv-kafka-2"></a>**INV-KAFKA-2.** In nontransactional mode, after a
  delivery error `produce()` raises it before enqueueing and `flush()` raises it
  after flushing. Enforced by: `producer.MutationProducer.produce`,
  `producer.MutationProducer.flush`. Pinned by:
  `tests/kafka/test_producer.py::test_produce_raises_after_delivery_failure`,
  `tests/kafka/test_producer.py::test_flush_raises_delivery_failure`,
  `tests/kafka/test_producer.py::test_sticky_error_blocks_produce`.
- <a id="inv-kafka-3"></a>**INV-KAFKA-3.** Nontransactional delivery errors
  never clear and do not set `fatal`. Enforced by:
  `producer.MutationProducer._delivery_callback`. Pinned by:
  `tests/kafka/test_producer.py::test_sticky_error_persists_across_flushes`,
  `tests/kafka/test_producer.py::test_sticky_error_blocks_produce`.
- <a id="inv-kafka-4"></a>**INV-KAFKA-4.** In transactional mode `produce()`
  requires an open transaction. Enforced by: `producer.MutationProducer.produce`.
  Pinned by:
  `tests/kafka/test_producer.py::test_produce_in_transactional_mode_requires_open_transaction`.
- <a id="inv-kafka-5"></a>**INV-KAFKA-5.** Transaction-scoped delivery errors
  clear only on a successful `commit_transaction()` or `abort_transaction()`.
  Enforced by: `producer.MutationProducer.commit_transaction`,
  `producer.MutationProducer.abort_transaction`. Pinned by:
  `tests/kafka/test_producer.py::test_commit_clears_delivery_errors`,
  `tests/kafka/test_producer.py::test_abort_clears_delivery_errors`,
  `tests/kafka/test_producer.py::test_produce_abort_then_clean_flush`,
  `tests/kafka/test_producer.py::test_failed_abort_preserves_errors_and_marks_fatal`,
  `tests/kafka/test_producer.py::test_non_fatal_failed_commit_leaves_transaction_open`.
- <a id="inv-kafka-6"></a>**INV-KAFKA-6.** A commit that fails with a non-fatal
  error leaves the transaction open and the producer non-fatal. Enforced by:
  `producer.MutationProducer.commit_transaction`. Pinned by:
  `tests/kafka/test_producer.py::test_non_fatal_failed_commit_leaves_transaction_open`.
- <a id="inv-kafka-7"></a>**INV-KAFKA-7.** Any `abort_transaction()` failure
  makes the producer fatal (`FatalCause.KAFKA` if the error is a fatal Kafka
  error, else `FatalCause.ABORT_FAILED`), preserves delivery errors and leaves
  the transaction open. Enforced by: `producer.MutationProducer.abort_transaction`.
  Pinned by:
  `tests/kafka/test_producer.py::test_failed_abort_preserves_errors_and_marks_fatal`,
  `tests/kafka/test_producer.py::test_abort_fatal_kafka_error_sets_kafka_cause`,
  `tests/kafka/test_producer.py::test_abort_non_fatal_kafka_error_sets_abort_failed_cause`,
  `tests/kafka/test_producer.py::test_abort_non_kafka_exception_sets_abort_failed_cause`.
- <a id="inv-kafka-8"></a>**INV-KAFKA-8.** A fatal `error_cb` error, a fatal
  `KafkaException` from `init_transactions()`, `begin_transaction()` or
  `commit_transaction()`, and `mark_fatal()` set `fatal_cause` to
  `FatalCause.KAFKA`, which `ABORT_FAILED` never replaces; a non-fatal
  `error_cb` error does not set `fatal`. Enforced by:
  `producer.MutationProducer._error_cb`, `producer.MutationProducer.init_transactions`,
  `producer.MutationProducer.begin_transaction`,
  `producer.MutationProducer.commit_transaction`,
  `producer.MutationProducer.abort_transaction`, `producer.MutationProducer.mark_fatal`.
  Pinned by:
  `tests/kafka/test_producer.py::test_error_cb_fatal_sets_kafka_cause`,
  `tests/kafka/test_producer.py::test_error_cb_non_fatal_does_not_mark_fatal`,
  `tests/kafka/test_producer.py::test_init_fatal_kafka_error_sets_kafka_cause`,
  `tests/kafka/test_producer.py::test_begin_fatal_kafka_error_sets_kafka_cause`,
  `tests/kafka/test_producer.py::test_commit_fatal_kafka_error_sets_kafka_cause`,
  `tests/kafka/test_producer.py::test_mark_fatal_sets_kafka_cause`,
  `tests/kafka/test_producer.py::test_kafka_cause_overrides_abort_failed`,
  `tests/kafka/test_producer.py::test_mark_fatal_overrides_abort_failed`,
  `tests/kafka/test_producer.py::test_abort_failed_never_downgrades_kafka`.
- <a id="inv-kafka-9"></a>**INV-KAFKA-9.** `closed` is checked before `fatal`;
  either blocks `produce()`, `flush()`, `flush_or_raise()`, `__len__()` and all
  transaction methods with `RuntimeError`. Enforced by:
  `producer.MutationProducer._require_transactional`, `producer.MutationProducer.produce`,
  `producer.MutationProducer.flush`, `producer.MutationProducer.__len__`. Pinned by:
  `tests/kafka/test_producer.py::test_closed_reported_before_fatal`,
  `tests/kafka/test_producer.py::test_closed_blocks_transaction_methods`,
  `tests/kafka/test_producer.py::test_use_after_close_raises_closed_error`,
  `tests/kafka/test_producer.py::test_fatal_blocks_produce`,
  `tests/kafka/test_producer.py::test_fatal_blocks_flush`,
  `tests/kafka/test_producer.py::test_fatal_blocks_transaction_methods`,
  `tests/kafka/test_producer.py::test_len_raises_when_fatal`,
  `tests/kafka/test_producer.py::test_len_raises_when_closed`.
- <a id="inv-kafka-10"></a>**INV-KAFKA-10.** `init_transactions()` succeeds at
  most once and must precede other transaction methods; a failed call leaves the
  producer uninitialised. Enforced by: `producer.MutationProducer.init_transactions`,
  `producer.MutationProducer._require_transactional`. Pinned by:
  `tests/kafka/test_producer.py::test_init_transactions_cannot_be_called_twice`,
  `tests/kafka/test_producer.py::test_failed_init_does_not_mark_initialized`,
  `tests/kafka/test_producer.py::test_begin_before_init_fails`,
  `tests/kafka/test_producer.py::test_commit_before_init_fails`,
  `tests/kafka/test_producer.py::test_abort_before_init_fails`.
- <a id="inv-kafka-11"></a>**INV-KAFKA-11.** `close()` is idempotent, never
  raises, and purges messages still undelivered after its flush, releasing the
  librdkafka producer. Enforced by: `producer.MutationProducer.close`. Pinned by:
  `tests/kafka/test_producer.py::test_close_twice_is_noop`,
  `tests/kafka/test_producer.py::test_close_swallows_failed_abort`,
  `tests/kafka/test_producer.py::test_close_logs_warning_when_flush_raises`,
  `tests/kafka/test_producer.py::test_close_aborts_open_transaction`,
  `tests/kafka/test_producer_close_release.py::test_close_purges_queued_message_and_releases_producer`,
  `tests/kafka/test_producer_close_release.py::test_close_with_nothing_queued_releases_without_reports`.
- <a id="inv-kafka-12"></a>**INV-KAFKA-12.** Purge delivery reports from an
  aborted transaction do not leak into the next transaction. Enforced by:
  `producer.MutationProducer.abort_transaction`. Pinned by (integration only):
  `tests/kafka/test_producer_integration.py::TestTransactionalProducerIntegration::test_produce_abort_then_clean_flush`.
- <a id="inv-kafka-13"></a>**INV-KAFKA-13.** Exceptions from a user
  `on_delivery` or `error_cb` callback never escape into librdkafka; the
  delivery error is still recorded. Enforced by:
  `producer.MutationProducer._delivery_callback`, `producer.MutationProducer._error_cb`.
  Pinned by:
  `tests/kafka/test_producer.py::test_user_on_delivery_exception_does_not_escape`,
  `tests/kafka/test_producer.py::test_user_error_cb_exception_is_swallowed`,
  `tests/kafka/test_producer.py::test_user_error_cb_is_chained`.
- <a id="inv-kafka-14"></a>**INV-KAFKA-14.** A record of the CDC listener's
  default `max_record_bytes` with a stream-name key is accepted by `produce()`
  under the default `message.max.bytes` (see [Record size](#record-size)).
  Enforced by: `cdc.listener.DEFAULT_MAX_RECORD_BYTES`. Pinned by:
  `tests/kafka/test_producer_record_size.py::test_max_size_record_with_stream_key_is_accepted`.
