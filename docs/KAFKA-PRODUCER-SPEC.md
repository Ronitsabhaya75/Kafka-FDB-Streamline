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
`compression.type`, remain configurable.

This configuration provides durable, ordered, idempotent delivery within one
producer session. It is not end-to-end exactly-once delivery: that requires the
transaction and cursor protocol described in
[`kafka-delivery-guarantees.md`](kafka-delivery-guarantees.md). The CDC bridge must
not acknowledge an FDB version until `flush()` returns zero. A delivery report
failure makes `flush()` raise `KafkaException` and therefore prevents that
acknowledgement. The failure is final: every later `produce()` and `flush()` on
the same instance raises it again, so a caller cannot re-send version V after
V+1 has been queued. Recover by discarding the producer and resuming from the
last acknowledged FDB version.

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
