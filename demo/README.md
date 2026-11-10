# Live FoundationDB CDC → Kafka bridge demo

`run_demo.py` is the live FoundationDB CDC to Kafka demonstration. It does not
use a browser or a simulator.

## Modes

### Batch (default)

Write a fixed mutation count, then wait for the bridge to drain and verify
exact-once content against the committed workload:

1. Connect to FoundationDB with API version 800.
2. Configure and start the autonomous Kafka bridge daemon.
3. Commit representative FDB writes and atomic mutations.
4. Stream CDC mutations through Protobuf into Kafka with durable acknowledgements.
5. Consume the records from Kafka.
6. Verify every mutation byte for byte against the committed workload, plus
   VersionEnd boundaries, sequence numbers and declared totals.
7. Drain and stop the bridge cleanly, then report final metrics.

### Sustained

Keep writing while the bridge and a Kafka consumer run concurrently, then assert
catch-up and at-least-once delivery (duplicates reported, not treated as loss):

1. Start the bridge daemon and a background Kafka collector.
2. Run a background writer thread for `--duration` at `--target-rate`
   (optional `--count` cap). The main thread monitors backlog and catch-up.
3. Sample **mutation backlog** (`written - published`). This demo’s lag metric is
   that backlog (not FDB version-cursor distance). Soft-warn once above
   `--max-backlog`.
4. After writers stop, wait until backlog is zero within `--catchup-timeout`.
5. Drain the Kafka collector until decoded mutations cover the write set, then
   verify every written index appears ≥1 time; count duplicates; check VersionEnds.
6. Print written / published / acks / max backlog / catch-up / e2e timings.

**Lag threshold:** default `--max-backlog` is `max(target_rate * 5, 1000)` mutations
(about five seconds of target throughput). Exceeding it prints a one-time warning;
the hard gate is catch-up after writers stop. Monitor sample intervals in the
summary are progress-tick gaps, not Kafka produce RTT.

## Prerequisites

Running the demonstration requires live FoundationDB 8.0 (with CDC enabled)
and Kafka broker services, normally supplied by the project's Docker Compose stack.

## Batch command

```bash
docker compose -f .devcontainer/docker-compose.yml up -d --build
docker compose -f .devcontainer/docker-compose.yml exec app \
  pip3 install --break-system-packages -r requirements-dev.txt
docker compose -f .devcontainer/docker-compose.yml exec app \
  env PYTHONPATH=.:src:protobuf/gen python3 demo/run_demo.py --count 10000
```

Use `--kafka-bootstrap-servers` and `--timeout` to override the broker address
or the default one-hour deadline.

## Sustained command (card #54 reproduce)

```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
  env PYTHONPATH=.:src:protobuf/gen python3 demo/run_demo.py \
  --mode sustained --duration 30 --target-rate 500 --seed 1 \
  --catchup-timeout 60
```

Optional flags: `--count N` (hard write cap), `--max-backlog N` (soft lag threshold).

## Live integration test

```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
  env FDB_CDC_INTEGRATION=1 KAFKA_INTEGRATION=1 PYTHONPATH=.:src:protobuf/gen \
  pytest tests/demo/test_sustained_integration.py -m integration -v
```
