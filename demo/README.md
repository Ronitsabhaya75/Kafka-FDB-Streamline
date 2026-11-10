# Live 10,000-mutation bridge demo

`run_demo.py` is the live FoundationDB CDC to Kafka demonstration. It does not
use a browser or a simulator. The script walks through seven visible stages:

1. Connect to FoundationDB with API version 800.
2. Configure and start the autonomous Kafka bridge daemon.
3. Commit 10,000 representative FDB writes and atomic mutations.
4. Stream CDC mutations through Protobuf into Kafka with durable acknowledgements.
5. Consume the records from Kafka.
6. Verify every mutation byte for byte against the committed workload, plus
   VersionEnd boundaries, sequence numbers and declared totals.
7. Drain and stop the bridge cleanly, then report final metrics.

## Prerequisites

Running the demonstration requires live FoundationDB (the `FDB_VERSION` pinned
in the [`Dockerfile`](../Dockerfile), with native CDC enabled) and Kafka broker
services, normally supplied by the project's Docker Compose stack.

Run the demo from inside the application container so `kafka:19092` and the FDB
cluster file are available:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d --build
docker compose -f .devcontainer/docker-compose.yml exec app \
  pip3 install --break-system-packages -r requirements-dev.txt
docker compose -f .devcontainer/docker-compose.yml exec app \
  env PYTHONPATH=.:src:protobuf/gen python3 demo/run_demo.py --count 10000
```

The broker address defaults to `$KAFKA_BOOTSTRAP_SERVERS`, else `kafka:19092`
(the compose file sets the variable in the `app` container). Use
`--kafka-bootstrap-servers` and `--timeout` to override the broker address or the
default one-hour deadline, and `--count` to change the number of mutations.

The FDB image is `linux/amd64` only (FDB publishes x86_64 binaries), so on ARM
Macs the `app` container runs under emulation: expect a slower build and demo.
