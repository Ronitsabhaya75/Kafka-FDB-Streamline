# Live 10,000-mutation bridge demo

`run_demo.py` is the live FoundationDB CDC to Kafka demonstration. It does not
use a browser or a simulator. The script walks through seven visible stages:

1. Connect to FoundationDB with API version 800.
2. Configure and start the autonomous Kafka bridge daemon.
3. Commit 10,000 representative FDB writes and atomic mutations.
4. Stream CDC mutations through Protobuf into Kafka with durable acknowledgements.
5. Consume the records from Kafka.
6. Audit every Protobuf schema and emitted mutation type.
7. Drain and stop the bridge cleanly, then report final metrics.

## Prerequisites

The project branch used to run this demo must provide
`src.bridge.config.from_dict` and `src.bridge.pipeline.Bridge`. This checkout
does not currently contain those daemon APIs. It also requires live FDB 8.0 CDC
and Kafka services, normally supplied by the project's Docker Compose stack.

Once those daemon APIs are present, run the demo from inside the application
container so `kafka:19092` and the FDB cluster file are available:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d --build
docker compose -f .devcontainer/docker-compose.yml exec app \
  env PYTHONPATH=.:protobuf/gen python3 demo/run_demo.py --count 10000
```

Use `--kafka-bootstrap-servers` and `--timeout` to override the broker address
or the default one-hour deadline.
