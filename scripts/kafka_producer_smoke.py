"""Publish a Protobuf mutation and verify it with Kafka's console consumer."""

from __future__ import annotations

import argparse
import subprocess
import time
import uuid
from collections.abc import Sequence

from src.kafka import MutationProducer
from tests.kafka.utils import build_set_mutation_record


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--console-bootstrap-servers", default="kafka:19092")
    parser.add_argument(
        "--topic", default=f"fdb-cdc-producer-smoke-{uuid.uuid4().hex[:8]}"
    )
    parser.add_argument("--timeout", type=float, default=15.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the local Kafka producer smoke test.

    Args:
        argv: Optional command-line arguments.

    Returns:
        Process exit code.
    """
    args = _parser().parse_args(argv)
    key = f"smoke-{uuid.uuid4().hex}".encode()
    producer = MutationProducer(args.bootstrap_servers)
    record = build_set_mutation_record(
        stream="smoke-stream",
        version=time.time_ns(),
        seq=0,
        key=b"smoke:key",
        value=b"smoke:value",
    )
    producer.produce(args.topic, record, key=key)
    remaining = producer.flush(args.timeout)
    if remaining:
        raise RuntimeError(f"Kafka still has {remaining} undelivered message(s)")

    command = [
        "docker",
        "compose",
        "-f",
        ".devcontainer/docker-compose.yml",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        args.console_bootstrap_servers,
        "--topic",
        args.topic,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        str(int(args.timeout * 1000)),
        "--property",
        "print.key=true",
        "--property",
        "print.value=true",
    ]
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        timeout=args.timeout + 5,
    )
    stdout = result.stdout
    if key not in stdout:
        raise RuntimeError(f"console consumer did not receive key {key!r}")

    for line in stdout.split(b"\n"):
        line = line.rstrip(b"\r")
        if line.startswith(key + b"\t"):
            value_bytes = line[len(key) + 1 :]
            from fdbkafka.cdc.v1 import mutations_pb2

            parsed = mutations_pb2.FDBMutationRecord.FromString(value_bytes)
            if parsed.stream_name != "smoke-stream":
                raise RuntimeError(
                    "Decoded Protobuf does not match expected stream_name: "
                    f"{parsed.stream_name!r}"
                )
            print(
                f"verified Protobuf record on {args.topic!r} "
                f"with key {key.decode()!r}"
            )
            return 0

    raise RuntimeError(
        "Could not find and parse the protobuf value from console consumer output"
    )


if __name__ == "__main__":
    raise SystemExit(main())
