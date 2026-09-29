"""Publish a Protobuf mutation and verify it with Kafka's console consumer."""

from __future__ import annotations

import argparse
import subprocess
import time
import uuid
from collections.abc import Sequence

from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.kafka import MutationProducer


def _record(stream_name: str) -> mutations_pb2.FDBMutationRecord:
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return mutations_pb2.FDBMutationRecord(
        stream_name=stream_name,
        bridge_timestamp=timestamp,
        mutation=mutations_pb2.FDBMutation(
            version_index=mutations_pb2.FDBVersionIndex(
                fdb_version=time.time_ns(), sequence_no=0
            ),
            single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                key=b"smoke:key",
                value=b"smoke:value",
                mutation_type=(
                    mutations_pb2.FDBSingleKeyMutation.MUTATION_TYPE_SET_VALUE
                ),
            ),
        ),
    )


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
    producer.produce(args.topic, _record("smoke-stream"), key=key)
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
        "print.value=false",
    ]
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=args.timeout + 5,
    )
    expected_key = key.decode()
    if expected_key not in result.stdout.splitlines():
        raise RuntimeError(
            f"console consumer did not receive key {expected_key!r}: {result.stdout!r}"
        )
    print(f"verified Protobuf record on {args.topic!r} with key {expected_key!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
