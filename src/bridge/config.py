"""Bridge configuration from CLI flags and environment variables."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

DEFAULT_CLUSTER_FILE = "/etc/foundationdb/fdb.cluster"
DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC = "fdb-cdc"
DEFAULT_BATCH_SIZE = 100
DEFAULT_POLL_INTERVAL_MS = 100
DEFAULT_METRICS_HOST = "127.0.0.1"
DEFAULT_METRICS_PORT = 9102


@dataclass(frozen=True)
class BridgeConfig:
    """Runtime settings for the CDC → Kafka bridge daemon.

    Attributes:
        cluster_file: Path to the FoundationDB cluster file.
        kafka_bootstrap_servers: Kafka broker list (recorded for the producer).
        subspace_prefix: Optional key prefix used to register the CDC stream.
        topic: Destination Kafka topic.
        batch_size: Reserved for future Kafka batching (validated ``>= 1``).
        poll_interval_ms: Idle sleep between empty polls.
        stream_name: Registered CDC stream name.
        metrics_host: Observability HTTP bind host.
        metrics_port: Observability HTTP bind port.
    """

    cluster_file: str
    kafka_bootstrap_servers: str
    subspace_prefix: bytes | None
    topic: str
    batch_size: int
    poll_interval_ms: int
    stream_name: str
    metrics_host: str
    metrics_port: int


def parse_subspace_prefix(raw: str) -> bytes:
    """Decode a subspace prefix from hex or UTF-8 text.

    Args:
        raw: ``0x``-prefixed hex, even-length hex, or a UTF-8 string.

    Returns:
        The prefix as bytes.

    Raises:
        ValueError: If hex decoding fails.
    """
    text = raw.strip()
    if not text:
        raise ValueError("subspace prefix must be non-empty")
    if text.startswith(("0x", "0X")):
        hex_body = text[2:]
        if len(hex_body) % 2 != 0:
            raise ValueError("hex subspace prefix must have an even length")
        try:
            return bytes.fromhex(hex_body)
        except ValueError as exc:
            raise ValueError(f"invalid hex subspace prefix: {exc}") from exc
    if len(text) % 2 == 0 and all(c in "0123456789abcdefABCDEF" for c in text):
        try:
            return bytes.fromhex(text)
        except ValueError as exc:
            raise ValueError(f"invalid hex subspace prefix: {exc}") from exc
    return text.encode("utf-8")


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def _positive_int(raw: str, *, field: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{field} must be >= 1")
    return value


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the daemon CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="streamline-daemon",
        description="FoundationDB CDC → Kafka bridge daemon.",
    )
    parser.add_argument(
        "--cluster-file",
        default=None,
        help=f"FDB cluster file (env FDB_CLUSTER_FILE, default {DEFAULT_CLUSTER_FILE})",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=None,
        help=(
            "Kafka bootstrap servers (env KAFKA_BOOTSTRAP_SERVERS, "
            f"default {DEFAULT_KAFKA_BOOTSTRAP})"
        ),
    )
    parser.add_argument(
        "--subspace-prefix",
        default=None,
        help="CDC registration prefix: 0x-hex, even hex, or UTF-8 string",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help=f"Kafka topic (env KAFKA_TOPIC, default {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--batch-size",
        default=None,
        help=(
            "Reserved batch size "
            f"(env STREAMLINE_BATCH_SIZE, default {DEFAULT_BATCH_SIZE})"
        ),
    )
    parser.add_argument(
        "--poll-interval-ms",
        default=None,
        help=(
            "Idle poll sleep in ms (env STREAMLINE_POLL_INTERVAL_MS, "
            f"default {DEFAULT_POLL_INTERVAL_MS})"
        ),
    )
    parser.add_argument(
        "--stream-name",
        default=None,
        help="CDC stream name (env STREAMLINE_STREAM_NAME, default: topic)",
    )
    parser.add_argument(
        "--metrics-host",
        default=None,
        help=(
            "Metrics bind host "
            f"(env STREAMLINE_METRICS_HOST, default {DEFAULT_METRICS_HOST})"
        ),
    )
    parser.add_argument(
        "--metrics-port",
        default=None,
        help=(
            "Metrics bind port "
            f"(env STREAMLINE_METRICS_PORT, default {DEFAULT_METRICS_PORT})"
        ),
    )
    return parser


def load_config(argv: list[str] | None = None) -> BridgeConfig:
    """Parse CLI argv and environment into a :class:`BridgeConfig`.

    Args:
        argv: Argument list without the program name. ``None`` uses ``sys.argv[1:]``.

    Returns:
        Parsed configuration.

    Raises:
        SystemExit: On argparse usage errors.
        ValueError: On invalid numeric or subspace values.
    """
    args = build_arg_parser().parse_args(argv)

    cluster_file = args.cluster_file or _env("FDB_CLUSTER_FILE") or DEFAULT_CLUSTER_FILE
    kafka_bootstrap = (
        args.kafka_bootstrap_servers
        or _env("KAFKA_BOOTSTRAP_SERVERS")
        or DEFAULT_KAFKA_BOOTSTRAP
    )
    topic = args.topic or _env("KAFKA_TOPIC") or DEFAULT_TOPIC
    stream_name = args.stream_name or _env("STREAMLINE_STREAM_NAME") or topic

    subspace_raw = args.subspace_prefix or _env("STREAMLINE_SUBSPACE_PREFIX")
    subspace_prefix = (
        parse_subspace_prefix(subspace_raw) if subspace_raw is not None else None
    )

    batch_raw = args.batch_size or _env("STREAMLINE_BATCH_SIZE")
    batch_size = (
        _positive_int(batch_raw, field="batch-size")
        if batch_raw is not None
        else DEFAULT_BATCH_SIZE
    )

    poll_raw = args.poll_interval_ms or _env("STREAMLINE_POLL_INTERVAL_MS")
    poll_interval_ms = (
        _positive_int(poll_raw, field="poll-interval-ms")
        if poll_raw is not None
        else DEFAULT_POLL_INTERVAL_MS
    )

    metrics_host = (
        args.metrics_host or _env("STREAMLINE_METRICS_HOST") or DEFAULT_METRICS_HOST
    )
    port_raw = args.metrics_port or _env("STREAMLINE_METRICS_PORT")
    if port_raw is None:
        metrics_port = DEFAULT_METRICS_PORT
    else:
        metrics_port = _positive_int(str(port_raw), field="metrics-port")
        if metrics_port > 65535:
            raise ValueError("metrics-port must be <= 65535")

    return BridgeConfig(
        cluster_file=cluster_file,
        kafka_bootstrap_servers=kafka_bootstrap,
        subspace_prefix=subspace_prefix,
        topic=topic,
        batch_size=batch_size,
        poll_interval_ms=poll_interval_ms,
        stream_name=stream_name,
        metrics_host=metrics_host,
        metrics_port=metrics_port,
    )
