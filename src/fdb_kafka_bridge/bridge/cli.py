"""Production CLI for the existing Bridge daemon with durable FDB checkpoints."""

import argparse
import logging
import os
import re
import signal
import threading
from types import FrameType
from typing import Any

from fdb_kafka_bridge.bridge.config import BridgeConfig, from_dict
from fdb_kafka_bridge.bridge.pipeline import Bridge
from fdb_kafka_bridge.bridge.startup import StartupError
from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointFormatError,
    CheckpointOverlapError,
    CheckpointSinkMismatchError,
    CheckpointStreamMismatchError,
)
from fdb_kafka_bridge.cdc.errors import CDCInvalidCursorError
from fdb_kafka_bridge.cdc.listener import subspace_to_key_range

logger = logging.getLogger(__name__)


def load_config(argv: list[str] | None = None) -> BridgeConfig:
    """Parse flags over environment settings into the library's validated config.

    Args:
        argv: Arguments without program name; None reads sys.argv.

    Returns:
        Configuration using FDB checkpoints by default.

    Raises:
        SystemExit: Code two for invalid arguments or settings.
    """
    parser = argparse.ArgumentParser(description="Stream FoundationDB CDC to Kafka")
    options = {
        "stream-name": ("STREAMLINE_STREAM_NAME", None, str),
        "kafka-bootstrap-servers": ("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092", str),
        "cluster-file": ("FDB_CLUSTER_FILE", None, str),
        "topic": ("KAFKA_TOPIC", None, str),
        "transactional-id": ("STREAMLINE_TRANSACTIONAL_ID", None, str),
        "subspace-prefix": ("STREAMLINE_SUBSPACE_PREFIX", None, str),
        "checkpoint-path": (
            "STREAMLINE_CHECKPOINT_PATH",
            "fdb-kafka-bridge/checkpoints",
            str,
        ),
        "checkpoint-timeout-s": ("STREAMLINE_CHECKPOINT_TIMEOUT_S", "5", float),
        "checkpoint-retry-limit": ("STREAMLINE_CHECKPOINT_RETRY_LIMIT", "10", int),
        "flush-timeout-s": ("STREAMLINE_FLUSH_TIMEOUT_S", "30", float),
        "shutdown-timeout-s": ("STREAMLINE_SHUTDOWN_TIMEOUT_S", "30", float),
        "max-record-bytes": ("STREAMLINE_MAX_RECORD_BYTES", "900000", int),
    }
    for flag, (env, default, converter) in options.items():
        parser.add_argument(
            f"--{flag}",
            default=os.environ.get(env, default),
            type=converter,
            help=f"Environment: {env}",
        )
    args = parser.parse_args(argv)
    if args.stream_name is None:
        parser.error("--stream-name or STREAMLINE_STREAM_NAME is required")
    try:
        topic = args.topic if args.topic is not None else f"fdb.cdc.{args.stream_name}"
        if (
            len(topic) > 249
            or topic in (".", "..")
            or re.fullmatch(r"[a-zA-Z0-9._-]+", topic) is None
        ):
            raise ValueError(
                "Invalid Kafka topic; use --topic for non-ASCII stream names"
            )
        cdc: dict[str, Any] = {
            "stream_name": args.stream_name,
            "max_record_bytes": args.max_record_bytes,
            "checkpoint_path": args.checkpoint_path.split("/"),
            "checkpoint_timeout_seconds": args.checkpoint_timeout_s,
            "checkpoint_retry_limit": args.checkpoint_retry_limit,
        }
        if args.subspace_prefix is not None:
            prefix = args.subspace_prefix
            raw = (
                bytes.fromhex(prefix[2:])
                if prefix.startswith("0x")
                else prefix.encode()
            )
            cdc["begin_key"], cdc["end_key"] = subspace_to_key_range(raw)
        return from_dict(
            {
                "fdb": {"cluster_file": args.cluster_file},
                "cdc": cdc,
                "kafka": {
                    "bootstrap_servers": args.kafka_bootstrap_servers,
                    "topic": topic,
                    "transactional_id": args.transactional_id,
                },
                "pipeline": {
                    "flush_timeout_seconds": args.flush_timeout_s,
                    "shutdown_timeout_seconds": args.shutdown_timeout_s,
                },
            }
        )
    except (ValueError, TypeError) as exc:
        parser.error(str(exc))


def run_daemon(bridge: Bridge) -> None:
    """Run Bridge with signal handling and bounded teardown on the main thread.

    Args:
        bridge: Configured bridge whose resources are managed by this function.

    Raises:
        Exception: Startup, worker or teardown failure.
        RuntimeError: If the worker cannot stop within the configured join budget.
    """
    failures: list[BaseException] = []
    stop_requested = threading.Event()
    handlers: dict[signal.Signals, Any] = {}
    worker: threading.Thread | None = None

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop_requested.set()
        bridge.request_shutdown()

    def run() -> None:
        try:
            bridge.run()
        except BaseException as exc:
            failures.append(exc)

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, request_stop)
        bridge.start()
        worker = threading.Thread(target=run, name="cdc-kafka-bridge", daemon=True)
        worker.start()
        while worker.is_alive() and not stop_requested.wait(0.1):
            worker.join(0.1)
    finally:
        try:
            bridge.stop()
        finally:
            if worker is not None:
                worker.join(bridge.config.pipeline.shutdown_timeout_seconds)
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
    if failures:
        raise failures[0]
    if worker is not None and worker.is_alive():
        raise RuntimeError("Bridge worker did not stop within the shutdown budget")


def main(argv: list[str] | None = None) -> int:
    """Run the bridge CLI and return a supervisor-compatible status.

    Args:
        argv: Command-line arguments without program name.

    Returns:
        Zero on clean stop, one for runtime failures, three for unsafe startup.
        Invalid configuration raises SystemExit with code two.
    """
    config = load_config(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    try:
        run_daemon(Bridge(config))
    except (
        StartupError,
        CheckpointFormatError,
        CheckpointOverlapError,
        CheckpointSinkMismatchError,
        CheckpointStreamMismatchError,
        CDCInvalidCursorError,
    ):
        logger.exception("Unsafe startup or checkpoint; operator action required")
        return 3
    except Exception:
        logger.exception("Bridge failed")
        return 1
    return 0
