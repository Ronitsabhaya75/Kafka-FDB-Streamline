"""Production CLI entrypoint and argument parser for the Kafka Bridge daemon."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from fdb_kafka_bridge.bridge.config import (
    BridgeConfig,
    env_to_dict,
    from_dict,
)
from fdb_kafka_bridge.bridge.pipeline import Bridge

logger = logging.getLogger("fdb_kafka_bridge.bridge.cli")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the bridge daemon.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="fdb-kafka-bridge",
        description="Autonomous FoundationDB CDC to Kafka Bridge daemon.",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config_file",
        help="Path to JSON configuration file.",
    )

    # FDB group
    fdb_group = parser.add_argument_group("FoundationDB options")
    fdb_group.add_argument(
        "--fdb-api-version",
        type=int,
        help="FoundationDB API version (default: 800).",
    )
    fdb_group.add_argument(
        "--fdb-cluster-file",
        help="Path to fdb.cluster file.",
    )

    # CDC group
    cdc_group = parser.add_argument_group("CDC options")
    cdc_group.add_argument(
        "--cdc-stream-name",
        help="Registered CDC stream name.",
    )
    cdc_group.add_argument(
        "--cdc-begin-key",
        help="Hex-encoded beginning key for stream registration.",
    )
    cdc_group.add_argument(
        "--cdc-end-key",
        help="Hex-encoded ending key for stream registration.",
    )
    cdc_group.add_argument(
        "--cdc-max-record-bytes",
        type=int,
        help="Maximum Protobuf record size in bytes (default: 1,000,000).",
    )
    cdc_group.add_argument(
        "--cdc-cursor-file",
        help="Path to atomic cursor file for durable progress tracking.",
    )
    cdc_group.add_argument(
        "--cdc-idle-ack-max-replies",
        type=int,
        help="Maximum count of consecutive idle replies before watermark ack.",
    )
    cdc_group.add_argument(
        "--cdc-idle-ack-max-seconds",
        type=float,
        help="Optional maximum elapsed seconds before forced idle watermark ack.",
    )

    # Kafka group
    kafka_group = parser.add_argument_group("Kafka options")
    kafka_group.add_argument(
        "--kafka-bootstrap-servers",
        help="Kafka bootstrap broker list (default: localhost:9092).",
    )
    kafka_group.add_argument(
        "--kafka-topic",
        help="Destination Kafka topic name (default: fdb-cdc).",
    )
    kafka_group.add_argument(
        "--kafka-transactional-id",
        help="Transactional ID for exactly-once publishing to Kafka.",
    )

    # Pipeline group
    pipeline_group = parser.add_argument_group("Pipeline options")
    pipeline_group.add_argument(
        "--flush-timeout-seconds",
        type=float,
        help="Timeout in seconds for Kafka producer flush (default: 10.0).",
    )
    pipeline_group.add_argument(
        "--backoff-base-seconds",
        type=float,
        help="Initial backoff delay in seconds on recoverable errors (default: 0.1).",
    )
    pipeline_group.add_argument(
        "--backoff-max-seconds",
        type=float,
        help="Maximum backoff delay in seconds (default: 2.0).",
    )
    pipeline_group.add_argument(
        "--shutdown-timeout-seconds",
        type=float,
        help="Graceful shutdown timeout in seconds (default: 15.0).",
    )

    return parser


def parse_config(
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> BridgeConfig:
    """Parse configuration hierarchically: Defaults -> Config File -> Env -> CLI.

    Args:
        argv: Command-line arguments. Defaults to ``sys.argv[1:]``.
        env: Environment variables mapping. Defaults to ``os.environ``.

    Returns:
        Structured and validated BridgeConfig instance.

    Raises:
        ValueError: If required configuration is missing or invalid.
        TypeError: If an option has an incorrect type.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if env is None:
        env = os.environ

    # 1. Base from file or empty dict
    config_data: dict[str, Any] = {}
    config_path = args.config_file or env.get("BRIDGE_CONFIG_FILE")
    if config_path:
        with open(config_path, encoding="utf-8") as f:
            config_data = json.load(f)

    # 2. Merge environment variables
    env_data = env_to_dict(env)
    for section, sec_data in env_data.items():
        for k, v in sec_data.items():
            if v is not None:
                config_data.setdefault(section, {})[k] = v

    # Helper to set nested dict value
    def _set(section: str, key: str, val: Any) -> None:
        if val is not None:
            config_data.setdefault(section, {})[key] = val

    # 3. CLI flags take highest precedence
    _set("fdb", "api_version", args.fdb_api_version)
    _set("fdb", "cluster_file", args.fdb_cluster_file)
    _set("cdc", "stream_name", args.cdc_stream_name)
    _set("cdc", "begin_key", args.cdc_begin_key)
    _set("cdc", "end_key", args.cdc_end_key)
    _set("cdc", "max_record_bytes", args.cdc_max_record_bytes)
    _set("cdc", "cursor_file", args.cdc_cursor_file)
    _set("cdc", "idle_ack_max_replies", args.cdc_idle_ack_max_replies)
    _set("cdc", "idle_ack_max_seconds", args.cdc_idle_ack_max_seconds)
    _set("kafka", "bootstrap_servers", args.kafka_bootstrap_servers)
    _set("kafka", "topic", args.kafka_topic)
    _set("kafka", "transactional_id", args.kafka_transactional_id)
    _set("pipeline", "flush_timeout_seconds", args.flush_timeout_seconds)
    _set("pipeline", "backoff_base_seconds", args.backoff_base_seconds)
    _set("pipeline", "backoff_max_seconds", args.backoff_max_seconds)
    _set("pipeline", "shutdown_timeout_seconds", args.shutdown_timeout_seconds)

    return from_dict(config_data)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the bridge daemon from command-line arguments.

    Args:
        argv: Command-line arguments. Defaults to ``sys.argv[1:]``.

    Returns:
        Process exit code (0 for clean shutdown, 1 for fatal error).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        config = parse_config(argv)
    except Exception as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    bridge = Bridge(config)

    def _signal_handler(signum: int, _frame: Any) -> None:
        logger.info("Signal %d received, requesting graceful shutdown...", signum)
        bridge.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        bridge.start()
        bridge.run()
    except Exception as exc:
        logger.critical("Fatal daemon crash: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
