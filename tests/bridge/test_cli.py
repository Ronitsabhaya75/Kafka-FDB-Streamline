"""Unit tests for the bridge daemon CLI argument parsing and entrypoint."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from fdb_kafka_bridge.bridge.cli import build_parser, main, parse_config


def test_build_parser_has_idle_coalescing_options() -> None:
    parser = build_parser()
    actions = {a.dest: a for a in parser._actions}
    assert "cdc_idle_ack_max_replies" in actions
    assert "cdc_idle_ack_max_seconds" in actions


def test_parse_config_cli_flags() -> None:
    argv = [
        "--cdc-stream-name",
        "cli_stream",
        "--cdc-idle-ack-max-replies",
        "7",
        "--cdc-idle-ack-max-seconds",
        "20.5",
        "--kafka-topic",
        "cli_topic",
        "--kafka-bootstrap-servers",
        "cli_kafka:9092",
        "--kafka-transactional-id",
        "cli_txn",
        "--flush-timeout-seconds",
        "5.5",
        "--shutdown-timeout-seconds",
        "12.0",
    ]
    cfg = parse_config(argv, env={})
    assert cfg.cdc.stream_name == "cli_stream"
    assert cfg.cdc.idle_ack_max_replies == 7
    assert cfg.cdc.idle_ack_max_seconds == 20.5
    assert cfg.kafka.topic == "cli_topic"
    assert cfg.kafka.bootstrap_servers == "cli_kafka:9092"
    assert cfg.kafka.transactional_id == "cli_txn"
    assert cfg.pipeline.flush_timeout_seconds == 5.5
    assert cfg.pipeline.shutdown_timeout_seconds == 12.0


def test_parse_config_file_and_cli_override(tmp_path: Path) -> None:
    config_file = tmp_path / "bridge.json"
    config_file.write_text(
        json.dumps(
            {
                "cdc": {
                    "stream_name": "file_stream",
                    "idle_ack_max_replies": 3,
                    "idle_ack_max_seconds": 10.0,
                },
                "kafka": {"topic": "file_topic"},
            }
        ),
        encoding="utf-8",
    )
    # CLI override
    cfg = parse_config(
        ["--config", str(config_file), "--cdc-idle-ack-max-replies", "15"], env={}
    )
    assert cfg.cdc.stream_name == "file_stream"
    assert cfg.cdc.idle_ack_max_replies == 15
    assert cfg.cdc.idle_ack_max_seconds == 10.0
    assert cfg.kafka.topic == "file_topic"


def test_parse_config_env_override(tmp_path: Path) -> None:
    config_file = tmp_path / "bridge.json"
    config_file.write_text(
        json.dumps({"cdc": {"stream_name": "file_stream"}}), encoding="utf-8"
    )
    env = {
        "FDB_CDC_IDLE_ACK_MAX_REPLIES": "9",
        "FDB_CDC_IDLE_ACK_MAX_SECONDS": "30.0",
        "KAFKA_TOPIC": "env_topic",
    }
    cfg = parse_config(["--config", str(config_file)], env=env)
    assert cfg.cdc.stream_name == "file_stream"
    assert cfg.cdc.idle_ack_max_replies == 9
    assert cfg.cdc.idle_ack_max_seconds == 30.0
    assert cfg.kafka.topic == "env_topic"


def test_main_runs_bridge_and_handles_clean_exit() -> None:
    with (
        patch("fdb_kafka_bridge.bridge.cli.Bridge") as mock_bridge_cls,
        patch("fdb_kafka_bridge.bridge.cli.signal.signal"),
    ):
        mock_bridge = MagicMock()
        mock_bridge_cls.return_value = mock_bridge
        exit_code = main(["--cdc-stream-name", "main_stream"])
        assert exit_code == 0
        mock_bridge.start.assert_called_once()
        mock_bridge.run.assert_called_once()


def test_main_handles_config_error() -> None:
    # Missing stream_name causes ValueError
    exit_code = main([])
    assert exit_code == 1
