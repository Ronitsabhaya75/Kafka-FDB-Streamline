"""Unit tests for Bridge declarative configuration parsing."""

from __future__ import annotations

from src.bridge.config import (
    BridgeConfig,
    CDCConfig,
    FDBConfig,
    KafkaConfig,
    PipelineConfig,
    from_dict,
)


def test_default_config() -> None:
    """Verify default configurations have expected baseline settings."""
    cfg = BridgeConfig()
    assert cfg.fdb.api_version == 800
    assert cfg.fdb.cluster_file is None
    assert cfg.cdc.stream_name == ""
    assert cfg.cdc.begin_key == b""
    assert cfg.cdc.end_key == b""
    assert cfg.cdc.max_record_bytes == 1_000_000
    assert cfg.cdc.cursor_file is None
    assert cfg.kafka.bootstrap_servers == "localhost:9092"
    assert cfg.kafka.topic == "fdb-cdc"
    assert cfg.pipeline.flush_timeout_seconds == 10.0
    assert cfg.pipeline.backoff_base_seconds == 0.1
    assert cfg.pipeline.backoff_max_seconds == 2.0
    assert cfg.pipeline.shutdown_timeout_seconds == 15.0


def test_from_dict_empty() -> None:
    """Verify from_dict with empty dictionary returns default configuration."""
    cfg = from_dict({})
    assert isinstance(cfg, BridgeConfig)
    assert isinstance(cfg.fdb, FDBConfig)
    assert isinstance(cfg.cdc, CDCConfig)
    assert isinstance(cfg.kafka, KafkaConfig)
    assert isinstance(cfg.pipeline, PipelineConfig)
    assert cfg.fdb.api_version == 800
    assert cfg.cdc.stream_name == ""
    assert cfg.kafka.topic == "fdb-cdc"


def test_from_dict_custom_and_hex_keys() -> None:
    """Verify from_dict parses nested values and hex keys properly."""
    raw = {
        "fdb": {
            "api_version": 730,
            "cluster_file": "/tmp/test.cluster",
        },
        "cdc": {
            "stream_name": "test_stream",
            "begin_key": "616161",  # 'aaa' in hex
            "end_key": "626262",  # 'bbb' in hex
            "max_record_bytes": 500_000,
            "cursor_file": "/tmp/cursor.json",
        },
        "kafka": {
            "bootstrap_servers": "broker:9092",
            "topic": "events",
        },
        "pipeline": {
            "flush_timeout_seconds": 5.0,
            "backoff_base_seconds": 0.05,
            "backoff_max_seconds": 1.0,
            "shutdown_timeout_seconds": 8.0,
        },
    }
    cfg = from_dict(raw)
    assert cfg.fdb.api_version == 730
    assert cfg.fdb.cluster_file == "/tmp/test.cluster"
    assert cfg.cdc.stream_name == "test_stream"
    assert cfg.cdc.begin_key == b"aaa"
    assert cfg.cdc.end_key == b"bbb"
    assert cfg.cdc.max_record_bytes == 500_000
    assert cfg.cdc.cursor_file == "/tmp/cursor.json"
    assert cfg.kafka.bootstrap_servers == "broker:9092"
    assert cfg.kafka.topic == "events"
    assert cfg.pipeline.flush_timeout_seconds == 5.0
    assert cfg.pipeline.backoff_base_seconds == 0.05
    assert cfg.pipeline.backoff_max_seconds == 1.0
    assert cfg.pipeline.shutdown_timeout_seconds == 8.0


def test_from_dict_raw_bytes_keys() -> None:
    """Verify from_dict handles raw bytes for begin_key and end_key."""
    raw = {
        "cdc": {
            "begin_key": b"\x01\x02",
            "end_key": b"\x01\x03",
        }
    }
    cfg = from_dict(raw)
    assert cfg.cdc.begin_key == b"\x01\x02"
    assert cfg.cdc.end_key == b"\x01\x03"

