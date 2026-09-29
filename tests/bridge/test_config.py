"""Unit tests for bridge CLI configuration parsing."""

from __future__ import annotations

import pytest

from src.bridge.config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CLUSTER_FILE,
    DEFAULT_KAFKA_BOOTSTRAP,
    DEFAULT_METRICS_HOST,
    DEFAULT_METRICS_PORT,
    DEFAULT_POLL_INTERVAL_MS,
    DEFAULT_TOPIC,
    load_config,
    parse_subspace_prefix,
)


def test_defaults() -> None:
    cfg = load_config([])
    assert cfg.cluster_file == DEFAULT_CLUSTER_FILE
    assert cfg.kafka_bootstrap_servers == DEFAULT_KAFKA_BOOTSTRAP
    assert cfg.subspace_prefix is None
    assert cfg.topic == DEFAULT_TOPIC
    assert cfg.stream_name == DEFAULT_TOPIC
    assert cfg.batch_size == DEFAULT_BATCH_SIZE
    assert cfg.poll_interval_ms == DEFAULT_POLL_INTERVAL_MS
    assert cfg.metrics_host == DEFAULT_METRICS_HOST
    assert cfg.metrics_port == DEFAULT_METRICS_PORT


def test_flags_override_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FDB_CLUSTER_FILE", "/env/fdb.cluster")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "env-topic")
    monkeypatch.setenv("STREAMLINE_BATCH_SIZE", "50")
    monkeypatch.setenv("STREAMLINE_POLL_INTERVAL_MS", "25")
    cfg = load_config(
        [
            "--cluster-file",
            "/flag/fdb.cluster",
            "--kafka-bootstrap-servers",
            "flag-kafka:9092",
            "--topic",
            "flag-topic",
            "--batch-size",
            "10",
            "--poll-interval-ms",
            "5",
            "--stream-name",
            "flag-stream",
        ]
    )
    assert cfg.cluster_file == "/flag/fdb.cluster"
    assert cfg.kafka_bootstrap_servers == "flag-kafka:9092"
    assert cfg.topic == "flag-topic"
    assert cfg.stream_name == "flag-stream"
    assert cfg.batch_size == 10
    assert cfg.poll_interval_ms == 5


def test_env_used_when_flags_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STREAMLINE_SUBSPACE_PREFIX", "0x6162")
    monkeypatch.setenv("STREAMLINE_STREAM_NAME", "env-stream")
    monkeypatch.setenv("STREAMLINE_METRICS_PORT", "9200")
    cfg = load_config([])
    assert cfg.subspace_prefix == b"ab"
    assert cfg.stream_name == "env-stream"
    assert cfg.metrics_port == 9200


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0x6162", b"ab"),
        ("0Xff00", b"\xff\x00"),
        ("6162", b"ab"),
        ("app/", b"app/"),
    ],
)
def test_parse_subspace_prefix(raw: str, expected: bytes) -> None:
    assert parse_subspace_prefix(raw) == expected


def test_parse_subspace_prefix_rejects_bad_hex() -> None:
    with pytest.raises(ValueError, match="even length"):
        parse_subspace_prefix("0xabc")
    with pytest.raises(ValueError, match="non-empty"):
        parse_subspace_prefix("   ")


def test_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch-size"):
        load_config(["--batch-size", "0"])


def test_rejects_non_positive_poll_interval() -> None:
    with pytest.raises(ValueError, match="poll-interval-ms"):
        load_config(["--poll-interval-ms", "-1"])
