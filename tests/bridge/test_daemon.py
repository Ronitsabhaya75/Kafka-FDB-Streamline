"""Unit tests for BridgeDaemon run loop and shutdown."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.bridge.config import BridgeConfig
from src.bridge.daemon import BridgeDaemon


def _config(**overrides: object) -> BridgeConfig:
    base: dict[str, object] = {
        "cluster_file": "/tmp/fdb.cluster",
        "kafka_bootstrap_servers": "localhost:9092",
        "subspace_prefix": None,
        "topic": "fdb-cdc",
        "batch_size": 100,
        "poll_interval_ms": 1,
        "stream_name": "fdb-cdc",
        "metrics_host": "127.0.0.1",
        "metrics_port": 0,
    }
    base.update(overrides)
    return BridgeConfig(**base)  # type: ignore[arg-type]


def test_one_poll_produce_ack_cycle() -> None:
    listener = MagicMock()
    producer = MagicMock()
    records = [object()]
    calls = {"n": 0}

    def poll() -> list[object]:
        calls["n"] += 1
        if calls["n"] == 1:
            return records
        daemon.request_stop()
        return []

    listener.poll_records.side_effect = poll
    sleeps: list[float] = []

    daemon = BridgeDaemon(
        _config(),
        client=MagicMock(),
        listener=listener,
        producer=producer,
        sleep=sleeps.append,
        start_metrics=False,
    )
    daemon.run()

    producer.produce.assert_called_once_with(records)
    listener.acknowledge.assert_called_once()
    listener.start.assert_called_once()
    producer.close.assert_called_once()
    listener.close.assert_called_once()


def test_empty_poll_sleeps_without_ack() -> None:
    listener = MagicMock()
    producer = MagicMock()
    calls = {"n": 0}

    def poll() -> list[object]:
        calls["n"] += 1
        if calls["n"] >= 2:
            daemon.request_stop()
        return []

    listener.poll_records.side_effect = poll
    sleeps: list[float] = []

    daemon = BridgeDaemon(
        _config(poll_interval_ms=5),
        client=MagicMock(),
        listener=listener,
        producer=producer,
        sleep=sleeps.append,
        start_metrics=False,
    )
    daemon.run()

    assert sleeps == [0.005]
    producer.produce.assert_not_called()
    listener.acknowledge.assert_not_called()


def test_stop_after_produce_still_acknowledges() -> None:
    listener = MagicMock()
    producer = MagicMock()
    records = [object()]

    def poll() -> list[object]:
        daemon.request_stop()
        return records

    def produce(_records: object) -> int:
        return 1

    listener.poll_records.side_effect = poll
    producer.produce.side_effect = produce

    daemon = BridgeDaemon(
        _config(),
        client=MagicMock(),
        listener=listener,
        producer=producer,
        sleep=lambda _s: None,
        start_metrics=False,
    )
    daemon.run()

    producer.produce.assert_called_once_with(records)
    listener.acknowledge.assert_called_once()


def test_request_stop_sets_flag() -> None:
    daemon = BridgeDaemon(
        _config(),
        client=MagicMock(),
        listener=MagicMock(),
        producer=MagicMock(),
        start_metrics=False,
    )
    assert not daemon._stop.is_set()
    daemon.request_stop()
    assert daemon._stop.is_set()
