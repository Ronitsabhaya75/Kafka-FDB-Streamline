"""Tests for Bridge build, rebuild and stop lifecycle."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from src.bridge.config import BridgeConfig, from_dict
from src.bridge.pipeline import Bridge, BridgePipeline, _load_cursor_from_file
from src.cdc.client import FDBClient
from src.cdc.listener import FDBMutationListener
from src.kafka.producer import MutationProducer


def _config(**kafka: Any) -> BridgeConfig:
    return from_dict(
        {
            "cdc": {"stream_name": "s", "cursor_file": "/nonexistent/c.json"},
            "kafka": {"topic": "t", **kafka},
            "pipeline": {"shutdown_timeout_seconds": 0.2},
        }
    )


def _producer(transactional: bool = False) -> MagicMock:
    producer = create_autospec(MutationProducer, instance=True)
    producer.transactional = transactional
    return producer


def _listener() -> MagicMock:
    listener = create_autospec(FDBMutationListener, instance=True)
    listener.raw_stream_name = b"s"
    return listener


@pytest.fixture
def wiring() -> Iterator[dict[str, MagicMock]]:
    """Patch the classes Bridge builds from config."""
    with (
        patch("src.bridge.pipeline.FDBClient") as client_cls,
        patch("src.bridge.pipeline.FDBMutationListener") as listener_cls,
        patch("src.bridge.pipeline.MutationProducer") as producer_cls,
        patch("src.bridge.pipeline._load_cursor_from_file") as load_cursor,
    ):
        client_cls.return_value = create_autospec(FDBClient, instance=True)
        listener_cls.side_effect = lambda *a, **k: _listener()
        producer_cls.side_effect = lambda *a, **k: _producer(
            k.get("transactional_id") is not None
        )
        load_cursor.return_value = None
        yield {
            "client_cls": client_cls,
            "listener_cls": listener_cls,
            "producer_cls": producer_cls,
            "load_cursor": load_cursor,
        }


def test_start_builds_transactional_producer_from_config(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config(transactional_id="tx-1"))
    bridge.start()

    wiring["producer_cls"].assert_called_once_with(
        "localhost:9092", transactional_id="tx-1"
    )
    bridge._producer.init_transactions.assert_called_once()


def test_start_passes_stream_name_to_cursor_load(
    wiring: dict[str, MagicMock],
) -> None:
    Bridge(_config()).start()
    wiring["load_cursor"].assert_called_once_with(
        "/nonexistent/c.json", stream_name="s"
    )


def test_rebuild_keeps_transactional_mode_from_config(
    wiring: dict[str, MagicMock],
) -> None:
    injected = _producer(transactional=True)
    bridge = Bridge(_config(transactional_id="tx-1"), producer=injected)
    bridge.start()

    bridge._rebuild_pipeline()

    injected.close.assert_called_once()
    assert bridge._producer is not injected
    assert bridge._producer.transactional
    bridge._producer.init_transactions.assert_called_once()
    wiring["load_cursor"].assert_called_with("/nonexistent/c.json", stream_name="s")


def test_rebuild_uses_producer_factory(wiring: dict[str, MagicMock]) -> None:
    fresh = _producer(transactional=True)
    bridge = Bridge(
        _config(),
        producer=_producer(transactional=True),
        producer_factory=lambda: fresh,
    )
    bridge.start()
    bridge._rebuild_pipeline()

    assert bridge._producer is fresh
    fresh.init_transactions.assert_called_once()
    wiring["producer_cls"].assert_not_called()


def test_injected_transactional_producer_without_rebuild_source_rejected() -> None:
    with pytest.raises(ValueError, match="transactional"):
        Bridge(_config(), producer=_producer(transactional=True))


def test_failed_rebuild_raises_and_can_be_retried(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config())
    bridge.start()
    old_producer = bridge._producer

    broken = _listener()
    broken.start.side_effect = RuntimeError("fdb down")
    wiring["listener_cls"].side_effect = lambda *a, **k: broken

    with pytest.raises(RuntimeError, match="fdb down"):
        bridge._rebuild_pipeline()

    old_producer.close.assert_called_once()
    broken.close.assert_called_once()
    assert bridge._pipeline is None
    assert bridge._listener is None
    assert bridge._producer is None

    wiring["listener_cls"].side_effect = lambda *a, **k: _listener()
    bridge._rebuild_pipeline()
    assert isinstance(bridge._pipeline, BridgePipeline)
    assert bridge._listener is not None
    assert bridge._producer is not None


def test_failed_producer_build_closes_new_listener(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config())
    bridge.start()
    wiring["producer_cls"].side_effect = RuntimeError("bad kafka config")

    with pytest.raises(RuntimeError, match="bad kafka config"):
        bridge._rebuild_pipeline()

    assert bridge._listener is None
    assert bridge._pipeline is None


def test_rebuild_skipped_while_commit_outstanding(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config(transactional_id="tx-1"))
    bridge.start()
    producer = bridge._producer
    pipeline = bridge._pipeline
    pipeline._commit_outstanding = True

    bridge._rebuild_pipeline()

    producer.close.assert_not_called()
    assert bridge._pipeline is pipeline


def test_stop_finishes_outstanding_commit_before_closing(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config(transactional_id="tx-1"))
    bridge.start()
    producer = bridge._producer
    pipeline = bridge._pipeline
    pipeline._commit_outstanding = True

    calls: list[str] = []

    def _step() -> int:
        calls.append("step")
        pipeline._commit_outstanding = False
        return 1

    producer.close.side_effect = lambda *a, **k: calls.append("close")
    with patch.object(pipeline, "step", side_effect=_step):
        bridge.stop()

    assert calls == ["step", "close"]


def test_stop_bounded_when_commit_never_completes(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config(transactional_id="tx-1"))
    bridge.start()
    producer = bridge._producer
    pipeline = bridge._pipeline
    pipeline._commit_outstanding = True

    with patch.object(pipeline, "step", side_effect=RuntimeError("still retriable")):
        bridge.stop()

    producer.close.assert_called_once()


def test_stop_closes_everything_even_if_closes_raise(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config())
    bridge.start()
    producer, listener, client = bridge._producer, bridge._listener, bridge._client
    producer.close.side_effect = RuntimeError("producer close")
    listener.close.side_effect = RuntimeError("listener close")

    with pytest.raises(RuntimeError, match="producer close"):
        bridge.stop()

    listener.close.assert_called_once()
    client.close.assert_called_once()
    assert bridge._producer is None
    assert bridge._listener is None
    assert bridge._client is None


def test_stop_does_not_hang_behind_idle_step(wiring: dict[str, MagicMock]) -> None:
    bridge = Bridge(_config())
    bridge.start()
    listener = bridge._listener
    unblock = threading.Event()
    listener.close.side_effect = lambda: unblock.set()

    # Simulate step() parked in consume().wait() until the listener closes.
    held = threading.Event()

    def _park() -> None:
        with bridge._step_lock:
            held.set()
            unblock.wait(timeout=5)

    worker = threading.Thread(target=_park)
    worker.start()
    held.wait()

    began = time.monotonic()
    bridge.stop()
    worker.join(timeout=5)

    assert time.monotonic() - began < 2
    listener.close.assert_called()
    assert bridge._client is None


def test_stop_skips_commit_retry_while_step_still_running(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config(transactional_id="tx-1"))
    bridge.start()
    pipeline = bridge._pipeline
    pipeline._commit_outstanding = True

    # A step that closing the listener does not unblock (e.g. commit backoff).
    held, release = threading.Event(), threading.Event()

    def _hold() -> None:
        with bridge._step_lock:
            held.set()
            release.wait(timeout=5)

    worker = threading.Thread(target=_hold)
    worker.start()
    held.wait()
    try:
        with patch.object(pipeline, "step") as step:
            bridge.stop()
        step.assert_not_called()
    finally:
        release.set()
        worker.join(timeout=5)


def test_checkpoint_round_trips_through_stream_check(tmp_path: Any) -> None:
    cursor_file = str(tmp_path / "c.json")
    cfg = from_dict({"cdc": {"stream_name": "s", "cursor_file": cursor_file}})
    bridge = Bridge(cfg, client=create_autospec(FDBClient, instance=True))
    bridge._listener = _listener()
    bridge._producer = _producer()
    bridge.start()

    bridge._pipeline._checkpoint(MagicMock(stream_id=7, last_consumed_version=42))

    cursor = _load_cursor_from_file(cursor_file, stream_name="s")
    assert (cursor.stream_id, cursor.last_consumed_version) == (7, 42)
