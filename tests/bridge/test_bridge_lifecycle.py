"""Tests for Bridge build, rebuild and stop lifecycle."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from fdb_kafka_bridge.bridge.config import BridgeConfig, from_dict
from fdb_kafka_bridge.bridge.pipeline import (
    Bridge,
    BridgePipeline,
    _load_cursor_from_file,
)
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.listener import FDBMutationListener
from fdb_kafka_bridge.kafka.producer import MutationProducer


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
        patch("fdb_kafka_bridge.bridge.pipeline.FDBClient") as client_cls,
        patch("fdb_kafka_bridge.bridge.pipeline.FDBMutationListener") as listener_cls,
        patch("fdb_kafka_bridge.bridge.pipeline.MutationProducer") as producer_cls,
        patch("fdb_kafka_bridge.bridge.pipeline._load_cursor_from_file") as load_cursor,
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
    listener.cancel_pending.side_effect = lambda: unblock.set()

    # Simulate a native consume that only future cancellation unblocks.
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
        with patch.object(pipeline, "step") as step, pytest.raises(TimeoutError):
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


def test_unresponsive_close_cannot_extend_total_shutdown_budget(
    wiring: dict[str, MagicMock],
) -> None:
    bridge = Bridge(_config())
    bridge.start()
    producer = bridge._producer
    release = threading.Event()
    producer.close.side_effect = lambda **_: release.wait(3)
    began = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="total shutdown"):
            bridge.stop()
        elapsed = time.monotonic() - began
        assert 0.15 <= elapsed < 0.6
        assert producer.close.call_args.kwargs["timeout"] <= 0.2
    finally:
        release.set()


def test_shutdown_during_rebuild_skips_new_listener_poll(
    wiring: dict[str, MagicMock],
) -> None:
    config = from_dict(
        {
            "cdc": {"stream_name": "s"},
            "pipeline": {"backoff_base_seconds": 0.001, "shutdown_timeout_seconds": 2},
        }
    )
    bridge = Bridge(config)
    bridge.start()
    client = bridge._client
    old_pipeline = bridge._pipeline
    replacement = _listener()
    rebuilding, release = threading.Event(), threading.Event()
    failures: list[BaseException] = []

    def build_listener(*args: Any, **kwargs: Any) -> MagicMock:
        rebuilding.set()
        assert release.wait(2)
        return replacement

    def run() -> None:
        try:
            bridge.run()
        except BaseException as exc:
            failures.append(exc)

    def stop() -> None:
        try:
            bridge.stop()
        except BaseException as exc:
            failures.append(exc)

    wiring["listener_cls"].side_effect = build_listener
    with (
        patch.object(old_pipeline, "step", side_effect=RuntimeError("rebuild")),
        patch.object(bridge, "_needs_rebuild", return_value=True),
    ):
        worker = threading.Thread(target=run)
        closer = threading.Thread(target=stop)
        worker.start()
        try:
            assert rebuilding.wait(2)
            assert bridge._listener is None
            closer.start()
            assert bridge._shutdown_requested.wait(2)
        finally:
            release.set()
            worker.join(3)
            if closer.ident is not None:
                closer.join(3)
    assert not worker.is_alive()
    assert not closer.is_alive()
    assert not failures
    replacement.poll_records.assert_not_called()
    replacement.acknowledge.assert_not_called()
    replacement.close.assert_called_once()
    client.close.assert_called_once()
    assert bridge._listener is None
    assert bridge._producer is None


def test_run_after_stop_is_a_clean_noop(wiring: dict[str, MagicMock]) -> None:
    bridge = Bridge(_config())
    bridge.start()
    listener, producer = bridge._listener, bridge._producer
    bridge.stop()
    bridge.run()
    listener.poll_records.assert_not_called()
    listener.acknowledge.assert_not_called()
    producer.produce.assert_not_called()
    assert bridge.metrics.errors_retried == 0


def test_sigterm_between_daemon_gate_and_run_returns_cleanly(
    wiring: dict[str, MagicMock],
) -> None:
    import os
    import signal

    from fdb_kafka_bridge.bridge.cli import run_daemon

    bridge = Bridge(_config())
    run, stop = bridge.run, bridge.stop
    closed = threading.Event()

    def late_run() -> None:
        os.kill(os.getpid(), signal.SIGTERM)
        assert closed.wait(2)
        run()

    def stop_then_release(*, deadline: float | None = None) -> None:
        try:
            stop(deadline=deadline)
        finally:
            closed.set()

    with (
        patch.object(bridge, "run", side_effect=late_run),
        patch.object(bridge, "stop", side_effect=stop_then_release),
    ):
        run_daemon(bridge)
    assert closed.is_set()
    assert bridge._listener is None
    assert bridge._producer is None
    assert bridge._client is None
