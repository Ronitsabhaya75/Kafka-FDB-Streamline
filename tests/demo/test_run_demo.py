"""Unit tests for the demo's bridge teardown (no FDB or Kafka needed)."""

import importlib
import sys
import threading
from types import ModuleType
from unittest.mock import create_autospec

import pytest

from src.bridge.pipeline import Bridge


@pytest.fixture
def run_demo(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # run_demo imports fdb at module level; only the FDB-writing helpers use it.
    monkeypatch.setitem(sys.modules, "fdb", ModuleType("fdb"))
    monkeypatch.delitem(sys.modules, "demo.run_demo", raising=False)
    return importlib.import_module("demo.run_demo")


def _thread(alive: bool) -> threading.Thread:
    thread = create_autospec(threading.Thread, instance=True)
    thread.is_alive.return_value = alive
    return thread


def test_stop_failure_is_logged_not_raised(
    run_demo: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    bridge = create_autospec(Bridge, instance=True)
    bridge.stop.side_effect = RuntimeError("close failed")

    run_demo._stop_bridge(bridge, _thread(alive=False))

    bridge.stop.assert_called_once_with()
    assert "close failed" in capsys.readouterr().err


def test_stuck_thread_skips_stop(run_demo: ModuleType) -> None:
    bridge = create_autospec(Bridge, instance=True)

    run_demo._stop_bridge(bridge, _thread(alive=True))

    bridge.stop.assert_not_called()


def test_stops_bridge_without_thread(run_demo: ModuleType) -> None:
    bridge = create_autospec(Bridge, instance=True)

    run_demo._stop_bridge(bridge, None)

    bridge.stop.assert_called_once_with()


def test_no_bridge_is_a_no_op(run_demo: ModuleType) -> None:
    run_demo._stop_bridge(None, None)
