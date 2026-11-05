"""Unit tests for the v0.1.0 benchmark harness helpers (no FDB/Kafka)."""

from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path
from types import ModuleType

import pytest

_HARNESS = (
    Path(__file__).resolve().parents[2] / "benchmarks" / "0.1.0v" / "bench_e2e.py"
)


@pytest.fixture(scope="module")
def bench() -> ModuleType:
    """Load ``bench_e2e.py`` by path; ``0.1.0v`` is not an importable package name."""
    spec = importlib.util.spec_from_file_location("bench_e2e_v010", _HARNESS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_percentile_nearest_rank(bench: ModuleType) -> None:
    assert bench.percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.0
    assert math.isnan(bench.percentile([], 50))


def test_slope_two_points(bench: ModuleType) -> None:
    assert bench.slope([0.0, 10.0], [0.0, 100.0]) == 10.0
    assert bench.slope([1.0], [1.0]) == 0.0


def test_value_at_is_step_lookup(bench: ModuleType) -> None:
    times = [1.0, 2.0, 3.0]
    values = [10.0, 20.0, 30.0]
    assert bench.value_at(times, values, 2.5) == 20.0
    assert bench.value_at(times, values, 0.5) == 0


def test_append_step_sample_keeps_series_aligned(bench: ModuleType) -> None:
    step_t: list[float] = []
    step_acked: list[float] = []
    step_bytes: list[float] = []
    bench.append_step_sample(
        step_t, step_acked, step_bytes, t=1.0, acked=5, wire_bytes=100
    )
    bench.append_step_sample(
        step_t, step_acked, step_bytes, t=2.0, acked=5, wire_bytes=100
    )
    assert len(step_t) == len(step_acked) == len(step_bytes) == 2
    assert bench.value_at(step_t, step_bytes, 2.0) == 100


def test_exit_code_ok(bench: ModuleType) -> None:
    assert bench.exit_code_for_results({"backlog_growing": False, "drained": True}) == 0


def test_exit_code_growing_backlog(bench: ModuleType) -> None:
    assert bench.exit_code_for_results({"backlog_growing": True, "drained": True}) == 1


def test_exit_code_not_drained(bench: ModuleType) -> None:
    results = {"backlog_growing": False, "drained": False}
    assert bench.exit_code_for_results(results) == 1


def test_exit_code_integrity_failure(bench: ModuleType) -> None:
    results = {
        "backlog_growing": False,
        "drained": True,
        "integrity": {
            "duplicates": 1,
            "missing": 0,
            "order_violations": 0,
        },
    }
    assert bench.exit_code_for_results(results) == 1


def test_exit_code_cursor_mismatch(bench: ModuleType) -> None:
    results = {
        "backlog_growing": False,
        "drained": True,
        "cursor_matches_listener": False,
    }
    assert bench.exit_code_for_results(results) == 1


def test_exit_code_cursor_covers_writer_mismatch(bench: ModuleType) -> None:
    results = {
        "backlog_growing": False,
        "drained": True,
        "cursor_covers_writer": False,
    }
    assert bench.exit_code_for_results(results) == 1


def test_exit_code_missing_keys(bench: ModuleType) -> None:
    results = {
        "backlog_growing": False,
        "drained": True,
        "integrity": {
            "duplicates": 0,
            "missing": 5,
            "order_violations": 0,
        },
    }
    assert bench.exit_code_for_results(results) == 1


def test_exit_code_order_violations(bench: ModuleType) -> None:
    results = {
        "backlog_growing": False,
        "drained": True,
        "integrity": {
            "duplicates": 0,
            "missing": 0,
            "order_violations": 2,
        },
    }
    assert bench.exit_code_for_results(results) == 1


def test_verify_requires_unbounded_key_pool(
    bench: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit):
        bench.parse_args(["--verify", "--key-pool", "500000"])
    captured = capsys.readouterr()
    assert "--key-pool 0" in captured.err


def test_verify_rejected_with_use_bridge(
    bench: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit):
        bench.parse_args(["--use-bridge", "--verify", "--key-pool", "0"])
    captured = capsys.readouterr()
    assert "--verify is not supported with --use-bridge" in captured.err


def test_stop_writer_raises_if_error_set(bench: ModuleType) -> None:
    class FakeErrorWriter:
        def __init__(self) -> None:
            self.stop_event = type("E", (), {"set": lambda self: None})()
            self.error = RuntimeError("FDB commit failed")

        def join(self, timeout: float | None = None) -> None:
            return None

        def is_alive(self) -> bool:
            return False

    expected_err = r"writer failed: RuntimeError\('FDB commit failed'\)"
    with pytest.raises(RuntimeError, match=expected_err):
        bench.stop_writer(FakeErrorWriter(), timeout=0.01)


def test_stop_writer_raises_if_thread_alive(
    bench: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeWriter:
        def __init__(self) -> None:
            self.stop_event = type("E", (), {"set": lambda self: None})()
            self.error = None

        def join(self, timeout: float | None = None) -> None:
            return None

        def is_alive(self) -> bool:
            return True

    with pytest.raises(TimeoutError, match="writer did not stop"):
        bench.stop_writer(FakeWriter(), timeout=0.01)
