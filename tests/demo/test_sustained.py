"""Unit tests for sustained-demo helpers and CLI wiring (no FDB/Kafka)."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest

from demo.sustained import (
    SustainedStats,
    caught_up,
    default_max_backlog,
    mutation_backlog,
)


@pytest.fixture
def run_demo(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setitem(sys.modules, "fdb", ModuleType("fdb"))
    monkeypatch.delitem(sys.modules, "demo.run_demo", raising=False)
    return importlib.import_module("demo.run_demo")


def test_default_max_backlog_scales_with_rate() -> None:
    assert default_max_backlog(500) == 2500
    assert default_max_backlog(50) == 1000  # floor
    assert default_max_backlog(0) == 1000


def test_mutation_backlog_is_non_negative() -> None:
    assert mutation_backlog(written=10, published=3) == 7
    assert mutation_backlog(written=3, published=10) == 0


def test_caught_up_requires_progress() -> None:
    assert caught_up(written=0, published=0) is False
    assert caught_up(written=5, published=4) is False
    assert caught_up(written=5, published=5) is True
    assert caught_up(written=5, published=6) is True


def test_sustained_stats_tracks_peak_backlog() -> None:
    stats = SustainedStats()
    assert stats.sample_backlog(written=100, published=40) == 60
    assert stats.sample_backlog(written=120, published=90) == 30
    assert stats.max_backlog == 60


def test_arguments_sustained_defaults(run_demo: ModuleType) -> None:
    args = run_demo._arguments(
        ["--mode", "sustained", "--duration", "12", "--target-rate", "100"]
    )
    assert args.mode == "sustained"
    assert args.duration == 12.0
    assert args.target_rate == 100.0
    assert args.seed == run_demo.DEFAULT_SEED
    assert args.catchup_timeout == run_demo.DEFAULT_CATCHUP_TIMEOUT_SECONDS
    assert args.count is None
    assert args.max_backlog is None


def test_arguments_batch_count_default_none(run_demo: ModuleType) -> None:
    args = run_demo._arguments([])
    assert args.mode == "batch"
    assert args.count is None


def test_record_mutation_count_ignores_version_ends(run_demo: ModuleType) -> None:
    from fdbkafka.cdc.v1 import mutations_pb2

    batch = mutations_pb2.FDBMutationRecord(
        batch=mutations_pb2.FDBMutationBatch(
            mutations=[
                mutations_pb2.FDBMutation(
                    version_index=mutations_pb2.FDBVersionIndex(
                        fdb_version=1, sequence_no=0
                    ),
                    single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
                        key=b"k", value=b"v", mutation_type=0
                    ),
                )
            ]
        )
    )
    end = mutations_pb2.FDBMutationRecord(
        version_end=mutations_pb2.VersionEnd(fdb_version=1, total_mutations=1)
    )
    assert run_demo._record_mutation_count([batch, end]) == 1
