"""Lease environments: the in-memory fake and, when enabled, live FoundationDB."""

import os
import signal
import sys
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from importlib import import_module
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from fdb_kafka_bridge.cdc.checkpoint import CheckpointStore, SinkIdentity
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.lease import Leases
from tests.cdc import fake_fdb

LIVE = os.environ.get("FDB_CDC_INTEGRATION") == "1"
LIVE_TEST_TIMEOUT_S = 30.0


def _live_timed_out(signum: int, frame: object) -> None:
    raise TimeoutError(f"live lease test exceeded {LIVE_TEST_TIMEOUT_S} s")


@dataclass
class LeaseEnv:
    kind: str
    fdb: ModuleType
    clock: fake_fdb.FakeClock
    store: CheckpointStore
    monkeypatch: pytest.MonkeyPatch

    @property
    def db(self) -> Any:
        return self.store.client.db

    def leases(self, pod_name: str = "pod-a", **kwargs: Any) -> Leases:
        return Leases(self.store, pod_name=pod_name, clock=self.clock, **kwargs)

    def get(self, key: bytes) -> bytes | None:
        tr = self.db.create_transaction()
        try:
            return tr.get(key).value
        finally:
            tr.cancel()

    def put(self, key: bytes, value: bytes) -> None:
        self.db[key] = value

    def race(self, action: Callable[[], None]) -> None:
        """Run ``action`` just before the next transaction's first commit."""
        db = self.db
        create = db.create_transaction

        def racing() -> Any:
            self.monkeypatch.setattr(db, "create_transaction", create)
            tr = create()
            commit = tr.commit
            pending = [action]

            def commit_after_race() -> Any:
                while pending:
                    pending.pop()()
                return commit()

            tr.commit = commit_after_race
            return tr

        self.monkeypatch.setattr(db, "create_transaction", racing)


def _fake(monkeypatch: pytest.MonkeyPatch) -> LeaseEnv:
    fdb, tuple_module = fake_fdb.module()
    monkeypatch.setitem(sys.modules, "fdb", fdb)
    monkeypatch.setitem(sys.modules, "fdb.tuple", tuple_module)
    clock = fake_fdb.FakeClock()
    client = SimpleNamespace(db=fake_fdb.Database(clock))
    store = CheckpointStore(client, sink=SinkIdentity("cluster", "topic"))  # type: ignore[arg-type]  # duck-typed client: only .db is used
    store.open()
    return LeaseEnv("fake", fdb, clock, store, monkeypatch)


@pytest.fixture
def fake_env(monkeypatch: pytest.MonkeyPatch) -> LeaseEnv:
    return _fake(monkeypatch)


@pytest.fixture(
    params=[
        "fake",
        pytest.param(
            "live",
            marks=[
                pytest.mark.integration,
                pytest.mark.skipif(not LIVE, reason="requires live FDB"),
            ],
        ),
    ]
)
def lease_env(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[LeaseEnv]:
    if request.param == "fake":
        yield _fake(monkeypatch)
        return
    client = FDBClient()
    client.open()
    store = CheckpointStore(
        client,
        sink=SinkIdentity("cluster", "topic"),
        path=("bridge-tests", uuid.uuid4().hex),
    )
    store.open()
    # FakeClock never advances on its own, so a real-FDB retry loop bounded only by
    # the fake clock would spin forever; fail the test on wall-clock time instead.
    previous = signal.signal(signal.SIGALRM, _live_timed_out)
    signal.setitimer(signal.ITIMER_REAL, LIVE_TEST_TIMEOUT_S)
    try:
        yield LeaseEnv(
            "live", import_module("fdb"), fake_fdb.FakeClock(), store, monkeypatch
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        begin, end = store.key_range()
        del client.db[begin:end]
        client.close()
