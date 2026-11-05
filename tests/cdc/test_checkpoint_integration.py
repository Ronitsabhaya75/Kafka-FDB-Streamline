"""Checkpoint durability and optimistic guards against a real FoundationDB."""

import os
import uuid
from collections.abc import Iterator
from importlib import import_module
from typing import Any

import pytest

from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointCursor,
    CheckpointError,
    CheckpointFormatError,
    CheckpointOverlapError,
    CheckpointRegressionError,
    CheckpointStore,
    CheckpointStreamMismatchError,
    CheckpointWriteError,
    SinkIdentity,
)
from fdb_kafka_bridge.cdc.client import FDBClient

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1", reason="requires live FDB"
    ),
]


@pytest.fixture
def store() -> Iterator[CheckpointStore]:
    client = FDBClient()
    client.open()
    store = CheckpointStore(
        client,
        sink=SinkIdentity("cluster", "topic"),
        path=("bridge-tests", uuid.uuid4().hex),
    )
    try:
        yield store
    finally:
        import_module("fdb").directory.remove_if_exists(client.db, store.path)
        client.close()


def test_live_store_guards_and_restart(store: CheckpointStore) -> None:
    store.open(create=False)
    assert store.load(b"s") is None
    with pytest.raises(CheckpointError):
        store.key_range()
    store.open()
    assert store.load(b"s") is None
    initial = CheckpointCursor(2**64 - 1, 10)
    store.save(b"s", initial)
    store.save(b"s", initial)
    reopened = CheckpointStore(store.client, sink=store.sink, path=store.path)
    reopened.open(create=False)
    assert reopened.load(b"s") == initial
    store.save(b"s", CheckpointCursor(initial.stream_id, 11))
    with pytest.raises(CheckpointRegressionError):
        reopened.save(b"s", initial)
    with pytest.raises(CheckpointStreamMismatchError):
        store.save(b"s", CheckpointCursor(1, 12))
    assert store.load(b"s") == CheckpointCursor(initial.stream_id, 11)
    store.delete(b"s")
    assert store.load(b"s") is None


@pytest.mark.parametrize("value", [b"bad", b"", b"\x15\x02", b"\x00"])
def test_corrupt_value_rejected(store: CheckpointStore, value: bytes) -> None:
    store.open()
    key = store._directory.pack((b"s",))
    store.client.db[key] = value
    with pytest.raises(CheckpointFormatError):
        store.load(b"s")


def test_overlap_rolls_back_directory_creation(store: CheckpointStore) -> None:
    directory = import_module("fdb").directory
    with pytest.raises(CheckpointOverlapError):
        store.open(forbidden_ranges=[(b"", b"\xfe")])
    assert not directory.exists(store.client.db, store.path)
    with pytest.raises(CheckpointOverlapError):
        store.open(forbidden_ranges=[(b"\xfe", b"\xff")])
    assert not directory.exists(store.client.db, store.path)
    store.open()
    with pytest.raises(CheckpointOverlapError):
        store.open(create=False, forbidden_ranges=[store.key_range()])


def test_transaction_timeout_is_wrapped(store: CheckpointStore) -> None:
    def expired(tr: Any) -> None:
        tr.options.set_timeout(1)
        import time

        time.sleep(0.01)
        tr.get(b"bridge-timeout-test").wait()

    with pytest.raises(CheckpointWriteError):
        store._run(expired)


def test_retry_conflict_and_exhaustion(store: CheckpointStore) -> None:
    fdb = import_module("fdb")
    calls = 0

    def conflict_once(tr: Any) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise fdb.FDBError(1020)
        return "success"

    assert store._run(conflict_once) == "success"
    assert calls == 2
    store.retry_limit = 0

    def conflict(tr: Any) -> None:
        raise fdb.FDBError(1020)

    with pytest.raises(CheckpointWriteError, match="retries exhausted"):
        store._run(conflict)


@pytest.mark.parametrize(
    "sink",
    [SinkIdentity("other-cluster", "topic"), SinkIdentity("cluster", "other-topic")],
)
def test_sink_identity_blocks_load_and_save(
    store: CheckpointStore, sink: SinkIdentity
) -> None:
    from fdb_kafka_bridge.cdc.checkpoint import CheckpointSinkMismatchError

    store.open()
    cursor = CheckpointCursor(1, 10)
    store.save(b"s", cursor)
    other = CheckpointStore(store.client, sink=sink, path=store.path)
    other.open(create=False)
    with pytest.raises(CheckpointSinkMismatchError):
        other.load(b"s")
    with pytest.raises(CheckpointSinkMismatchError):
        other.save(b"s", CheckpointCursor(1, 11))
    assert store.load(b"s") == cursor
