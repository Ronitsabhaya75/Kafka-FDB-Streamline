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
        store.open()
        begin, end = store.key_range()
        del client.db[begin:end]
        client.close()


def test_live_store_guards_and_restart(store: CheckpointStore) -> None:
    store.open(create=False)
    assert store.load(b"s") is None
    assert store.key_range()
    store.claim(b"s")
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
        store.save(b"s", initial)
    with pytest.raises(CheckpointStreamMismatchError):
        store.save(b"s", CheckpointCursor(1, 12))
    assert store.load(b"s") == CheckpointCursor(initial.stream_id, 11)
    store.delete(b"s")
    assert store.load(b"s") is None


@pytest.mark.parametrize("value", [b"bad", b"", b"\x15\x02", b"\x00"])
def test_corrupt_value_rejected(store: CheckpointStore, value: bytes) -> None:
    store.open()
    key = store.subspace.pack((b"s",))
    store.client.db[key] = value
    with pytest.raises(CheckpointFormatError):
        store.load(b"s")


def test_overlap_refuses_without_allocation(store: CheckpointStore) -> None:
    with pytest.raises(CheckpointOverlapError):
        store.open(forbidden_ranges=[(b"", b"\xfe")])
    store.open(forbidden_ranges=[(b"\xfe", b"\xff")])
    store.open()
    with pytest.raises(CheckpointOverlapError):
        store.open(create=False, forbidden_ranges=[store.key_range()])


def test_transaction_timeout_is_wrapped(store: CheckpointStore) -> None:
    def expired(tr: Any) -> None:
        tr.options.set_timeout(1)
        import time

        time.sleep(0.01)
        tr.get(b"bridge-timeout-test").wait()

    with pytest.raises(CheckpointError) as error:
        store.run_transaction(expired)
    assert not isinstance(error.value, CheckpointWriteError)


def test_retry_conflict_and_exhaustion(store: CheckpointStore) -> None:
    fdb = import_module("fdb")
    calls = 0

    def conflict_once(tr: Any) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise fdb.FDBError(1020)
        return "success"

    assert store.run_transaction(conflict_once) == "success"
    assert calls == 2
    store.retry_limit = 0

    def conflict(tr: Any) -> None:
        raise fdb.FDBError(1020)

    with pytest.raises(CheckpointWriteError, match="retries exhausted"):
        store.run_transaction(conflict)


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
    store.claim(b"s")
    store.save(b"s", cursor)
    other = CheckpointStore(store.client, sink=sink, path=store.path)
    other.open(create=False)
    with pytest.raises(CheckpointSinkMismatchError):
        other.load(b"s")
    with pytest.raises(CheckpointSinkMismatchError):
        other.save(b"s", CheckpointCursor(1, 11))
    assert store.load(b"s") == cursor


def test_takeover_fences_equal_save_load_and_rebuild(store: CheckpointStore) -> None:
    from fdb_kafka_bridge.cdc.checkpoint import CheckpointFencedError

    store.open()
    with pytest.raises(CheckpointFencedError, match="Claim"):
        store.save(b"s", CheckpointCursor(1, 10))
    store.claim(b"s")
    store.save(b"s", CheckpointCursor(1, 10))
    store.claim(b"s")
    replacement = CheckpointStore(store.client, sink=store.sink, path=store.path)
    replacement.open()
    replacement.claim(b"s")
    assert replacement.load(b"s") == CheckpointCursor(1, 10)
    for action in (
        lambda: store.save(b"s", CheckpointCursor(1, 10)),
        lambda: store.save(b"s", CheckpointCursor(1, 11)),
        lambda: store.load(b"s"),
        lambda: store.claim(b"s"),
        lambda: store.verify_owner(b"s"),
    ):
        with pytest.raises(CheckpointFencedError):
            action()
    replacement.save(b"s", CheckpointCursor(1, 11))
    assert replacement.load(b"s") == CheckpointCursor(1, 11)


def test_removed_owner_is_fatal(store: CheckpointStore) -> None:
    from fdb_kafka_bridge.cdc.checkpoint import CheckpointFencedError

    store.open()
    store.claim(b"s")
    del store.client.db[store.subspace.pack((b"s", "owner"))]
    with pytest.raises(CheckpointFencedError):
        store.save(b"s", CheckpointCursor(1, 10))


@pytest.mark.parametrize("value", [b"bad", b"", b"\x15\x01", b"\x00"])
def test_corrupt_owner_is_fatal(store: CheckpointStore, value: bytes) -> None:
    store.open()
    store.client.db[store.subspace.pack((b"s", "owner"))] = value
    with pytest.raises(CheckpointFormatError, match="owner"):
        store.claim(b"s")


def test_unopened_subspace_is_rejected(store: CheckpointStore) -> None:
    with pytest.raises(CheckpointError, match="not open"):
        store.key_range()


def test_claim_reconciles_commit_unknown(
    store: CheckpointStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import MagicMock

    fdb = import_module("fdb")
    create = store.client.db.create_transaction
    first = True

    def transaction() -> Any:
        tr = create()
        commit = tr.commit

        def uncertain_commit() -> Any:
            future = commit()

            def wait() -> None:
                nonlocal first
                future.wait()
                if first:
                    first = False
                    raise fdb.FDBError(1021)

            return MagicMock(wait=wait)

        tr.commit = uncertain_commit
        return tr

    store.open()
    monkeypatch.setattr(store.client.db, "create_transaction", transaction)
    store.claim(b"s")
    assert store._owners[b"s"][0] == 1
    store.verify_owner(b"s")


def test_save_conflicts_with_owner_takeover(store: CheckpointStore) -> None:
    fdb = import_module("fdb")
    store.open()
    store.claim(b"s")
    store.save(b"s", CheckpointCursor(1, 10))
    tr = store.client.db.create_transaction()
    store._check_owner(tr, b"s")
    replacement = CheckpointStore(store.client, sink=store.sink, path=store.path)
    replacement.open()
    replacement.claim(b"s")
    tr[store.subspace.pack((b"s",))] = fdb.tuple.pack((1, 1, 11, "cluster", "topic"))
    with pytest.raises(fdb.FDBError) as error:
        tr.commit().wait()
    assert error.value.code == 1020


@pytest.mark.parametrize("code", [1031, 2004])
def test_nonretryable_native_error_is_fatal(store: CheckpointStore, code: int) -> None:
    fdb = import_module("fdb")
    calls = 0

    def fail(tr: Any) -> None:
        nonlocal calls
        calls += 1
        raise fdb.FDBError(code)

    with pytest.raises(CheckpointError) as error:
        store.run_transaction(fail)
    assert not isinstance(error.value, CheckpointWriteError)
    assert calls == 1


@pytest.mark.parametrize("code", [1031, 1020])
def test_error_during_backoff_is_classified(
    store: CheckpointStore, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    from unittest.mock import MagicMock

    fdb = import_module("fdb")
    tr = store.client.db.create_transaction()
    tr.on_error = MagicMock(
        return_value=MagicMock(wait=MagicMock(side_effect=fdb.FDBError(code)))
    )
    monkeypatch.setattr(store.client.db, "create_transaction", lambda: tr)

    def conflict(tr: Any) -> None:
        raise fdb.FDBError(1020)

    with pytest.raises(CheckpointError) as error:
        store.run_transaction(conflict)
    assert isinstance(error.value, CheckpointWriteError) is (code == 1020)
