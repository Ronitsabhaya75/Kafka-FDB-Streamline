"""Lease library logic over the fake store and fake clock (timing, retries, codec)."""

import threading

import pytest

from fdb_kafka_bridge.cdc.checkpoint import CheckpointFormatError
from fdb_kafka_bridge.cdc.lease import (
    POD_GC_STALE_S,
    SELF_DROP_S,
    STALE_S,
    OwnerRecord,
    RenewResult,
    SystemClock,
    Watcher,
    decode_owner,
    encode_owner,
)
from tests.cdc import fake_fdb
from tests.cdc.conftest import LeaseEnv

TOKEN = bytes(range(16))


def test_owner_record_v2_round_trips(fake_env: LeaseEnv) -> None:
    held = encode_owner(7, TOKEN, "pod-a")
    assert held == fake_env.fdb.tuple.pack((2, 7, TOKEN, "pod-a"))
    assert decode_owner(held) == OwnerRecord(2, 7, TOKEN, "pod-a")
    assert decode_owner(encode_owner(7, None, "pod-a")) == OwnerRecord(
        2, 7, None, "pod-a"
    )
    assert decode_owner(None) is None


def test_v1_record_reads_as_held(fake_env: LeaseEnv) -> None:
    v1 = fake_env.fdb.tuple.pack((1, 3, TOKEN))
    assert decode_owner(v1) == OwnerRecord(1, 3, TOKEN, None)


@pytest.mark.parametrize(
    "value",
    [
        b"bad",
        b"",
        b"\x00",
        (3, 1, TOKEN, "pod"),
        (2, 0, TOKEN, "pod"),
        (2, 1, b"short", "pod"),
        (2, 1, TOKEN, b"pod"),
        (2, 1, TOKEN),
        (1, 1, None),
        (1, 0, TOKEN),
        (4, TOKEN),
        ("x", TOKEN),
        (0, TOKEN),
        (1, b"short"),
        (2.0, 1, TOKEN, "pod"),
        (True, 1, TOKEN),
        (1.0, 1, TOKEN),
    ],
)
def test_unreadable_owner_records(fake_env: LeaseEnv, value: bytes | tuple) -> None:
    raw = fake_env.fdb.tuple.pack(value) if isinstance(value, tuple) else value
    with pytest.raises(CheckpointFormatError):
        decode_owner(raw)


def test_fake_tuple_matches_known_bytes() -> None:
    assert fake_fdb.pack((b"s\x00", "owner", 0, 1, 256, -1, None)) == (
        b"\x01s\x00\xff\x00\x02owner\x00\x14\x15\x01\x16\x01\x00\x13\xfe\x00"
    )
    assert fake_fdb.unpack(fake_fdb.pack((b"s\x00", "owner", 300, -300, None))) == (
        b"s\x00",
        "owner",
        300,
        -300,
        None,
    )
    assert fake_fdb.pack((True, False, 2.0, -2.0)) == (
        b"\x27\x26\x21\xc0" + bytes(7) + b"\x21\x3f" + b"\xff" * 7
    )
    assert fake_fdb.unpack(fake_fdb.pack((True, False, 2.0, -2.0))) == (
        True,
        False,
        2.0,
        -2.0,
    )


S = b"stream"


def fail(
    env: LeaseEnv,
    op: str,
    code: int,
    times: int = 1,
    advance: float = 0.0,
) -> list[int]:
    """Raise ``code`` at the next ``times`` occurrences of ``op``."""
    hits: list[int] = []

    def on_op(name: str, tr: fake_fdb.Transaction) -> None:
        if name == op and len(hits) < times:
            hits.append(code)
            env.clock.advance(advance)
            raise fake_fdb.FDBError(code)

    env.db.on_op = on_op
    return hits


def test_transaction_priorities_and_timeouts(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    claim_tr = fake_env.db.transactions[-1]
    assert claim_tr.priorities == ["system_immediate"]
    assert (claim_tr.timeout_ms, claim_tr.retry_limit) == (1000, 5)
    fake_env.clock.advance(0.3)
    leases.renew({claimed.lease: claimed.started})
    renew_tr = fake_env.db.transactions[-1]
    assert renew_tr.priorities == ["system_immediate"]
    assert renew_tr.timeout_ms == 500  # to the lease's self-drop deadline
    Watcher(leases, [S]).poll()
    watch_tr = fake_env.db.transactions[-1]
    assert (watch_tr.priorities, watch_tr.timeout_ms) == (["system_immediate"], 200)
    assert leases.release(claimed.lease)
    release_tr = fake_env.db.transactions[-1]
    assert release_tr.priorities == ["default"]
    assert (release_tr.timeout_ms, release_tr.retry_limit) == (1000, 5)
    leases.gc_pods([b"x" * 16])
    assert fake_env.db.transactions[-1].priority == "batch"
    leases.clear_pod()
    assert fake_env.db.transactions[-1].priority == "default"


def test_claim_landed_under_commit_unknown_anchors_on_that_attempt(
    fake_env: LeaseEnv,
) -> None:
    started = fake_env.clock.monotonic()
    fail(fake_env, "committed", 1021, advance=0.1)
    claimed = fake_env.leases().claim(S)
    assert claimed is not None
    assert claimed.started == started
    assert claimed.lease.epoch == 1
    assert fake_env.db.transactions[-1].priorities == ["system_immediate"] * 2


def test_claim_not_landed_under_commit_unknown_anchors_on_retry(
    fake_env: LeaseEnv,
) -> None:
    # Failed attempt's 0.1 s, then the fake on_error's first 10 ms backoff.
    retry_start = fake_env.clock.monotonic() + 0.1 + 0.01
    fail(fake_env, "commit", 1021, advance=0.1)
    claimed = fake_env.leases().claim(S)
    assert claimed is not None
    assert claimed.started == retry_start


@pytest.mark.parametrize(
    ("code", "times", "advance"),
    [(1020, 6, 0.0), (1020, 1, 1.0), (2004, 1, 0.0)],
    ids=["retry-limit", "timeout", "permanent"],
)
def test_claim_failure_is_not_claimed(
    fake_env: LeaseEnv, code: int, times: int, advance: float
) -> None:
    hits = fail(fake_env, "commit", code, times, advance)
    assert fake_env.leases().claim(S) is None
    assert len(hits) == times
    assert fake_env.get(fake_env.store.subspace.pack((S, "owner"))) is None


def test_release_landed_under_commit_unknown_is_released(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    fail(fake_env, "committed", 1021)
    assert leases.release(claimed.lease)


def test_release_failure_reports_not_released(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    fail(fake_env, "commit", 1020, times=6)
    assert not leases.release(claimed.lease)


def test_release_of_unreadable_record_is_a_format_error(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    fake_env.put(fake_env.store.subspace.pack((S, "owner")), b"bad")
    with pytest.raises(CheckpointFormatError):
        leases.release(claimed.lease)
    assert fake_env.get(fake_env.store.subspace.pack((S, "owner"))) == b"bad"


def test_renew_retries_and_anchors_on_the_committed_attempt(
    fake_env: LeaseEnv,
) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    fail(fake_env, "commit", 2004, advance=0.1)
    result = leases.renew({claimed.lease: claimed.started})
    assert result.renewed == {claimed.lease}
    assert result.started == fake_env.clock.monotonic()
    assert result.started > claimed.started


def test_renew_attempt_runs_to_deadline_then_expires_the_lease(
    fake_env: LeaseEnv,
) -> None:
    leases = fake_env.leases()
    old = leases.claim(b"old")
    new = leases.claim(b"new")
    assert old is not None and new is not None
    fake_env.clock.advance(0.5)
    fail(fake_env, "get", 1020, advance=0.35)
    young = fake_env.clock.monotonic() - 0.1
    result = leases.renew({old.lease: old.started, new.lease: young})
    assert abs(fake_env.db.transactions[-2].timeout_ms - 300) <= 1
    assert result.expired == {old.lease}
    assert result.renewed == {new.lease}


def test_renew_already_expired_lease_is_not_renewed(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    fake_env.clock.advance(0.81)
    result = leases.renew({claimed.lease: claimed.started})
    assert result.expired == {claimed.lease}
    assert result.started is not None and not result.renewed
    assert fake_env.get(fake_env.store.subspace.pack((S, "hb"))) is None


def test_renew_that_never_commits_gives_up_at_the_deadline(
    fake_env: LeaseEnv,
) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    start = fake_env.clock.monotonic()
    fail(fake_env, "commit", 1020, times=1000)
    result = leases.renew({claimed.lease: claimed.started})
    assert result == RenewResult(
        None, frozenset(), frozenset(), frozenset(), frozenset({claimed.lease})
    )
    assert fake_env.clock.monotonic() >= start + SELF_DROP_S


def test_failed_watch_keeps_anchors_until_a_fresh_read(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None
    leases.register_pod()
    leases.renew({claimed.lease: claimed.started})
    watcher = Watcher(fake_env.leases("pod-b"), [S])
    assert watcher.poll()
    fake_env.clock.advance(STALE_S + POD_GC_STALE_S + 1)
    fail(fake_env, "get_range", 1031)
    assert not watcher.poll()
    assert watcher.claimable() == {}
    assert watcher.stale_pods() == []
    assert watcher.poll()
    assert S in watcher.claimable()
    assert watcher.stale_pods() == [leases.instance_id]


def test_non_bytes_stream_names_are_rejected(fake_env: LeaseEnv) -> None:
    leases = fake_env.leases()
    with pytest.raises(TypeError):
        leases.claim("stream")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Watcher(leases, ["stream"])  # type: ignore[list-item]


def test_system_clock() -> None:
    clock = SystemClock()
    before = clock.monotonic()
    clock.sleep(0)
    assert clock.monotonic() >= before
    event = threading.Event()
    assert not clock.wait(event, 0)
    event.set()
    assert clock.wait(event, 0)
