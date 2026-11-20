"""Claim, renew, release and watch outcomes; identical on the fake and live FDB."""

from fdb_kafka_bridge.cdc.lease import (
    POD_GC_STALE_S,
    STALE_S,
    Lease,
    LeaseValue,
    OwnerRecord,
    Watcher,
    decode_owner,
    encode_owner,
)
from tests.cdc.conftest import LeaseEnv

S = b"stream"


def owner(env: LeaseEnv, stream: bytes = S) -> OwnerRecord | None:
    return decode_owner(env.get(env.store.subspace.pack((stream, "owner"))))


def heartbeat(env: LeaseEnv, stream: bytes = S) -> bytes | None:
    return env.get(env.store.subspace.pack((stream, "hb")))


def test_claim_unowned_stream_writes_v2_and_leaves_heartbeat(
    lease_env: LeaseEnv,
) -> None:
    claimed = lease_env.leases("pod-a").claim(S)
    assert claimed is not None
    lease = claimed.lease
    assert lease.stream_name == S and lease.epoch == 1 and len(lease.owner_id) == 16
    assert claimed.started == lease_env.clock.monotonic()
    assert owner(lease_env) == OwnerRecord(2, 1, lease.owner_id, "pod-a")
    assert heartbeat(lease_env) is None


def test_held_lease_is_not_claimed_without_stale_value(lease_env: LeaseEnv) -> None:
    first = lease_env.leases("pod-a").claim(S)
    assert first is not None
    assert lease_env.leases("pod-b").claim(S) is None
    assert owner(lease_env).owner_id == first.lease.owner_id


def test_claim_of_stale_value_raises_epoch(lease_env: LeaseEnv) -> None:
    first = lease_env.leases("pod-a").claim(S)
    assert first is not None
    seen = LeaseValue(encode_owner(1, first.lease.owner_id, "pod-a"), 0)
    second = lease_env.leases("pod-b").claim(S, seen)
    assert second is not None
    assert second.lease.epoch == 2
    assert second.lease.owner_id != first.lease.owner_id
    assert owner(lease_env) == OwnerRecord(2, 2, second.lease.owner_id, "pod-b")


def test_claim_of_outdated_value_is_refused(lease_env: LeaseEnv) -> None:
    first = lease_env.leases("pod-a").claim(S)
    assert first is not None
    outdated = LeaseValue(encode_owner(1, first.lease.owner_id, "pod-a"), 7)
    assert lease_env.leases("pod-b").claim(S, outdated) is None


def test_v1_record_is_claimed_as_v2_with_next_epoch(lease_env: LeaseEnv) -> None:
    key = lease_env.store.subspace.pack((S, "owner"))
    v1 = lease_env.fdb.tuple.pack((1, 4, bytes(16)))
    lease_env.put(key, v1)
    assert lease_env.leases().claim(S) is None
    claimed = lease_env.leases().claim(S, LeaseValue(v1, 0))
    assert claimed is not None and claimed.lease.epoch == 5
    assert owner(lease_env).version == 2


def test_unreadable_record_is_never_claimed(lease_env: LeaseEnv) -> None:
    key = lease_env.store.subspace.pack((S, "owner"))
    lease_env.put(key, b"bad")
    assert lease_env.leases().claim(S, LeaseValue(b"bad", 0)) is None
    assert lease_env.get(key) == b"bad"


def test_claim_racing_a_renew_aborts(lease_env: LeaseEnv) -> None:
    leader = lease_env.leases("pod-a")
    first = leader.claim(S)
    assert first is not None
    seen = LeaseValue(encode_owner(1, first.lease.owner_id, "pod-a"), 0)
    lease_env.race(lambda: leader.renew({first.lease: first.started}))
    assert lease_env.leases("pod-b").claim(S, seen) is None
    assert owner(lease_env).owner_id == first.lease.owner_id


def test_racing_claimers_have_one_winner(lease_env: LeaseEnv) -> None:
    winner: list[Lease] = []

    def rival() -> None:
        claimed = lease_env.leases("pod-b").claim(S)
        assert claimed is not None
        winner.append(claimed.lease)

    lease_env.race(rival)
    assert lease_env.leases("pod-a").claim(S) is None
    assert owner(lease_env).owner_id == winner[0].owner_id


def pod_heartbeat(env: LeaseEnv, instance_id: bytes) -> bytes | None:
    return env.get(env.store.subspace.pack(("pod", instance_id, "hb")))


def test_renew_bumps_held_heartbeats_and_pod_heartbeat(lease_env: LeaseEnv) -> None:
    leases = lease_env.leases("pod-a")
    a = leases.claim(b"a")
    b = leases.claim(b"b")
    assert a is not None and b is not None
    lease_env.clock.advance(0.2)
    result = leases.renew({a.lease: a.started, b.lease: b.started})
    assert result.started == lease_env.clock.monotonic()
    assert result.renewed == {a.lease, b.lease}
    assert not result.lost and not result.expired
    leases.renew({a.lease: result.started})
    assert heartbeat(lease_env, b"a") == (2).to_bytes(8, "little")
    assert heartbeat(lease_env, b"b") == (1).to_bytes(8, "little")
    assert pod_heartbeat(lease_env, leases.instance_id) == (2).to_bytes(8, "little")


def test_renew_with_no_leases_bumps_pod_heartbeat(lease_env: LeaseEnv) -> None:
    leases = lease_env.leases()
    result = leases.renew({})
    assert result.started is not None and not result.renewed
    assert pod_heartbeat(lease_env, leases.instance_id) == (1).to_bytes(8, "little")


def test_renew_reports_lost_tenures(lease_env: LeaseEnv) -> None:
    leader = lease_env.leases("pod-a")
    claimed = leader.claim(S)
    assert claimed is not None
    stale = LeaseValue(encode_owner(1, claimed.lease.owner_id, "pod-a"), 0)
    assert lease_env.leases("pod-b").claim(S, stale) is not None
    missing = Lease(b"missing", 1, bytes(16))
    result = leader.renew({claimed.lease: claimed.started, missing: claimed.started})
    assert result.started is not None
    assert result.lost == {claimed.lease, missing}
    assert not result.renewed
    assert heartbeat(lease_env) is None


def test_renew_is_not_aborted_by_a_racing_claim(lease_env: LeaseEnv) -> None:
    leader = lease_env.leases("pod-a")
    claimed = leader.claim(S)
    assert claimed is not None
    stale = LeaseValue(encode_owner(1, claimed.lease.owner_id, "pod-a"), 0)
    taken: list[object] = []
    lease_env.race(lambda: taken.append(lease_env.leases("pod-b").claim(S, stale)))
    first = leader.renew({claimed.lease: claimed.started})
    assert taken[0] is not None
    # The loss surfaces one tick late; the save fence covers that tick.
    assert first.renewed == {claimed.lease}
    second = leader.renew({claimed.lease: first.started})
    assert second.lost == {claimed.lease}


def test_release_clears_owner_and_keeps_epoch(lease_env: LeaseEnv) -> None:
    leases = lease_env.leases("pod-a")
    claimed = leases.claim(S)
    assert claimed is not None
    assert leases.release(claimed.lease)
    assert owner(lease_env) == OwnerRecord(2, 1, None, "pod-a")
    assert leases.release(claimed.lease)
    again = lease_env.leases("pod-b").claim(S)
    assert again is not None and again.lease.epoch == 2


def test_release_of_lost_tenure_changes_nothing(lease_env: LeaseEnv) -> None:
    leader = lease_env.leases("pod-a")
    claimed = leader.claim(S)
    assert claimed is not None
    stale = LeaseValue(encode_owner(1, claimed.lease.owner_id, "pod-a"), 0)
    taker = lease_env.leases("pod-b").claim(S, stale)
    assert taker is not None
    assert not leader.release(claimed.lease)
    assert owner(lease_env).owner_id == taker.lease.owner_id
    assert not leader.release(Lease(b"missing", 1, bytes(16)))


def test_release_racing_a_claim_does_not_release(lease_env: LeaseEnv) -> None:
    leader = lease_env.leases("pod-a")
    claimed = leader.claim(S)
    assert claimed is not None
    stale = LeaseValue(encode_owner(1, claimed.lease.owner_id, "pod-a"), 0)
    lease_env.race(lambda: lease_env.leases("pod-b").claim(S, stale))
    assert not leader.release(claimed.lease)
    assert owner(lease_env).epoch == 2


def test_watch_unclaimed_and_released_streams_are_claimable_at_once(
    lease_env: LeaseEnv,
) -> None:
    leases = lease_env.leases()
    claimed = leases.claim(b"released")
    assert claimed is not None and leases.release(claimed.lease)
    watcher = Watcher(leases, [b"never", b"released"])
    assert watcher.poll()
    released = lease_env.get(lease_env.store.subspace.pack((b"released", "owner")))
    assert watcher.claimable() == {
        b"never": LeaseValue(None, 0),
        b"released": LeaseValue(released, 0),
    }


def test_watch_held_lease_goes_stale_only_while_unchanged(lease_env: LeaseEnv) -> None:
    leader = lease_env.leases("pod-a")
    claimed = leader.claim(S)
    assert claimed is not None
    watcher = Watcher(lease_env.leases("pod-b"), [S])
    assert watcher.poll() and watcher.claimable() == {}
    lease_env.clock.advance(STALE_S / 2)
    leader.renew({claimed.lease: claimed.started})
    lease_env.clock.advance(STALE_S / 2)
    assert watcher.poll() and watcher.claimable() == {}
    lease_env.clock.advance(STALE_S * 3 / 4)
    assert watcher.poll() and watcher.claimable() == {}
    lease_env.clock.advance(STALE_S / 4)
    assert watcher.poll()
    seen = watcher.claimable()[S]
    assert seen.heartbeat == 1
    taken = lease_env.leases("pod-b").claim(S, seen)
    assert taken is not None and taken.lease.epoch == 2


def test_watch_v1_record_goes_stale(lease_env: LeaseEnv) -> None:
    v1 = lease_env.fdb.tuple.pack((1, 4, bytes(16)))
    lease_env.put(lease_env.store.subspace.pack((S, "owner")), v1)
    watcher = Watcher(lease_env.leases(), [S])
    assert watcher.poll() and watcher.claimable() == {}
    lease_env.clock.advance(STALE_S)
    assert watcher.poll() and watcher.claimable() == {S: LeaseValue(v1, 0)}


def test_watch_blocks_unreadable_records_and_ignores_foreign_keys(
    lease_env: LeaseEnv,
) -> None:
    sub = lease_env.store.subspace
    lease_env.put(sub.pack((S, "owner")), b"bad")
    lease_env.put(sub.pack((b"other", "owner")), b"bad")
    lease_env.put(sub.pack((S,)), b"cursor")
    lease_env.put(sub.key() + b"\xfe", b"garbage key")
    watcher = Watcher(lease_env.leases(), [S, b"fine"])
    assert watcher.poll()
    lease_env.clock.advance(STALE_S)
    assert watcher.poll()
    assert watcher.claimable() == {b"fine": LeaseValue(None, 0)}
    assert watcher.unreadable() == {S}


def test_pod_keys_register_age_and_collect(lease_env: LeaseEnv) -> None:
    sub = lease_env.store.subspace
    live = lease_env.leases("pod-live")
    dead = lease_env.leases("pod-dead")
    for pod in (live, dead):
        pod.register_pod()
    live.renew({})
    assert lease_env.get(sub.pack(("pod", dead.instance_id, "name"))) == b"pod-dead"
    watcher = Watcher(live, [])
    assert watcher.poll() and watcher.stale_pods() == []
    lease_env.clock.advance(STALE_S + POD_GC_STALE_S)
    live.renew({})
    assert watcher.poll() and watcher.stale_pods() == []
    lease_env.clock.advance(0.01)
    assert watcher.poll() and watcher.stale_pods() == [dead.instance_id]
    live.gc_pods(watcher.stale_pods())
    live.gc_pods(watcher.stale_pods())
    assert lease_env.get(sub.pack(("pod", dead.instance_id, "hb"))) is None
    assert lease_env.get(sub.pack(("pod", dead.instance_id, "name"))) is None
    assert watcher.poll() and watcher.stale_pods() == []
    live.clear_pod()
    assert lease_env.get(sub.pack(("pod", live.instance_id, "hb"))) is None
    assert lease_env.get(sub.pack(("pod", live.instance_id, "name"))) is None
