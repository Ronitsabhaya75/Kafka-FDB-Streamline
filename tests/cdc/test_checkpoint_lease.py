"""CheckpointStore fenced by an explicit Lease, and unchanged without one."""

import pytest

from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointCursor,
    CheckpointFencedError,
    CheckpointFormatError,
)
from fdb_kafka_bridge.cdc.lease import (
    Claimed,
    Lease,
    LeaseValue,
    decode_owner,
    encode_owner,
)
from tests.cdc.conftest import LeaseEnv

S = b"stream"


def test_lease_fences_save_load_and_verify(lease_env: LeaseEnv) -> None:
    store = lease_env.store
    claimed = lease_env.leases("pod-a").claim(S)
    assert claimed is not None
    lease = claimed.lease
    assert store.load(S, lease) is None
    store.save(S, CheckpointCursor(1, 10), lease)
    store.verify_owner(S, lease)
    assert store.load(S, lease) == CheckpointCursor(1, 10)
    # The save fence neither reads nor bumps the heartbeat.
    assert lease_env.get(store.subspace.pack((S, "hb"))) is None
    stale = LeaseValue(encode_owner(1, lease.owner_id, "pod-a"), 0)
    taker = lease_env.leases("pod-b").claim(S, stale)
    assert taker is not None
    for action in (
        lambda: store.save(S, CheckpointCursor(1, 11), lease),
        lambda: store.load(S, lease),
        lambda: store.verify_owner(S, lease),
    ):
        with pytest.raises(CheckpointFencedError):
            action()
    store.save(S, CheckpointCursor(1, 11), taker.lease)
    assert store.load(S, taker.lease) == CheckpointCursor(1, 11)


def test_released_or_missing_owner_fences(lease_env: LeaseEnv) -> None:
    store = lease_env.store
    leases = lease_env.leases()
    claimed = leases.claim(S)
    assert claimed is not None and leases.release(claimed.lease)
    with pytest.raises(CheckpointFencedError):
        store.verify_owner(S, claimed.lease)
    with pytest.raises(CheckpointFencedError):
        store.verify_owner(b"other", Lease(b"other", 1, claimed.lease.owner_id))


def test_save_loses_to_a_claim_racing_its_commit(lease_env: LeaseEnv) -> None:
    store = lease_env.store
    claimed = lease_env.leases("pod-a").claim(S)
    assert claimed is not None
    lease = claimed.lease
    store.save(S, CheckpointCursor(1, 10), lease)
    stale = LeaseValue(encode_owner(1, lease.owner_id, "pod-a"), 0)
    taken: list[Claimed | None] = []
    lease_env.race(lambda: taken.append(lease_env.leases("pod-b").claim(S, stale)))
    with pytest.raises(CheckpointFencedError):
        store.save(S, CheckpointCursor(1, 11), lease)
    [taker] = taken
    assert taker is not None
    assert store.load(S, taker.lease) == CheckpointCursor(1, 10)


def test_v1_record_never_satisfies_a_lease(lease_env: LeaseEnv) -> None:
    store = lease_env.store
    store.claim(S)
    record = decode_owner(lease_env.get(store.subspace.pack((S, "owner"))))
    assert record is not None and record.version == 1
    with pytest.raises(CheckpointFencedError):
        store.load(S, Lease(S, record.epoch, record.owner_id))


def test_unreadable_owner_is_a_format_error(lease_env: LeaseEnv) -> None:
    store = lease_env.store
    lease_env.put(store.subspace.pack((S, "owner")), b"bad")
    with pytest.raises(CheckpointFormatError):
        store.verify_owner(S, Lease(S, 1, bytes(16)))


def test_lease_for_another_stream_is_rejected(lease_env: LeaseEnv) -> None:
    with pytest.raises(ValueError):
        lease_env.store.load(S, Lease(b"other", 1, bytes(16)))


def test_non_bytes_stream_name_is_rejected(lease_env: LeaseEnv) -> None:
    store = lease_env.store
    for action in (
        lambda: store.claim("s"),
        lambda: store.load("s"),
        lambda: store.save("s", CheckpointCursor(1, 1)),
        lambda: store.verify_owner("s"),
        lambda: store.delete("s"),
    ):
        with pytest.raises(TypeError):
            action()


def test_without_lease_the_process_claim_path_is_unchanged(
    lease_env: LeaseEnv,
) -> None:
    store = lease_env.store
    assert store.load(S) is None
    with pytest.raises(CheckpointFencedError, match="Claim"):
        store.save(S, CheckpointCursor(1, 10))
    store.claim(S)
    store.save(S, CheckpointCursor(1, 10))
    store.verify_owner(S)
    assert store.load(S) == CheckpointCursor(1, 10)
