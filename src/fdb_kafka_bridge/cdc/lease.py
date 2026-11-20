"""Per-stream leases on the checkpoint store: owner record v2, heartbeats, pod keys.

Implements the lease state and transactions of ``docs/FAILOVER-SPEC.md``. Every
lease key lives in the open ``CheckpointStore`` subspace. Stream keys lead with the
raw stream name as bytes and pod keys with the text ``"pod"``, so the two never
collide and one range read returns both.
"""

import os
import struct
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib import import_module
from threading import Event
from typing import Any, Protocol

from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointFormatError,
    CheckpointStore,
    check_stream_name,
)

# Timings (FAILOVER-SPEC §Timings). Module constants, not configuration.
RENEW_PERIOD_S: float = 0.2
WATCH_PERIOD_S: float = 0.1
WATCH_TIMEOUT_S: float = 0.2
STALE_S: float = 1.0
SELF_DROP_S: float = 0.8
STOP_BUDGET_S: float = STALE_S - SELF_DROP_S
CLAIM_TIMEOUT_S: float = 1.0
CLAIM_RETRY_LIMIT: int = 5
RELEASE_TIMEOUT_S: float = 1.0
RELEASE_RETRY_LIMIT: int = 5
CLAIM_DELAY_STEP_S: float = 0.05
CLAIM_DELAY_MAX_S: float = 0.2
CLAIM_DELAY_JITTER_S: float = 0.05
BACKSTOP_S: float = STALE_S + 2 * CLAIM_DELAY_MAX_S
BLOCKED_RECHECK_S: float = 10.0
PROGRESS_DEADLINE_S: float = 10.0
RECLAIM_COOLDOWN_S: float = 5.0
RECLAIM_COOLDOWN_MAX_S: float = 60.0
POD_JOIN_S: float = 10.0
POD_GC_PERIOD_S: float = 60.0
POD_GC_STALE_S: float = 300.0

OWNER_RECORD_V2: int = 2
_V1 = 1


class Clock(Protocol):
    """Time source threaded through the lease library and its callers."""

    def monotonic(self) -> float:
        """Return monotonic seconds."""
        ...

    def sleep(self, seconds: float) -> None:
        """Block for ``seconds``."""
        ...

    def wait(self, event: Event, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for ``event``; return whether it is set."""
        ...


class SystemClock:
    """Real monotonic time."""

    def monotonic(self) -> float:
        """Return ``time.monotonic()``."""
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        """Sleep for ``seconds``."""
        time.sleep(seconds)

    def wait(self, event: Event, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for ``event``; return whether it is set."""
        return event.wait(timeout)


@dataclass(frozen=True)
class OwnerRecord:
    """Decoded owner record.

    Attributes:
        version: 2 for the current format, 1 for a single-process record. A v1
            record is held with no heartbeat, so it only ever goes stale.
        epoch: Claim counter; never decreases.
        owner_id: Current tenure's 16-byte identity, or None once released.
        pod_name: Holder's display name (v2 only); never compared.
    """

    version: int
    epoch: int
    owner_id: bytes | None
    pod_name: str | None


def encode_owner(epoch: int, owner_id: bytes | None, pod_name: str) -> bytes:
    """Encode an owner record v2.

    Args:
        epoch: Claim counter.
        owner_id: Tenure identity, or None for a released record.
        pod_name: Display name of the holder.

    Returns:
        Tuple-packed ``(2, epoch, owner_id, pod_name)``.
    """
    return import_module("fdb.tuple").pack((OWNER_RECORD_V2, epoch, owner_id, pod_name))


def _is_epoch(value: object) -> bool:
    return type(value) is int and value >= 1


def _is_owner_id(value: object) -> bool:
    return isinstance(value, bytes) and len(value) == 16


def decode_owner(value: bytes | None) -> OwnerRecord | None:
    """Decode a stored owner record of any readable version.

    Args:
        value: Raw owner key value, or None when the key is absent.

    Returns:
        The record, or None when no record exists.

    Raises:
        CheckpointFormatError: If the record is corrupt or has an unknown version.
    """
    if value is None:
        return None
    try:
        fields = import_module("fdb.tuple").unpack(value)
    except (ValueError, TypeError, IndexError, struct.error) as exc:
        raise CheckpointFormatError("Unreadable owner record") from exc
    match fields:
        case (2, epoch, owner_id, str() as pod_name) if _is_epoch(epoch) and (
            owner_id is None or _is_owner_id(owner_id)
        ):
            return OwnerRecord(OWNER_RECORD_V2, epoch, owner_id, pod_name)
        case (1, epoch, owner_id) if _is_epoch(epoch) and _is_owner_id(owner_id):
            return OwnerRecord(_V1, epoch, owner_id, None)
    raise CheckpointFormatError(f"Unreadable owner record {fields!r}")


def decode_heartbeat(value: bytes | None) -> int:
    """Decode a little-endian heartbeat counter; an absent key reads as 0."""
    return int.from_bytes(value or b"", "little")


_ONE = (1).to_bytes(8, "little")
_RENEW_BACKOFF_S = 0.01


@dataclass(frozen=True)
class Lease:
    """Proof of one tenure, passed to every fenced checkpoint operation.

    Attributes:
        stream_name: Raw stream name.
        epoch: Epoch written by the claim.
        owner_id: Tenure identity written by the claim.
    """

    stream_name: bytes
    epoch: int
    owner_id: bytes


@dataclass(frozen=True)
class LeaseValue:
    """What a watcher observed for a stream: raw owner record plus heartbeat.

    Attributes:
        owner: Raw owner record bytes, or None when absent.
        heartbeat: Heartbeat counter (0 when absent).
    """

    owner: bytes | None
    heartbeat: int


@dataclass(frozen=True)
class Claimed:
    """A successful claim.

    Attributes:
        lease: The new tenure.
        started: Monotonic pre-GRV start of the attempt that committed; the
            tenure's first renew anchor (self-drop deadline = started + SELF_DROP_S).
    """

    lease: Lease
    started: float


@dataclass(frozen=True)
class RenewResult:
    """Outcome of one renew tick.

    Attributes:
        started: Pre-GRV start of the committed attempt, the new ``last_ok_start``
            for every lease in ``renewed``; None if no attempt committed before
            the deadline.
        renewed: Leases whose heartbeat the committed attempt bumped.
        lost: Leases whose owner record no longer names their tenure.
        unreadable: Leases whose owner record cannot be decoded; never claimed,
            so the stream blocks rather than moving.
        expired: Leases whose self-drop deadline passed before an attempt.
    """

    started: float | None
    renewed: frozenset[Lease]
    lost: frozenset[Lease]
    unreadable: frozenset[Lease]
    expired: frozenset[Lease]


class Leases:
    """Lease transactions for one pod over an open checkpoint store."""

    def __init__(
        self,
        store: CheckpointStore,
        *,
        pod_name: str,
        instance_id: bytes | None = None,
        clock: Clock | None = None,
    ) -> None:
        """Bind to an open store without touching FDB.

        Args:
            store: Opened checkpoint store whose subspace holds the leases.
            pod_name: Display name written into owner records and the pod name key.
            instance_id: 16-byte identity of this pod process; random by default.
            clock: Time source; real monotonic time by default.
        """
        self.store = store
        self.pod_name = pod_name
        self.instance_id = os.urandom(16) if instance_id is None else instance_id
        self.clock: Clock = SystemClock() if clock is None else clock

    def _key(self, stream_name: bytes, kind: str) -> bytes:
        check_stream_name(stream_name)
        return self.store.subspace.pack((stream_name, kind))

    def _pod_key(self, kind: str) -> bytes:
        return self.store.subspace.pack(("pod", self.instance_id, kind))

    def claim(
        self, stream_name: bytes, seen: LeaseValue | None = None
    ) -> Claimed | None:
        """Claim a released stream, or one whose lease still shows ``seen``.

        Any failure means not claimed; a claim that committed unseen goes stale.

        Args:
            stream_name: Raw stream name.
            seen: Value the caller watched go stale; None claims only a released
                or absent owner record.

        Returns:
            The claim, or None when not claimed.

        Raises:
            TypeError: If ``stream_name`` is not bytes.
        """
        fdb = import_module("fdb")
        owner_key = self._key(stream_name, "owner")
        hb_key = self._key(stream_name, "hb")
        owner_id = os.urandom(16)
        # Starts of attempts whose commit may have landed. A later attempt that
        # reads our owner_id anchors on the earliest of them: no watcher can have
        # seen the record before that attempt began.
        maybe_committed: list[float] = []
        tr = self.store.client.db.create_transaction()
        try:
            while True:
                start = self.clock.monotonic()
                try:
                    tr.options.set_priority_system_immediate()
                    tr.options.set_timeout(int(CLAIM_TIMEOUT_S * 1000))
                    tr.options.set_retry_limit(CLAIM_RETRY_LIMIT)
                    owner_future = tr.get(owner_key)
                    # Non-snapshot so a renew committing meanwhile aborts the claim.
                    hb_future = tr.get(hb_key)
                    raw = owner_future.value
                    current = LeaseValue(raw, decode_heartbeat(hb_future.value))
                    try:
                        record = decode_owner(raw)
                    except CheckpointFormatError:
                        return None
                    if record is not None and record.owner_id == owner_id:
                        lease = Lease(stream_name, record.epoch, owner_id)
                        return Claimed(lease, maybe_committed[0])
                    if not (
                        record is None or record.owner_id is None or current == seen
                    ):
                        return None
                    epoch = 1 if record is None else record.epoch + 1
                    tr[owner_key] = encode_owner(epoch, owner_id, self.pod_name)
                    tr.commit().wait()
                    return Claimed(Lease(stream_name, epoch, owner_id), start)
                except fdb.FDBError as exc:
                    if fdb.predicates.is_maybe_committed(exc):
                        maybe_committed.append(start)
                    tr.on_error(exc.code).wait()
        except fdb.FDBError:
            return None
        finally:
            tr.cancel()

    def renew(self, leases: Mapping[Lease, float]) -> RenewResult:
        """Bump the heartbeat of every lease still ours, plus the pod heartbeat.

        One snapshot-read transaction with no read conflicts, so a claim never
        aborts it. Retries without limit until an attempt commits or the deadline
        passes; each attempt times out at the earliest remaining deadline.

        Args:
            leases: Each held lease mapped to its ``last_ok_start``.

        Returns:
            The tick's outcome.
        """
        fdb = import_module("fdb")
        pending = dict(leases)
        expired: set[Lease] = set()
        # Bounds a tick with no leases; every lease deadline is at or before it.
        pod_deadline = self.clock.monotonic() + SELF_DROP_S
        while True:
            now = self.clock.monotonic()
            for lease, last_ok_start in list(pending.items()):
                if now - last_ok_start > SELF_DROP_S:
                    expired.add(lease)
                    del pending[lease]
            if now >= pod_deadline:
                return RenewResult(
                    None, frozenset(), frozenset(), frozenset(), frozenset(expired)
                )
            deadline = min([t + SELF_DROP_S for t in pending.values()] + [pod_deadline])
            tr = self.store.client.db.create_transaction()
            try:
                tr.options.set_priority_system_immediate()
                tr.options.set_timeout(max(1, int((deadline - now) * 1000)))
                reads = {
                    lease: tr.snapshot.get(self._key(lease.stream_name, "owner"))
                    for lease in pending
                }
                renewed: set[Lease] = set()
                unreadable: set[Lease] = set()
                for lease, future in reads.items():
                    try:
                        record = decode_owner(future.value)
                    except CheckpointFormatError:
                        unreadable.add(lease)
                        continue
                    if _shows(record, lease.epoch, lease.owner_id):
                        tr.add(self._key(lease.stream_name, "hb"), _ONE)
                        renewed.add(lease)
                tr.add(self._pod_key("hb"), _ONE)
                tr.commit().wait()
                return RenewResult(
                    now,
                    frozenset(renewed),
                    frozenset(pending) - renewed - unreadable,
                    frozenset(unreadable),
                    frozenset(expired),
                )
            except fdb.FDBError as exc:
                try:
                    tr.on_error(exc.code).wait()
                except fdb.FDBError:
                    # Even permanent errors retry; only the deadline ends it.
                    self.clock.sleep(_RENEW_BACKOFF_S)
            finally:
                tr.cancel()

    def release(self, lease: Lease) -> bool:
        """Release a tenure that is still ours: clear the owner ID, keep the epoch.

        Idempotent. Runs at default priority, so a throttled release fails and the
        caller drops instead.

        Args:
            lease: Tenure to release.

        Returns:
            Whether the owner record now shows this tenure released.

        Raises:
            TypeError: If ``lease.stream_name`` is not bytes.
        """
        fdb = import_module("fdb")
        key = self._key(lease.stream_name, "owner")
        tr = self.store.client.db.create_transaction()
        try:
            while True:
                try:
                    tr.options.set_timeout(int(RELEASE_TIMEOUT_S * 1000))
                    tr.options.set_retry_limit(RELEASE_RETRY_LIMIT)
                    record = _record(tr.get(key).value)
                    if not _shows(record, lease.epoch, lease.owner_id):
                        # Every claim raises the epoch, so only this tenure can
                        # have written (epoch, None): our own earlier attempt.
                        return _shows(record, lease.epoch, None)
                    tr[key] = encode_owner(lease.epoch, None, self.pod_name)
                    tr.commit().wait()
                    return True
                except fdb.FDBError as exc:
                    tr.on_error(exc.code).wait()
        except fdb.FDBError:
            return False
        finally:
            tr.cancel()

    def register_pod(self) -> None:
        """Write this pod's display-name key; once, at pod start.

        Raises:
            CheckpointError: If the store transaction fails.
        """
        key = self._pod_key("name")
        name = self.pod_name.encode("utf-8")

        def write(tr: Any) -> None:
            tr[key] = name

        self.store.run_transaction(write, write=True)

    def clear_pod(self) -> None:
        """Clear this pod's heartbeat and name keys; after its releases on SIGTERM.

        Raises:
            CheckpointError: If the store transaction fails.
        """
        self._clear_pods([self.instance_id], batch=False)

    def gc_pods(self, instance_ids: Iterable[bytes]) -> None:
        """Blindly clear both keys of each pod; idempotent, at batch priority.

        Args:
            instance_ids: Pods to forget, e.g. ``Watcher.stale_pods()``.

        Raises:
            CheckpointError: If the store transaction fails.
        """
        self._clear_pods(instance_ids, batch=True)

    def _clear_pods(self, instance_ids: Iterable[bytes], *, batch: bool) -> None:
        sub = self.store.subspace
        keys = [
            sub.pack(("pod", instance_id, kind))
            for instance_id in instance_ids
            for kind in ("hb", "name")
        ]

        def clear(tr: Any) -> None:
            if batch:
                tr.options.set_priority_batch()
            for key in keys:
                del tr[key]

        self.store.run_transaction(clear, write=True)


def _record(value: bytes | None) -> OwnerRecord | None:
    """Decode an owner record, treating an unreadable one as absent."""
    try:
        return decode_owner(value)
    except CheckpointFormatError:
        return None


def _shows(record: OwnerRecord | None, epoch: int, owner_id: bytes | None) -> bool:
    """Return whether an owner record holds exactly ``(epoch, owner_id)``."""
    return record is not None and (record.epoch, record.owner_id) == (epoch, owner_id)


@dataclass(frozen=True)
class _Seen:
    value: LeaseValue
    since: float
    record: OwnerRecord | None
    readable: bool


class Watcher:
    """Tracks stream leases and pod heartbeats from one range read per tick.

    Each observed value is anchored at the monotonic time of the read that first
    returned it, taken after the read completes: any renew the read reflects
    started before then, so no lease is judged stale early.
    """

    def __init__(self, leases: Leases, stream_names: Iterable[bytes]) -> None:
        """Watch the given configured streams.

        Args:
            leases: Lease library supplying the store and clock.
            stream_names: Configured raw stream names; other streams are ignored.

        Raises:
            TypeError: If a stream name is not bytes.
        """
        self._leases = leases
        self._streams = frozenset(stream_names)
        for name in self._streams:
            check_stream_name(name)
        self._seen: dict[bytes, _Seen] = {}
        self._pods: dict[bytes, tuple[int, float]] = {}
        self._read_at: float | None = None

    def poll(self) -> bool:
        """Run one watch read: no retries, ``WATCH_TIMEOUT_S`` timeout.

        A failed read keeps every anchor, but nothing is claimable until a later
        read succeeds and still shows the same value.

        Returns:
            Whether the read succeeded.
        """
        fdb = import_module("fdb")
        store = self._leases.store
        tr = store.client.db.create_transaction()
        try:
            tr.options.set_priority_system_immediate()
            tr.options.set_timeout(int(WATCH_TIMEOUT_S * 1000))
            begin, end = store.key_range()
            rows = list(tr.get_range(begin, end))
        except fdb.FDBError:
            self._read_at = None
            return False
        finally:
            tr.cancel()
        now = self._leases.clock.monotonic()
        owners: dict[bytes, bytes] = {}
        heartbeats: dict[bytes, int] = {}
        pods: dict[bytes, int] = {}
        for row in rows:
            try:
                key = store.subspace.unpack(row.key)
            except (ValueError, TypeError, IndexError, struct.error):
                continue  # Not a lease key.
            match key:
                case (bytes() as name, "owner") if name in self._streams:
                    owners[name] = row.value
                case (bytes() as name, "hb") if name in self._streams:
                    heartbeats[name] = decode_heartbeat(row.value)
                case ("pod", bytes() as instance_id, "hb"):
                    pods[instance_id] = decode_heartbeat(row.value)
                case ("pod", bytes() as instance_id, _):
                    pods.setdefault(instance_id, 0)
        for name in self._streams:
            owner = owners.get(name)
            value = LeaseValue(owner, heartbeats.get(name, 0))
            previous = self._seen.get(name)
            if previous is None or previous.value != value:
                try:
                    self._seen[name] = _Seen(value, now, decode_owner(owner), True)
                except CheckpointFormatError:
                    self._seen[name] = _Seen(value, now, None, False)
        observed: dict[bytes, tuple[int, float]] = {}
        for instance_id, heartbeat in pods.items():
            known = self._pods.get(instance_id)
            if known is None or known[0] != heartbeat:
                known = (heartbeat, now)
            observed[instance_id] = known
        self._pods = observed
        self._read_at = now
        return True

    def claimable(self) -> dict[bytes, LeaseValue]:
        """Return streams that may be claimed, each with the value to claim against.

        A stream is claimable when its owner record is absent or released, or when
        its value has stayed unchanged for ``STALE_S`` up to the last read. Empty
        after a failed read. Unreadable records are never claimable.
        """
        if self._read_at is None:
            return {}
        return {
            name: seen.value
            for name, seen in self._seen.items()
            if seen.readable
            and (
                seen.record is None
                or seen.record.owner_id is None
                or self._read_at - seen.since >= STALE_S
            )
        }

    def unreadable(self) -> frozenset[bytes]:
        """Return configured streams whose owner record cannot be decoded."""
        return frozenset(name for name, seen in self._seen.items() if not seen.readable)

    def stale_pods(self) -> list[bytes]:
        """Return pods whose heartbeat has been stale for over ``POD_GC_STALE_S``."""
        if self._read_at is None:
            return []
        limit = STALE_S + POD_GC_STALE_S
        return [
            instance_id
            for instance_id, (_, since) in self._pods.items()
            if self._read_at - since > limit
        ]
