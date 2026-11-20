"""In-memory stand-in for the ``fdb`` bindings, honest enough for lease semantics.

Models what the lease conformance suite relies on: MVCC snapshot reads, read
conflict ranges (snapshot reads add none), ``add`` as a blind little-endian
counter, per-transaction timeout measured on an injected clock from creation,
``on_error`` retry limits and backoff, and the tuple layer for ``None``,
``bytes``, ``str`` and ``int``. ``Database.on_op`` lets a test inject latency or
errors at ``get``, ``get_range``, ``commit`` and ``committed`` (after the writes
apply, so raising there models ``commit_unknown_result`` on a landed commit).
"""

import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import Any

RETRYABLE = frozenset({1007, 1009, 1020, 1021, 1037, 1039, 1042, 1078, 1213})
MAYBE_COMMITTED = frozenset({1021, 1039})
TIMED_OUT = 1031


class FakeClock:
    """Monotonic clock that only moves when told to."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, seconds)

    def wait(self, event: threading.Event, timeout: float) -> bool:
        if not event.is_set():
            self.now += max(0.0, timeout)
        return event.is_set()


class FDBError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"fdb error {code}")
        self.code = code


predicates = SimpleNamespace(
    is_retryable=lambda e: e.code in RETRYABLE,
    is_maybe_committed=lambda e: e.code in MAYBE_COMMITTED,
)


# --- tuple layer (subset of fdb.tuple, byte-compatible for the types it accepts)


def _escape(raw: bytes) -> bytes:
    return raw.replace(b"\x00", b"\x00\xff") + b"\x00"


def _pack_one(value: object) -> bytes:
    if value is None:
        return b"\x00"
    if isinstance(value, bytes):
        return b"\x01" + _escape(value)
    if isinstance(value, str):
        return b"\x02" + _escape(value.encode("utf-8"))
    if type(value) is int:
        if value == 0:
            return b"\x14"
        n = (abs(value).bit_length() + 7) // 8
        if n > 8:
            raise ValueError("integer too large for fake tuple layer")
        if value > 0:
            return bytes([0x14 + n]) + value.to_bytes(n, "big")
        return bytes([0x14 - n]) + ((1 << (8 * n)) - 1 + value).to_bytes(n, "big")
    raise ValueError(f"unsupported tuple element {value!r}")


def pack(t: tuple, prefix: bytes = b"") -> bytes:
    return prefix + b"".join(_pack_one(v) for v in t)


def _unescape(data: bytes, pos: int) -> tuple[bytes, int]:
    out = bytearray()
    while True:
        if pos >= len(data):
            raise ValueError("unterminated tuple string")
        byte = data[pos]
        if byte == 0:
            if pos + 1 < len(data) and data[pos + 1] == 0xFF:
                out.append(0)
                pos += 2
                continue
            return bytes(out), pos + 1
        out.append(byte)
        pos += 1


def unpack(key: bytes, prefix_len: int = 0) -> tuple:
    out: list[object] = []
    pos = prefix_len
    while pos < len(key):
        code = key[pos]
        pos += 1
        if code == 0x00:
            out.append(None)
        elif code == 0x01:
            raw, pos = _unescape(key, pos)
            out.append(raw)
        elif code == 0x02:
            raw, pos = _unescape(key, pos)
            out.append(raw.decode("utf-8"))
        elif 0x0C <= code <= 0x1C:
            n = abs(code - 0x14)
            if pos + n > len(key):
                raise ValueError("truncated tuple integer")
            raw_int = int.from_bytes(key[pos : pos + n], "big")
            out.append(raw_int if code >= 0x14 else raw_int - ((1 << (8 * n)) - 1))
            pos += n
        else:
            raise ValueError(f"unknown tuple type code {code:#x}")
    return tuple(out)


class Subspace:
    def __init__(self, prefix_tuple: tuple = (), raw_prefix: bytes = b"") -> None:
        self.raw_prefix = pack(prefix_tuple, raw_prefix)

    def key(self) -> bytes:
        return self.raw_prefix

    def pack(self, t: tuple = ()) -> bytes:
        return pack(t, self.raw_prefix)

    def unpack(self, key: bytes) -> tuple:
        if not key.startswith(self.raw_prefix):
            raise ValueError("Cannot unpack key that is not in subspace.")
        return unpack(key, len(self.raw_prefix))


# --- database


def _add(existing: bytes | None, param: bytes) -> bytes:
    n = len(param)
    base = (existing or b"")[:n].ljust(n, b"\x00")
    total = int.from_bytes(base, "little") + int.from_bytes(param, "little")
    return (total % (1 << (8 * n))).to_bytes(n, "little")


def _apply(value: bytes | None, ops: Iterable[tuple[str, Any]]) -> bytes | None:
    for kind, arg in ops:
        if kind == "set":
            value = arg
        elif kind == "clear":
            value = None
        else:
            value = _add(value, arg)
    return value


@dataclass
class KeyValue:
    key: bytes
    value: bytes


class _Done:
    def __init__(self, value: Any = None) -> None:
        self.value = value

    def wait(self) -> "_Done":
        return self

    def present(self) -> bool:
        return self.value is not None


class _Options:
    def __init__(self, tr: "Transaction") -> None:
        self._tr = tr

    def set_timeout(self, ms: int) -> None:
        self._tr.timeout_ms = ms

    def set_retry_limit(self, n: int) -> None:
        self._tr.retry_limit = n

    def set_priority_system_immediate(self) -> None:
        self._tr.priority = "system_immediate"

    def set_priority_batch(self) -> None:
        self._tr.priority = "batch"


class _Reader:
    def __init__(self, tr: "Transaction", snapshot: bool) -> None:
        self._tr = tr
        self._snapshot = snapshot

    def get(self, key: bytes) -> _Done:
        tr = self._tr
        tr._op("get")
        if not self._snapshot:
            tr.reads.append((key, key + b"\x00"))
        base = tr.db._at(key, tr._version())
        return _Done(_apply(base, (op for k, op in tr.writes if k == key)))

    def get_range(self, begin: bytes, end: bytes) -> list[KeyValue]:
        tr = self._tr
        tr._op("get_range")
        if not self._snapshot:
            tr.reads.append((begin, end))
        version = tr._version()
        keys = {k for k in tr.db.history if begin <= k < end}
        keys |= {k for k, _ in tr.writes if begin <= k < end}
        rows = []
        for key in sorted(keys):
            value = _apply(
                tr.db._at(key, version), (op for k, op in tr.writes if k == key)
            )
            if value is not None:
                rows.append(KeyValue(key, value))
        return rows


class Transaction(_Reader):
    def __init__(self, db: "Database") -> None:
        super().__init__(self, snapshot=False)
        self.db = db
        self.options = _Options(self)
        self.snapshot = _Reader(self, snapshot=True)
        self.created = db.clock.monotonic()
        self.timeout_ms = 0
        self.retry_limit: int | None = None
        self.retries = 0
        self.priority = "default"
        self.priorities: list[str] = []
        self.cancelled = False
        self._reset()

    def _reset(self) -> None:
        self.read_version: int | None = None
        self.reads: list[tuple[bytes, bytes]] = []
        self.writes: list[tuple[bytes, tuple[str, Any]]] = []

    def _version(self) -> int:
        if self.read_version is None:
            self.priorities.append(self.priority)
            self.read_version = self.db.version
        return self.read_version

    def _timed_out(self) -> bool:
        return bool(self.timeout_ms) and (
            self.db.clock.monotonic() - self.created >= self.timeout_ms / 1000
        )

    def _op(self, name: str) -> None:
        if self.db.on_op is not None:
            self.db.on_op(name, self)
        if self._timed_out():
            raise FDBError(TIMED_OUT)

    def set(self, key: bytes, value: bytes) -> None:
        self.writes.append((key, ("set", value)))

    __setitem__ = set

    def clear(self, key: bytes) -> None:
        self.writes.append((key, ("clear", None)))

    __delitem__ = clear

    def add(self, key: bytes, param: bytes) -> None:
        self.writes.append((key, ("add", param)))

    def commit(self) -> _Done:
        self._op("commit")
        db = self.db
        if self.writes and self.read_version is not None:
            for begin, end in self.reads:
                for key, versions in db.history.items():
                    if begin <= key < end and versions[-1][0] > self.read_version:
                        raise FDBError(1020)
        if self.writes:
            db.version += 1
            for key, op in self.writes:
                value = _apply(db._at(key, db.version), [op])
                db.history.setdefault(key, []).append((db.version, value))
        self._op("committed")
        return _Done()

    def on_error(self, code: int) -> _Done:
        if self._timed_out():
            raise FDBError(TIMED_OUT)
        if code not in RETRYABLE:
            raise FDBError(code)
        if self.retry_limit is not None and self.retries >= self.retry_limit:
            raise FDBError(code)
        self.retries += 1
        self.db.clock.sleep(min(0.01 * 2 ** (self.retries - 1), 1.0))
        self.priority = "default"
        self._reset()
        return _Done()

    def cancel(self) -> None:
        self.cancelled = True


class Database:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.version = 0
        self.history: dict[bytes, list[tuple[int, bytes | None]]] = {}
        self.on_op: Callable[[str, Transaction], None] | None = None
        self.transactions: list[Transaction] = []

    def _at(self, key: bytes, version: int) -> bytes | None:
        value = None
        for v, stored in self.history.get(key, []):
            if v <= version:
                value = stored
        return value

    def create_transaction(self) -> Transaction:
        tr = Transaction(self)
        self.transactions.append(tr)
        return tr

    def __getitem__(self, key: bytes) -> bytes | None:
        return self._at(key, self.version)

    def __setitem__(self, key: bytes, value: bytes) -> None:
        self.version += 1
        self.history.setdefault(key, []).append((self.version, value))

    def __delitem__(self, key: bytes) -> None:
        self.version += 1
        self.history.setdefault(key, []).append((self.version, None))


def module() -> tuple[ModuleType, ModuleType]:
    """Build ``fdb`` and ``fdb.tuple`` stand-ins for ``sys.modules``."""
    tuple_module = ModuleType("fdb.tuple")
    tuple_module.pack = pack  # type: ignore[attr-defined]
    tuple_module.unpack = unpack  # type: ignore[attr-defined]
    fdb = ModuleType("fdb")
    fdb.FDBError = FDBError  # type: ignore[attr-defined]
    fdb.predicates = predicates  # type: ignore[attr-defined]
    fdb.tuple = tuple_module  # type: ignore[attr-defined]
    fdb.Subspace = Subspace  # type: ignore[attr-defined]
    return fdb, tuple_module
