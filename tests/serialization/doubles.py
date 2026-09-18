from dataclasses import dataclass
from typing import Any, NamedTuple


class NativeMutation(NamedTuple):
    type: int
    param1: bytes
    param2: bytes


class VersionGroup(NamedTuple):
    version: int
    mutations: tuple[NativeMutation, ...]


@dataclass(frozen=True, slots=True)
class AttrOnlyMutation:
    type: int
    param1: bytes
    param2: bytes


STREAM = "orders"
TS = 1_700_000_000_123_456_789
V = 2**40 + 7
ENV: dict[str, Any] = {"stream_name": STREAM, "bridge_timestamp_ns": TS}
SET = NativeMutation(0, b"k", b"v")
ADD = NativeMutation(2, b"\x15\x01k\x00", b"\xff\x00v")
CLEAR = NativeMutation(1, b"a", b"b\xff")
