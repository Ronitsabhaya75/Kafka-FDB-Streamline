"""Input protocol and immutable output values of the serialization package."""

import enum
from dataclasses import dataclass
from typing import Final, NamedTuple, Protocol


class MutationType(enum.IntEnum):
    """The 16 declared type codes, mirroring upstream `CdcMutationType`.

    An input, test and display convenience only: the read path never looks a type
    code up in it and output values never carry it.
    """

    SET_VALUE = 0
    CLEAR_RANGE = 1
    ADD = 2
    AND = 6
    OR = 7
    XOR = 8
    APPEND_IF_FITS = 9
    MAX = 12
    MIN = 13
    SET_VERSIONSTAMPED_KEY = 14
    SET_VERSIONSTAMPED_VALUE = 15
    BYTE_MIN = 16
    BYTE_MAX = 17
    MIN_V2 = 18
    AND_V2 = 19
    COMPARE_AND_CLEAR = 20


DECLARED_TYPE_CODES: Final[frozenset[int]] = frozenset(MutationType)

MAX_TYPE_CODE: Final = 255  # native uint8_t
MAX_VERSION: Final = 2**63 - 1  # native int64_t commit version
MAX_SEQUENCE_NO: Final = 2**32 - 1  # proto uint32
MAX_TOTAL_MUTATIONS: Final = 2**32 - 1  # proto uint32
MAX_BRIDGE_TIMESTAMP_NS: Final = 253_402_300_799_999_999_999  # 9999-12-31T23:59:59.9…Z


class NativeMutation(Protocol):
    """One native mutation as FDB CDC delivers it; upstream `CdcMutation` conforms."""

    @property
    def type(self) -> int:
        """Raw type code, 0..255."""
        ...

    @property
    def param1(self) -> bytes:
        """Key, or begin key of a clear range."""
        ...

    @property
    def param2(self) -> bytes:
        """Value or operand, or end key of a clear range."""
        ...


class VersionIndex(NamedTuple):
    """Identity of a mutation on the wire; orders lexicographically in stream order."""

    fdb_version: int
    sequence_no: int


@dataclass(frozen=True, slots=True)
class Mutation:
    """A native mutation tagged with its version index. Satisfies `NativeMutation`."""

    type: int
    param1: bytes
    param2: bytes
    version_index: VersionIndex


@dataclass(frozen=True, slots=True)
class MutationBatch:
    """The mutations of one batch record, in wire order."""

    mutations: tuple[Mutation, ...]


@dataclass(frozen=True, slots=True)
class VersionEnd:
    """The group at `fdb_version` is complete and held `total_mutations`."""

    fdb_version: int
    total_mutations: int


type RecordBody = Mutation | MutationBatch | VersionEnd


@dataclass(frozen=True, slots=True)
class Record:
    """One deserialized record: envelope fields plus exactly one record body."""

    stream_name: str
    bridge_timestamp_ns: int | None
    body: RecordBody
