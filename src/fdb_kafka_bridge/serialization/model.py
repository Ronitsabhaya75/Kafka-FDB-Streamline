"""Input protocol and immutable output values of the serialization package."""

import enum
from dataclasses import dataclass
from typing import Final, NamedTuple, Protocol


class MutationType(enum.IntEnum):
    """The 16 declared type codes, mirroring upstream `CdcMutationType`.

    A convenience for callers, tests and display. The read path never looks a type
    code up in it, and output values never carry it.
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


# Package-private range limits (not part of the public ``serialization`` API).
_MAX_TYPE_CODE: Final = 255  # native uint8_t
_MAX_VERSION: Final = 2**63 - 1  # native int64_t commit version
_MAX_SEQUENCE_NO: Final = 2**32 - 1  # proto uint32
_MAX_TOTAL_MUTATIONS: Final = 2**32 - 1  # proto uint32
_MAX_BRIDGE_TIMESTAMP_NS: Final = 253_402_300_799_999_999_999  # 9999-12-31T23:59:59.9…Z


class NativeMutation(Protocol):
    """One native mutation as FDB CDC delivers it. Upstream `CdcMutation` conforms."""

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
    """Identity of a mutation on the wire. Tuple order is stream order."""

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
    """The group at `fdb_version` is complete and held `total_mutations`.

    `bridge_timestamp_ns` is the version end's own timestamp, read like
    `Record.bridge_timestamp_ns` and independently of it.
    """

    fdb_version: int
    total_mutations: int
    bridge_timestamp_ns: int | None


@dataclass(frozen=True, slots=True)
class FDBDirectoryContext:
    """Resolved FoundationDB Directory Layer context for a record."""

    path: tuple[str, ...]
    subspace_prefix: bytes


type RecordBody = Mutation | MutationBatch | VersionEnd


@dataclass(frozen=True, slots=True)
class Record:
    """One deserialized record, the envelope fields plus exactly one record body."""

    stream_name: str
    bridge_timestamp_ns: int | None
    body: RecordBody
    directory: FDBDirectoryContext | None = None
    stream_id: int | None = None
    source_id: str | None = None


@dataclass(frozen=True, slots=True)
class KafkaSinkIdentity:
    """Target Kafka cluster and topic for sink-mismatch protection."""

    cluster_id: str
    topic: str


@dataclass(frozen=True, slots=True)
class CDCCheckpoint:
    """Durable CDC checkpoint state recorded in FoundationDB."""

    format_version: int
    source_id: str
    stream_name: str
    stream_id: int
    last_consumed_version: int
    sink: KafkaSinkIdentity
    updating_instance_id: str
    update_timestamp_ns: int | None
    lease_epoch: int
    range_config_fingerprint: str


@dataclass(frozen=True, slots=True)
class LeaderLease:
    """Leader lease state recorded in FoundationDB for active leader coordination."""

    source_id: str
    stream_name: str
    owner_instance_id: str
    lease_epoch: int
    heartbeat_sequence: int
    expiration_timestamp_ns: int | None


type DedupKey = tuple[str | None, int | None, int, int]
type LegacyDedupKey = tuple[int, int]


def deduplication_identity(
    record: Record,
    mutation: Mutation,
) -> DedupKey:
    """Return the production deduplication identity for a mutation in a record.

    Production identity: (source_id, stream_id, fdb_version, sequence_no)

    Args:
        record: Deserialized record envelope containing the mutation.
        mutation: Deserialized mutation with its version index.

    Returns:
        4-tuple of (source_id, stream_id, fdb_version, sequence_no).
    """
    return (
        record.source_id,
        record.stream_id,
        mutation.version_index.fdb_version,
        mutation.version_index.sequence_no,
    )


def legacy_deduplication_identity(
    mutation: Mutation,
) -> LegacyDedupKey:
    """Return the legacy fallback deduplication identity (fdb_version, sequence_no).

    Used when source_id or stream_id is missing/unconfigured (e.g. v0.1.0 records).

    Args:
        mutation: Deserialized mutation with its version index.

    Returns:
        2-tuple of (fdb_version, sequence_no).
    """
    return (
        mutation.version_index.fdb_version,
        mutation.version_index.sequence_no,
    )
