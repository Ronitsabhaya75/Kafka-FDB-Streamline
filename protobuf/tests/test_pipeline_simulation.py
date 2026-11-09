"""Pipeline simulation: version groups through the wire to a §6 reader.

Drives `serialize_version_group` output through a minimal reader that follows the
reader contract in docs/SERIALIZATION-SPEC.md §6. Live FDB CDC integration is
covered by tests/cdc/test_listener_integration.py.
"""

from fdb_kafka_bridge.serialization import (
    Mutation,
    MutationBatch,
    VersionEnd,
    deserialize_record,
    serialize_batch,
    serialize_mutation,
    serialize_version_group,
)
from tests.serialization.doubles import NativeMutation

STREAM = "fdb-cdc:accounts-directory"
TS = 1_700_000_000_000_000_000

GROUPS: dict[int, list[NativeMutation]] = {
    1_050_000: [
        NativeMutation(0, b"acc:1001:balance", (5000).to_bytes(8, "little")),
        NativeMutation(2, b"acc:1001:balance", (100).to_bytes(8, "little")),
        NativeMutation(1, b"acc:temporary:0000", b"acc:temporary:\xff"),
        NativeMutation(9, b"acc:1001:ledger", b"deposit:+$100\n"),
    ],
    1_050_050: [],
    1_050_100: [NativeMutation(0, bytes([i]), bytes(40)) for i in range(6)],
}


class Reader:
    """Commits a version group only on its `VersionEnd`, per SPEC §6."""

    def __init__(self) -> None:
        self.committed: dict[int, list[tuple[int, bytes, bytes]]] = {}
        self.watermark = -1
        self.pending: list[Mutation] = []

    def feed(self, data: bytes) -> None:
        body = deserialize_record(data).body
        if isinstance(body, VersionEnd):
            self._end(body)
            return
        mutations = body.mutations if isinstance(body, MutationBatch) else (body,)
        for mutation in mutations:
            version, sequence_no = mutation.version_index
            if version <= self.watermark:
                continue  # Already committed: identity is the version index.
            if sequence_no == 0 and self.pending:
                self.pending.clear()  # (V, 0) again before the version end.
            expected = (
                self.pending[-1].version_index[0] if self.pending else version,
                len(self.pending),
            )
            if mutation.version_index != expected:
                raise AssertionError(f"gap: got {mutation.version_index}")
            self.pending.append(mutation)

    def _end(self, end: VersionEnd) -> None:
        if end.fdb_version <= self.watermark:
            self.pending.clear()  # Repeat or regression: redelivery.
            return
        if any(m.version_index[0] != end.fdb_version for m in self.pending):
            raise AssertionError("pending mutations from another version")
        if len(self.pending) != end.total_mutations:
            raise AssertionError("version end count mismatch")
        self.committed[end.fdb_version] = [
            (m.type, m.param1, m.param2) for m in self.pending
        ]
        self.watermark = end.fdb_version
        self.pending = []


def records_for(version: int, budget: int = 150) -> list[bytes]:
    return serialize_version_group(
        GROUPS[version],
        fdb_version=version,
        max_record_bytes=budget,
        stream_name=STREAM,
        bridge_timestamp_ns=TS,
    )


def expected_commits() -> dict[int, list[tuple[int, bytes, bytes]]]:
    return {v: [(m.type, m.param1, m.param2) for m in g] for v, g in GROUPS.items()}


def test_sliced_groups_commit_once_each_including_an_empty_group() -> None:
    reader = Reader()
    stream = [r for v in GROUPS for r in records_for(v)]

    for data in stream:
        reader.feed(data)

    assert len(stream) > len(GROUPS) * 2  # Several records at one version.
    assert reader.committed == expected_commits()
    assert reader.pending == []


def test_mutation_record_and_one_element_batch_are_interchangeable() -> None:
    env = {"stream_name": STREAM, "bridge_timestamp_ns": TS}
    version = 1_050_000
    first, second, *rest = GROUPS[version]
    as_mixed = [
        serialize_mutation(first, fdb_version=version, sequence_no=0, **env),
        serialize_batch([second], fdb_version=version, first_sequence_no=1, **env),
        serialize_batch(rest, fdb_version=version, first_sequence_no=2, **env),
        records_for(version)[-1],
    ]
    reader = Reader()

    for data in as_mixed:
        reader.feed(data)

    assert reader.committed == {version: expected_commits()[version]}


def test_redelivered_partial_group_is_discarded_not_rejected() -> None:
    reader = Reader()
    v1, v2, v3 = GROUPS
    partial = records_for(v3)[:-1]
    assert len(partial) > 1

    stream = [
        *records_for(v1),
        *records_for(v2),
        partial[0],  # Delivery of v3 cut short...
        *records_for(v1),  # ...then redelivery from v1: a regression.
        *records_for(v2),
        *records_for(v3),
    ]
    for data in stream:
        reader.feed(data)

    assert reader.committed == expected_commits()
    assert reader.watermark == v3


def test_group_restarting_at_position_zero_discards_the_partial() -> None:
    reader = Reader()
    version = 1_050_100
    records = records_for(version)

    for data in [records[0], records[1], *records]:
        reader.feed(data)

    assert reader.committed == {version: expected_commits()[version]}
