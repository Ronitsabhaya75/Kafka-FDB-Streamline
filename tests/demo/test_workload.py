"""Unit tests for the demo's workload verification (no FDB or Kafka needed)."""

from collections.abc import Callable

import pytest

from demo.workload import expected_mutation, verify_workload
from fdbkafka.cdc.v1 import mutations_pb2

PREFIX = b"demo/abc/"
STAMP = bytes(range(1, 11))


def _wire(index: int) -> tuple[int, bytes, bytes]:
    """What CDC emits for `index`: the expectation with its stamp resolved."""
    exp = expected_mutation(PREFIX, index)
    params = [exp.param1, exp.param2]
    if exp.stamp is not None:
        which, offset = exp.stamp
        p = params[which - 1]
        params[which - 1] = p[:offset] + STAMP + p[offset + len(STAMP) :]
    return exp.type_code, params[0], params[1]


def _mutation(
    native: tuple[int, bytes, bytes], version: int, seq: int
) -> mutations_pb2.FDBMutation:
    type_code, p1, p2 = native
    vi = mutations_pb2.FDBVersionIndex(fdb_version=version, sequence_no=seq)
    if type_code == 1:
        return mutations_pb2.FDBMutation(
            version_index=vi,
            clear_range=mutations_pb2.FDBClearRange(begin_key=p1, end_key=p2),
        )
    return mutations_pb2.FDBMutation(
        version_index=vi,
        single_key_mutation=mutations_pb2.FDBSingleKeyMutation(
            key=p1, value=p2, mutation_type=type_code
        ),
    )


def _stream(count: int, per_version: int = 5) -> list[mutations_pb2.FDBMutationRecord]:
    """A well-formed record stream: one batch + one VersionEnd per version."""
    records = []
    for v, start in enumerate(range(0, count, per_version), start=100):
        idxs = range(start, min(start + per_version, count))
        batch = mutations_pb2.FDBMutationBatch(
            mutations=[_mutation(_wire(i), v, s) for s, i in enumerate(idxs)]
        )
        records.append(mutations_pb2.FDBMutationRecord(stream_name="s", batch=batch))
        records.append(
            mutations_pb2.FDBMutationRecord(
                stream_name="s",
                version_end=mutations_pb2.VersionEnd(
                    fdb_version=v, total_mutations=len(idxs)
                ),
            )
        )
    return records


def _first_mutation(
    records: list[mutations_pb2.FDBMutationRecord], index: int
) -> mutations_pb2.FDBMutation:
    for rec in records:
        if rec.HasField("batch"):
            for m in rec.batch.mutations:
                skm = (
                    m.single_key_mutation if m.HasField("single_key_mutation") else None
                )
                key = skm.key if skm else m.clear_range.begin_key
                if f"{index:08d}".encode() in key:
                    return m
    raise LookupError(index)


class TestExpectedMutation:
    def test_wire_forms_follow_api_800_rewrites(self) -> None:
        assert expected_mutation(PREFIX, 0).type_code == 0
        assert expected_mutation(PREFIX, 3).type_code == 19  # AND -> AND_V2
        assert expected_mutation(PREFIX, 8).type_code == 18  # MIN -> MIN_V2
        assert expected_mutation(PREFIX, 9).type_code == 0  # versionstamped -> SET
        assert expected_mutation(PREFIX, 10).type_code == 0

    def test_values_derive_from_index(self) -> None:
        exp = expected_mutation(PREFIX, 28)
        assert (exp.param1, exp.param2) == (PREFIX + b"set_00000028", b"val-28")
        clr = expected_mutation(PREFIX, 15)
        assert (clr.type_code, clr.param1, clr.param2) == (
            1,
            PREFIX + b"clr_00000015/a",
            PREFIX + b"clr_00000015/z",
        )
        add = expected_mutation(PREFIX, 16)
        assert add.param2 == (16).to_bytes(8, "little")

    def test_versionstamp_offset_suffix_is_stripped(self) -> None:
        vsk = expected_mutation(PREFIX, 9)
        assert vsk.param1 == PREFIX + b"vsk_00000009" + b"\x00" * 10
        assert vsk.stamp == (1, len(PREFIX + b"vsk_00000009"))
        vsv = expected_mutation(PREFIX, 10)
        assert vsv.param2 == b"vsv_00000010" + b"\x00" * 10
        assert vsv.stamp == (2, len(b"vsv_00000010"))


class TestVerifyWorkload:
    def test_well_formed_stream_passes(self) -> None:
        counts = verify_workload(_stream(42), PREFIX, 42)
        assert sum(counts.values()) == 42
        assert counts[0] == 12  # SET x2 + versionstamped x2, per 14-op cycle

    def test_single_mutation_records_are_counted(self) -> None:
        records = [
            mutations_pb2.FDBMutationRecord(mutation=_mutation(_wire(0), 7, 0)),
            mutations_pb2.FDBMutationRecord(
                version_end=mutations_pb2.VersionEnd(fdb_version=7, total_mutations=1)
            ),
        ]
        assert verify_workload(records, PREFIX, 1) == {0: 1}

    def test_missing_version_end_fails(self) -> None:
        records = [r for r in _stream(20) if r.version_end.fdb_version != 101]
        with pytest.raises(AssertionError, match="VersionEnd"):
            verify_workload(records, PREFIX, 20)

    def test_duplicate_version_end_fails(self) -> None:
        records = _stream(20)
        records.append(records[1])
        with pytest.raises(AssertionError, match="VersionEnd"):
            verify_workload(records, PREFIX, 20)

    def test_wrong_declared_total_fails(self) -> None:
        records = _stream(20)
        records[1].version_end.total_mutations = 4
        with pytest.raises(AssertionError, match="total"):
            verify_workload(records, PREFIX, 20)

    def test_sequence_gap_fails(self) -> None:
        records = _stream(20)
        records[0].batch.mutations[2].version_index.sequence_no = 7
        with pytest.raises(AssertionError, match="sequence"):
            verify_workload(records, PREFIX, 20)

    def test_duplicate_coordinate_fails(self) -> None:
        records = _stream(20)
        records[0].batch.mutations[1].version_index.sequence_no = 0
        with pytest.raises(AssertionError, match="duplicate"):
            verify_workload(records, PREFIX, 20)

    def test_wrong_value_fails(self) -> None:
        records = _stream(20)
        _first_mutation(records, 0).single_key_mutation.value = b"val-1"
        with pytest.raises(AssertionError, match="index 0"):
            verify_workload(records, PREFIX, 20)

    def test_wrong_type_fails(self) -> None:
        records = _stream(20)
        _first_mutation(records, 3).single_key_mutation.mutation_type = 6
        with pytest.raises(AssertionError, match="index 3"):
            verify_workload(records, PREFIX, 20)

    def test_missing_index_fails(self) -> None:
        records = _stream(20)
        del records[0].batch.mutations[4]
        records[1].version_end.total_mutations = 4
        with pytest.raises(AssertionError, match="missing"):
            verify_workload(records, PREFIX, 20)

    def test_index_out_of_range_fails(self) -> None:
        with pytest.raises(AssertionError, match="index 20"):
            verify_workload(_stream(21), PREFIX, 20)

    def test_duplicate_index_fails(self) -> None:
        records = _stream(20)
        dup = mutations_pb2.FDBMutation()
        dup.CopyFrom(records[0].batch.mutations[0])
        dup.version_index.sequence_no = 5
        records[0].batch.mutations.append(dup)
        records[1].version_end.total_mutations = 6
        with pytest.raises(AssertionError, match="index 0"):
            verify_workload(records, PREFIX, 20)

    def test_foreign_key_fails(self) -> None:
        records = _stream(20)
        _first_mutation(records, 0).single_key_mutation.key = b"other/set_00000000"
        with pytest.raises(AssertionError, match="prefix"):
            verify_workload(records, PREFIX, 20)

    @pytest.mark.parametrize(
        "corrupt",
        [
            lambda p: p[:-1],  # wrong length
            lambda p: p.replace(b"vsk_", b"vsq_"),  # wrong bytes before stamp
        ],
    )
    def test_versionstamped_key_compared_on_prefix_and_length(
        self, corrupt: Callable[[bytes], bytes]
    ) -> None:
        records = _stream(20)
        skm = _first_mutation(records, 9).single_key_mutation
        skm.key = corrupt(skm.key)
        with pytest.raises(AssertionError):
            verify_workload(records, PREFIX, 20)

    def test_versionstamped_value_compared_on_prefix_and_length(self) -> None:
        records = _stream(20)
        skm = _first_mutation(records, 10).single_key_mutation
        skm.value = skm.value + b"\x00"
        with pytest.raises(AssertionError, match="index 10"):
            verify_workload(records, PREFIX, 20)
