"""Expected CDC output of the demo workload, and a verifier for consumed records.

Pure functions only, so the checks are unit-testable without FDB or Kafka. Must
stay in step with `_write_samples` in `run_demo.py`: every key encodes its
workload index and every value is a function of `index % 14`.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from fdbkafka.cdc.v1 import mutations_pb2

VERSIONSTAMP_LEN = 10

_SET_VALUE = 0
_CLEAR_RANGE = 1
_INDEX_IN_KEY = re.compile(rb"[a-z_]+?(\d{8})")


@dataclass(frozen=True)
class ExpectedMutation:
    """One mutation as native CDC should emit it at API 800.

    Attributes:
        type_code: Wire mutation type.
        param1: Key, or clear-range begin key.
        param2: Value/operand, or clear-range end key.
        stamp: `(param number, byte offset)` of a versionstamp placeholder whose
            resolved bytes are unpredictable, or None if both params are exact.
    """

    type_code: int
    param1: bytes
    param2: bytes
    stamp: tuple[int, int] | None = None

    def matches(self, type_code: int, param1: bytes, param2: bytes) -> bool:
        """Whether an emitted mutation matches, ignoring only the stamp bytes.

        Args:
            type_code: Emitted mutation type.
            param1: Emitted key or begin key.
            param2: Emitted value or end key.

        Returns:
            True if type and params match byte for byte outside the stamp, and a
            stamped param has the expected length.
        """
        if type_code != self.type_code:
            return False
        got, want = [param1, param2], [self.param1, self.param2]
        if self.stamp is not None:
            which, offset = self.stamp
            g, w = got[which - 1], want[which - 1]
            end = offset + VERSIONSTAMP_LEN
            if len(g) != len(w) or g[:offset] != w[:offset] or g[end:] != w[end:]:
                return False
            got[which - 1] = want[which - 1]
        return got == want


def expected_mutation(prefix: bytes, index: int) -> ExpectedMutation:
    """The mutation CDC should emit for workload operation `index`.

    Reflects the rewrites FDB applies before CDC sees a mutation: `AND`/`MIN`
    arrive as `AND_V2`/`MIN_V2`, and versionstamped ops arrive as `SET_VALUE`
    with the stamp spliced in and the 4-byte offset suffix stripped.

    Args:
        prefix: Workload key prefix.
        index: Workload operation index.

    Returns:
        The expected wire mutation.
    """
    mod = index % 14
    idx = f"{index:08d}".encode()
    le8 = (index & 0xFFFFFFFF).to_bytes(8, "little")
    if mod == 0:
        return ExpectedMutation(_SET_VALUE, prefix + b"set_" + idx, b"val-%d" % index)
    if mod == 1:
        clr = prefix + b"clr_" + idx
        return ExpectedMutation(_CLEAR_RANGE, clr + b"/a", clr + b"/z")
    if mod == 9:
        key = prefix + b"vsk_" + idx
        placeholder = key + b"\x00" * VERSIONSTAMP_LEN
        return ExpectedMutation(_SET_VALUE, placeholder, b"vsk_payload", (1, len(key)))
    if mod == 10:
        val = b"vsv_" + idx
        placeholder = val + b"\x00" * VERSIONSTAMP_LEN
        return ExpectedMutation(
            _SET_VALUE, prefix + b"vsv_" + idx, placeholder, (2, len(val))
        )
    if mod == 13:
        return ExpectedMutation(
            _SET_VALUE, prefix + b"set_b_" + idx, b"val-b-%d" % index
        )
    type_code, tag, operand = {
        2: (2, b"add_", le8),
        3: (19, b"and_", b"\x0f"),
        4: (7, b"or_", b"\xf0"),
        5: (8, b"xor_", b"\xaa"),
        6: (9, b"app_", b"chunk"),
        7: (12, b"max_", le8),
        8: (18, b"min_", le8),
        11: (16, b"bmin_", b"\x10"),
        12: (17, b"bmax_", b"\xfe"),
    }[mod]
    return ExpectedMutation(type_code, prefix + tag + idx, operand)


def _native(m: mutations_pb2.FDBMutation) -> tuple[int, bytes, bytes]:
    if m.HasField("clear_range"):
        return _CLEAR_RANGE, m.clear_range.begin_key, m.clear_range.end_key
    skm = m.single_key_mutation
    return int(skm.mutation_type), skm.key, skm.value


def verify_workload(
    records: Iterable[mutations_pb2.FDBMutationRecord], prefix: bytes, count: int
) -> dict[int, int]:
    """Assert consumed records are exactly the CDC output of the demo workload.

    Checks that decoded workload indices are exactly `range(count)`, each matching
    its expected mutation byte for byte (versionstamped params on prefix, suffix
    and length); that `(fdb_version, sequence_no)` coordinates are unique; that
    every version has exactly one VersionEnd whose `total_mutations` equals that
    version's mutation count with sequence numbers `0..total-1`; and that the
    VersionEnd totals sum to `count`.

    Args:
        records: Decoded Kafka records, in any order.
        prefix: Workload key prefix.
        count: Number of workload operations written.

    Returns:
        Verified mutation count per wire type code.

    Raises:
        AssertionError: On the first check that fails.
    """
    seqs_by_version: dict[int, set[int]] = defaultdict(set)
    totals: dict[int, int] = {}
    seen: set[int] = set()
    type_counts: dict[int, int] = defaultdict(int)

    def check(m: mutations_pb2.FDBMutation) -> None:
        vi = m.version_index
        seqs = seqs_by_version[vi.fdb_version]
        if vi.sequence_no in seqs:
            raise AssertionError(
                f"duplicate coordinate ({vi.fdb_version}, {vi.sequence_no})"
            )
        seqs.add(vi.sequence_no)

        type_code, p1, p2 = _native(m)
        match = _INDEX_IN_KEY.match(p1, len(prefix)) if p1.startswith(prefix) else None
        if match is None:
            raise AssertionError(f"key {p1!r} is not a workload key under prefix")
        index = int(match.group(1))
        if index >= count or index in seen:
            raise AssertionError(f"unexpected or repeated mutation for index {index}")
        exp = expected_mutation(prefix, index)
        if not exp.matches(type_code, p1, p2):
            raise AssertionError(
                f"mutation for index {index} is {(type_code, p1, p2)!r}, "
                f"expected {(exp.type_code, exp.param1, exp.param2)!r}"
            )
        seen.add(index)
        type_counts[type_code] += 1

    for rec in records:
        if rec.HasField("batch"):
            for m in rec.batch.mutations:
                check(m)
        elif rec.HasField("mutation"):
            check(rec.mutation)
        elif rec.HasField("version_end"):
            ve = rec.version_end
            if ve.fdb_version in totals:
                raise AssertionError(f"duplicate VersionEnd for {ve.fdb_version}")
            totals[ve.fdb_version] = ve.total_mutations

    if len(seen) != count:
        missing = sorted(set(range(count)) - seen)
        raise AssertionError(f"{len(missing)} indices missing, e.g. {missing[:5]}")
    for version in seqs_by_version:
        if version not in totals:
            raise AssertionError(f"version {version} has no VersionEnd")
    for version, total in totals.items():
        if seqs_by_version.get(version, set()) != set(range(total)):
            raise AssertionError(
                f"version {version}: sequence numbers do not reconcile with "
                f"declared total {total}"
            )
    if sum(totals.values()) != count:
        raise AssertionError(f"VersionEnd totals sum to {sum(totals.values())}")
    return dict(type_counts)
