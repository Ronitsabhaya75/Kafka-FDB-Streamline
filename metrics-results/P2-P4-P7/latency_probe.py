"""Write-to-Kafka latency of one delivered batch, for the S3 coalescing grid.

Pure (no FDB/Kafka): profile.py calls it right after
``BridgePipeline._produce_and_flush`` returns.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

TS_WIDTH = 19  # bench_e2e.Writer prefixes each value with f"{time.time_ns():019d}|"


def batch_latency_ms(records: Sequence[Any], flushed_ns: int) -> float | None:
    """Return ms from the newest mutation's client write to ``flushed_ns``.

    Args:
        records: ``FDBMutationRecord``s of one delivered reply, in order.
        flushed_ns: ``time.time_ns()`` taken when the Kafka flush returned.

    Returns:
        Latency in ms, or None if no batch ends in a timestamped SET value.
    """
    for record in reversed(records):
        if not record.HasField("batch") or not len(record.batch.mutations):
            continue
        last = record.batch.mutations[-1]
        if last.WhichOneof("mutation") != "single_key_mutation":
            return None
        value = last.single_key_mutation.value
        stamp = value[:TS_WIDTH]
        if value[TS_WIDTH : TS_WIDTH + 1] == b"|" and stamp.isdigit():
            return (flushed_ns - int(stamp)) / 1e6
        return None
    return None
