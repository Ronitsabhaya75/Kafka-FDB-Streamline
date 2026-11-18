"""Summarize bridge_tput.py run JSONs into a markdown table.

Usage: ``python3 p9_table.py raw/*.json > table.md``. Runs with
``drained: false`` are excluded. Medians and [min-max] per mode (and per rate for
sustained); drain as % of kafka-alone uses the two medians.
"""

from __future__ import annotations

import json
import statistics
import sys
from collections.abc import Iterable, Sequence
from typing import Any

GROWING_QUORUM = 3


def med_range(values: Sequence[float]) -> tuple[float, float, float]:
    """Median, min and max."""
    return statistics.median(values), min(values), max(values)


def _key(run: dict[str, Any]) -> str:
    mode = run["mode"]
    if mode == "kafka-alone" and run.get("flush_at_end"):
        return "kafka-alone --flush-at-end"
    if mode == "sustained":
        return f"sustained @{run['rate']:g}"
    if mode == "drain" and run.get("byte_counter") is False:
        return "drain (no byte counter)"
    return mode


def _cell(values: list[float | None], scale: float = 1.0) -> str:
    vs = [v * scale for v in values if v is not None]
    if not vs:
        return "-"
    m, lo, hi = med_range(vs)
    return f"{m:,.1f} [{lo:,.1f}-{hi:,.1f}]"


def highest_flat_rate(runs: Iterable[dict[str, Any]]) -> float | None:
    """Highest rate below the first one where >=3 runs show a growing backlog."""
    by_rate: dict[float, list[bool]] = {}
    for r in runs:
        if r["mode"] == "sustained":
            by_rate.setdefault(r["rate"], []).append(bool(r["backlog_growing"]))
    flat = None
    for rate in sorted(by_rate):
        if sum(by_rate[rate]) >= GROWING_QUORUM:
            break
        flat = rate
    return flat


def summarize(runs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Group drained runs and compute the per-group stats and headline ratios."""
    runs = list(runs)
    valid = [r for r in runs if r.get("drained", True)]
    groups: dict[str, dict[str, Any]] = {}
    for r in valid:
        g = groups.setdefault(_key(r), {"runs": []})
        g["runs"].append(r)
    for g in groups.values():
        g["n"] = len(g["runs"])
        g["median_mps"] = statistics.median(r["mutations_per_s"] for r in g["runs"])
    out: dict[str, Any] = {
        "groups": groups,
        "excluded": len(runs) - len(valid),
        "highest_flat_rate": highest_flat_rate(runs),
        "drain_pct_of_raw": None,
    }
    if "drain" in groups and "kafka-alone" in groups:
        out["drain_pct_of_raw"] = (
            100 * groups["drain"]["median_mps"] / groups["kafka-alone"]["median_mps"]
        )
    return out


def render(summary: dict[str, Any]) -> str:
    """Markdown table plus headline lines."""
    lines = [
        "| group | n | mutations/s | payload MB/s | tx MB/s (lz4) | records/reply"
        " | max lag ms (~) | growing |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for name in sorted(summary["groups"]):
        rs = summary["groups"][name]["runs"]
        growing = (
            f"{sum(bool(r.get('backlog_growing')) for r in rs)}/{len(rs)}"
            if name.startswith("sustained")
            else "-"
        )
        lines.append(
            f"| {name} | {len(rs)} | {_cell([r['mutations_per_s'] for r in rs])}"
            f" | {_cell([r.get('payload_mb_per_s') for r in rs])}"
            f" | {_cell([r.get('tx_mb_per_s') for r in rs])}"
            f" | {_cell([r.get('records_per_reply') for r in rs])}"
            f" | {_cell([r.get('lag_max_ms_approx') for r in rs])} | {growing} |"
        )
    pct = summary["drain_pct_of_raw"]
    lines += [
        "",
        (
            f"- drain as % of raw sink (medians): {pct:.1f}%"
            if pct is not None
            else "- drain as % of raw sink: n/a"
        ),
        f"- highest sustained rate with flat backlog: {summary['highest_flat_rate']}",
        f"- runs excluded (not drained): {summary['excluded']}",
    ]
    return "\n".join(lines)


def main(paths: Sequence[str]) -> None:
    """Load JSONs and print the table."""
    runs = []
    for p in paths:
        with open(p) as f:
            runs.append(json.load(f))
    print(render(summarize(runs)))


if __name__ == "__main__":
    main(sys.argv[1:])
