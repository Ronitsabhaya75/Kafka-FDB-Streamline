"""Summarize S3 coalescing-grid runs and apply the G4 latency-neutral rule.

Input: profile.py JSON files named ``<cell>_<trial>.json``. Cells without a
prefix are the fix build's grid (``off`` is its baseline); ``ctl-*`` cells ran
the base build (old run loop) against ``ctl-off``; ``low-*`` cells ran at low
load against ``low-off``.

Usage:
  python3 grid_table.py raw/*.json [--min-runs 5] > table.md
  python3 grid_table.py --check raw/R10_1.json   # exit 1 if the run is invalid
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

FIELDS = [
    "t", "step", "gap", "consume", "serialize", "produce", "flush",
    "checkpoint", "ack", "muts", "groups", "recs", "bytes", "published",
]  # fmt: skip
METRICS = [
    ("acks_s", "acks/s", "{:.1f}"),
    ("ack_ckpt_share", "ack+ckpt % wall", "{:.1%}"),
    ("zero_pub_s", "zero-publish steps/s", "{:.1f}"),
    ("gap_share", "gap % wall", "{:.1%}"),
    ("consume_p50_ms", "consume p50 ms", "{:.2f}"),
    ("delivered_s", "delivered mut/s", "{:.0f}"),
    ("lat_p50", "lat p50 ms", "{:.1f}"),
    ("lat_p90", "lat p90 ms", "{:.1f}"),
    ("lat_p99", "lat p99 ms", "{:.1f}"),
]
DELIVERY_TOLERANCE = 0.02
OFF_P99_MAX_SPREAD = 2.0


def percentile(values: Sequence[float], p: float) -> float:
    """Nearest-rank percentile (same as bench_e2e); NaN if empty."""
    if not values:
        return math.nan
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def summarize(run: dict[str, Any]) -> dict[str, Any]:
    """Per-run metrics over the measurement window ``[w0, w1)``.

    Args:
        run: A profile.py JSON document (``meta``, ``fields``, ``steps``, ``lat``).

    Returns:
        The metrics in ``METRICS`` plus ``offered_s``, ``valid`` and ``why``.
    """
    meta = run["meta"]
    w0, w1 = meta["window"]
    wall = w1 - w0
    fields = run["fields"]
    steps = [dict(zip(fields, s, strict=True)) for s in run["steps"]]
    steps = [s for s in steps if w0 <= s["t"] < w1]
    lat = [ms for t, ms in run.get("lat", []) if w0 <= t < w1]

    def total(key: str) -> float:
        return sum(s[key] for s in steps)

    delivered = total("muts") / wall
    offered = float(meta["rate"])
    out: dict[str, Any] = {
        # The ack timer wraps FDBMutationListener.acknowledge, so idle flushes count.
        "acks_s": sum(1 for s in steps if s["ack"] > 0) / wall,
        "ack_ckpt_share": (total("ack") + total("checkpoint")) / wall,
        "zero_pub_s": sum(1 for s in steps if s["published"] == 0) / wall,
        "gap_share": total("gap") / wall,
        "consume_p50_ms": percentile([s["consume"] for s in steps if s["consume"]], 0.5)
        * 1e3,
        "delivered_s": delivered,
        "offered_s": offered,
        "lat_p50": percentile(lat, 0.50),
        "lat_p90": percentile(lat, 0.90),
        "lat_p99": percentile(lat, 0.99),
    }
    why = []
    if meta.get("drained") is not True:
        why.append("not drained")
    # Writer commits batch_size-mutation txns: at low rates one txn more or less
    # in the window outweighs 2 %, so allow two txns.
    slack = max(DELIVERY_TOLERANCE * offered, 2 * meta.get("batch_size", 500) / wall)
    if offered and abs(delivered - offered) > slack:
        why.append(f"delivered {delivered:.0f}/s vs offered {offered:.0f}/s")
    if not lat:
        why.append("no latency samples")
    out["valid"] = not why
    out["why"] = "; ".join(why)
    return out


def _valid(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in runs if r["valid"]]


def _median(runs: list[dict[str, Any]], key: str) -> float:
    return statistics.median(r[key] for r in runs)


def is_neutral(cell: list[dict[str, Any]], off: list[dict[str, Any]]) -> bool:
    """G4: the cell's median p50 and median p99 both lie in off's [min, max]."""
    cell, off = _valid(cell), _valid(off)
    if not cell or not off:
        return False
    for key in ("lat_p50", "lat_p99"):
        lo, hi = min(r[key] for r in off), max(r[key] for r in off)
        if not lo <= _median(cell, key) <= hi:
            return False
    return True


def baseline_of(cell: str) -> str:
    """Name of the cell a given cell is compared against."""
    for prefix in ("ctl-", "low-"):
        if cell.startswith(prefix):
            return prefix + "off"
    return "off"


def ack_ratio(cell: list[dict[str, Any]], off: list[dict[str, Any]]) -> float:
    """Off median acks/s over the cell's: ``N`` in "N x fewer acks"."""
    cell_acks = _median(_valid(cell), "acks_s")
    return _median(_valid(off), "acks_s") / cell_acks if cell_acks else math.inf


def verdict(cells: dict[str, list[dict[str, Any]]], min_runs: int = 5) -> dict:
    """Apply G4 to the fix build's grid.

    Neutral cells (see ``is_neutral``) win; among them the biggest ack cut.
    With none neutral, the biggest ack cut is reported as a trade-off with its
    p50 cost. Control (``ctl-*``) and low-load (``low-*``) cells never compete.

    Returns:
        ``form`` ("neutral" | "trade-off" | "none"), ``cell``, ``ratio``,
        ``d_p50_ms``, ``neutral`` (list of names) and ``warnings``.
    """
    warnings = [
        f"{name}: {len(_valid(runs))} valid runs (< {min_runs}); re-run"
        for name, runs in cells.items()
        if len(_valid(runs)) < min_runs
    ]
    off = _valid(cells.get("off", []))
    out: dict[str, Any] = {
        "form": "none",
        "cell": None,
        "ratio": None,
        "d_p50_ms": None,
        "neutral": [],
        "warnings": warnings,
    }
    if not off:
        warnings.append("no valid off runs")
        return out
    p99 = [r["lat_p99"] for r in off]
    if min(p99) > 0 and max(p99) / min(p99) > OFF_P99_MAX_SPREAD:
        warnings.append(
            f"off p99 spread {max(p99) / min(p99):.1f}x > {OFF_P99_MAX_SPREAD:.0f}x: "
            "neutral is meaningless; re-run the grid with --duration 120"
        )
    grid = {
        name: runs
        for name, runs in cells.items()
        if baseline_of(name) == "off" and name != "off" and _valid(runs)
    }
    if not grid:
        return out
    neutral = [name for name, runs in grid.items() if is_neutral(runs, off)]
    pool = neutral or list(grid)
    best = max(pool, key=lambda name: ack_ratio(grid[name], off))
    out.update(
        form="neutral" if neutral else "trade-off",
        cell=best,
        ratio=ack_ratio(grid[best], off),
        d_p50_ms=_median(_valid(grid[best]), "lat_p50") - _median(off, "lat_p50"),
        neutral=neutral,
    )
    return out


def cell_of(path: Path) -> str:
    """``raw/R10_3.json`` -> ``R10``."""
    return path.stem.rsplit("_", 1)[0]


def _range(runs: list[dict[str, Any]], key: str, fmt: str) -> str:
    vals = [r[key] for r in runs]
    med = statistics.median(vals)
    return f"{fmt.format(med)} [{fmt.format(min(vals))}-{fmt.format(max(vals))}]"


def render(cells: dict[str, list[dict[str, Any]]], v: dict[str, Any]) -> str:
    """Markdown table (median [min-max] of valid runs) plus the G4 verdict."""
    head = ["cell", "valid", "ack cut vs baseline", "neutral"] + [m[1] for m in METRICS]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for name in sorted(cells, key=lambda n: (baseline_of(n), n != baseline_of(n), n)):
        runs = _valid(cells[name])
        base = cells.get(baseline_of(name), [])
        row = [name, f"{len(runs)}/{len(cells[name])}"]
        if not runs:
            lines.append("| " + " | ".join(row + ["-"] * (len(head) - 2)) + " |")
            continue
        if name == baseline_of(name) or not _valid(base):
            row += ["baseline" if name == baseline_of(name) else "-", "-"]
        else:
            row += [
                f"{ack_ratio(runs, base):.2f}x",
                "neutral" if is_neutral(runs, base) else "no",
            ]
        row += [_range(runs, key, fmt) for key, _, fmt in METRICS]
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    if v["form"] == "neutral":
        lines.append(
            f"**Verdict (G4): neutral.** Cite `{v['cell']}`: {v['ratio']:.2f}x fewer "
            f"acks, p50 {v['d_p50_ms']:+.1f} ms vs off (median p50 and p99 inside "
            f"off's min-max; within observed noise, not proven equal). "
            f"Neutral cells: {', '.join(v['neutral'])}. Check `low-{v['cell']}`."
        )
    elif v["form"] == "trade-off":
        lines.append(
            f"**Verdict (G4): none in grid is neutral.** Trade-off bullet, no pin: "
            f"`{v['cell']}` cuts acks {v['ratio']:.2f}x at {v['d_p50_ms']:+.1f} ms "
            "p50."
        )
    else:
        lines.append("**Verdict (G4): no result** (see warnings).")
    lines += [f"- WARNING: {w}" for w in v["warnings"]]
    for name in sorted(cells):
        bad = [r["why"] for r in cells[name] if not r["valid"]]
        if bad:
            lines.append(f"- {name}: {len(bad)} invalid run(s) excluded: {bad}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; see the module docstring."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("files", nargs="*", type=Path)
    ap.add_argument("--min-runs", type=int, default=5)
    ap.add_argument("--check", type=Path, help="validate one run; exit 1 if invalid")
    args = ap.parse_args(argv)
    if args.check:
        s = summarize(json.loads(args.check.read_text()))
        print(f"{args.check.name}: {'valid' if s['valid'] else 'INVALID: ' + s['why']}")
        return 0 if s["valid"] else 1
    cells: dict[str, list[dict[str, Any]]] = {}
    for path in args.files:
        cells.setdefault(cell_of(path), []).append(
            summarize(json.loads(path.read_text()))
        )
    sys.stdout.write(render(cells, verdict(cells, args.min_runs)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
