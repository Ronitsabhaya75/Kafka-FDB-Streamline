"""Unit tests for grid_table.py on synthetic profile.py runs (no FDB/Kafka)."""

# ruff: noqa: D103  # test names are the docs, as under tests/ (pyproject ignores D there)

import json
from pathlib import Path

import grid_table as gt
import pytest

FIELDS = gt.FIELDS
W0, W1 = 100.0, 110.0  # 10 s window


def _step(t: float, *, ack: float = 0.0, muts: int = 0, **kw: float) -> list:
    row = dict.fromkeys(FIELDS, 0)
    row.update(t=t, ack=ack, muts=muts, published=1 if muts or ack else 0, **kw)
    return [row[f] for f in FIELDS]


def _run(
    *,
    acks: int = 10,
    muts_per_ack: int = 1500,
    lat: list[float] | None = None,
    rate: float = 1500.0,
    drained: bool = True,
    extra_steps: list[list] | None = None,
) -> dict:
    steps = [_step(W0 + i * 0.5, ack=0.002, muts=muts_per_ack) for i in range(acks)]
    steps += extra_steps or []
    lat = lat if lat is not None else [10.0] * 10
    return {
        "meta": {
            "window": [W0, W1],
            "rate": rate,
            "drained": drained,
            "batch_size": 500,
        },
        "fields": FIELDS,
        "steps": steps,
        "lat": [[W0 + 1 + i * 0.01, v] for i, v in enumerate(lat)],
    }


def test_summarize_counts_acks_and_metrics_inside_window_only() -> None:
    outside = [
        _step(W0 - 1, ack=0.002, muts=999),
        _step(W1, ack=0.002, muts=999),  # window is half-open
    ]
    zero = [_step(W0 + 0.2, gap=0.1, consume=0.004), _step(W0 + 0.3, consume=0.006)]
    run = _run(extra_steps=outside + zero)
    run["lat"].append([W1 + 1, 999.0])

    s = gt.summarize(run)

    assert s["acks_s"] == pytest.approx(1.0)
    assert s["delivered_s"] == pytest.approx(1500.0)
    assert s["zero_pub_s"] == pytest.approx(0.2)
    assert s["gap_share"] == pytest.approx(0.01)
    assert s["consume_p50_ms"] == pytest.approx(4.0)
    assert s["lat_p99"] == pytest.approx(10.0)
    assert s["valid"] is True


def test_ack_counting_uses_ack_timer_not_published() -> None:
    idle_flush = _step(W0 + 0.1, ack=0.001)
    idle_flush[FIELDS.index("published")] = 1
    withheld = _step(W0 + 0.2)
    run = _run(acks=4, muts_per_ack=3750, extra_steps=[idle_flush, withheld])
    s = gt.summarize(run)
    assert s["acks_s"] == pytest.approx(0.5)
    assert s["ack_ckpt_share"] == pytest.approx(5 * 0.002 / 10 - 0.001 / 10)


def test_latency_percentiles_are_nearest_rank() -> None:
    s = gt.summarize(_run(lat=[float(v) for v in range(1, 101)]))
    assert (s["lat_p50"], s["lat_p90"], s["lat_p99"]) == (50.0, 90.0, 99.0)


def test_undrained_or_short_delivery_is_invalid() -> None:
    assert gt.summarize(_run(drained=False))["valid"] is False
    assert "drained" in gt.summarize(_run(drained=False))["why"]
    short = gt.summarize(_run(rate=2000.0))  # 1500 delivered vs 2000 offered
    assert short["valid"] is False and "delivered" in short["why"]
    assert gt.summarize(_run(rate=1520.0))["valid"] is True  # 1.3% off


def test_low_rate_tolerance_allows_two_writer_batches() -> None:
    # 100 mut/s, 500-mut txns: one txn more is +50 % in a 10 s window.
    run = _run(acks=3, muts_per_ack=500, rate=100.0)
    assert gt.summarize(run)["valid"] is True


def _sums(p50s: list[float], p99s: list[float], acks: float = 10.0) -> list[dict]:
    return [
        {"lat_p50": a, "lat_p99": b, "acks_s": acks, "valid": True}
        for a, b in zip(p50s, p99s, strict=True)
    ]


def test_neutral_rule_edges_are_inclusive() -> None:
    off = _sums([20, 22, 24], [70, 75, 80])
    assert gt.is_neutral(_sums([20, 20, 24], [80, 80, 70]), off)
    assert gt.is_neutral(_sums([24, 24, 24], [70, 70, 70]), off)


def test_neutral_rule_fails_if_either_median_leaves_range() -> None:
    off = _sums([20, 22, 24], [70, 75, 80])
    assert not gt.is_neutral(_sums([25, 25, 25], [75, 75, 75]), off)
    assert not gt.is_neutral(_sums([19.9, 19.9, 30], [75, 75, 75]), off)
    assert not gt.is_neutral(_sums([22, 22, 22], [80.1, 81, 60]), off)


def _cells(**cells: list[dict]) -> dict[str, list[dict]]:
    return dict(cells)


def test_verdict_picks_biggest_ack_cut_among_neutral_cells() -> None:
    off = _sums([20, 22, 24], [70, 75, 80], acks=100)
    v = gt.verdict(
        _cells(
            off=off,
            R2=_sums([21, 21, 21], [72, 72, 72], acks=50),
            R4=_sums([23, 23, 23], [78, 78, 78], acks=25),
            R10=_sums([40, 40, 40], [90, 90, 90], acks=10),  # biggest cut, not neutral
        ),
        min_runs=3,
    )
    assert v["form"] == "neutral"
    assert v["cell"] == "R4"
    assert v["ratio"] == pytest.approx(4.0)
    assert v["neutral"] == ["R2", "R4"]


def test_verdict_without_neutral_cell_is_trade_off_with_p50_cost() -> None:
    off = _sums([20, 22, 24], [70, 75, 80], acks=100)
    v = gt.verdict(
        _cells(
            off=off,
            R4=_sums([30, 30, 30], [85, 85, 85], acks=25),
            R10=_sums([40, 40, 40], [90, 90, 90], acks=10),
        ),
        min_runs=3,
    )
    assert v["form"] == "trade-off"
    assert v["cell"] == "R10"
    assert v["ratio"] == pytest.approx(10.0)
    assert v["d_p50_ms"] == pytest.approx(18.0)


def test_verdict_ignores_control_and_low_load_cells_and_invalid_runs() -> None:
    off = _sums([20, 22, 24], [70, 75, 80], acks=100)
    bad = _sums([21, 21, 21], [72, 72, 72], acks=1)
    for s in bad:
        s["valid"] = False
    v = gt.verdict(
        _cells(
            off=off,
            R2=_sums([21, 21, 21], [72, 72, 72], acks=50) + bad,
            **{
                "ctl-R10": _sums([21, 21, 21], [72, 72, 72], acks=1),
                "low-R2": _sums([21, 21, 21], [72, 72, 72], acks=1),
            },
        ),
        min_runs=3,
    )
    assert v["cell"] == "R2"
    assert v["ratio"] == pytest.approx(2.0)


def test_verdict_warns_on_wide_off_p99_and_too_few_runs() -> None:
    v = gt.verdict(
        _cells(
            off=_sums([20, 22, 24], [40, 75, 81], acks=100),
            R2=_sums([21, 21], [72, 72], acks=50),
        ),
        min_runs=3,
    )
    assert any("120" in w for w in v["warnings"])
    assert any("R2" in w for w in v["warnings"])


def test_cell_name_comes_from_file_name() -> None:
    assert gt.cell_of(Path("raw/S0.05_3.json")) == "S0.05"
    assert gt.cell_of(Path("raw/ctl-R10_1.json")) == "ctl-R10"


def test_main_renders_table_and_check_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    for i in range(3):
        (tmp_path / f"off_{i}.json").write_text(json.dumps(_run()))
        (tmp_path / f"R10_{i}.json").write_text(
            json.dumps(_run(acks=2, muts_per_ack=7500))
        )
    files = sorted(str(p) for p in tmp_path.glob("*.json"))

    assert gt.main([*files, "--min-runs", "3"]) == 0
    out = capsys.readouterr().out
    assert "| R10 |" in out and "5.00x" in out and "neutral" in out

    bad = tmp_path / "bad_1.json"
    bad.write_text(json.dumps(_run(drained=False)))
    assert gt.main(["--check", str(bad)]) == 1
    assert gt.main(["--check", files[0]]) == 0
