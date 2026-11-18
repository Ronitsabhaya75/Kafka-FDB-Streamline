"""Unit tests for the pure parts of bridge_tput.py and p9_table.py (no FDB/Kafka)."""

# ruff: noqa: D103  # test names are the docs, as under tests/ (pyproject ignores D there)

import bridge_tput as bt
import p9_table
import pytest


def test_value_for_is_exact_size_and_round_trips_timestamp() -> None:
    v = bt.value_for(1_700_000_000_123_456_789, 100)
    assert len(v) == 100
    assert int(v[:19]) == 1_700_000_000_123_456_789 and v[19:20] == b"|"
    assert len(bt.value_for(5, 20)) == 20
    with pytest.raises(ValueError):
        bt.value_for(5, 19)


def test_ols_slope() -> None:
    assert bt.ols_slope([0, 1, 2, 3], [1, 3, 5, 7]) == pytest.approx(2.0)
    assert bt.ols_slope([0, 1, 2], [4, 4, 4]) == 0.0
    with pytest.raises(ValueError):
        bt.ols_slope([1, 1], [0, 1])


def test_backlog_growing_boundary() -> None:
    assert not bt.backlog_growing(100.0, 10_000, published=1)
    assert bt.backlog_growing(100.001, 10_000, published=1)
    assert bt.backlog_growing(-5.0, 10_000, published=0)
    assert not bt.backlog_growing(0.005, 0, published=1)


def test_mid_window_rate_ignores_ramp_and_tail() -> None:
    # 1 s idle ramp, then 1000/s for 10 s, then a flat tail.
    samples = [(t / 10, 0.0) for t in range(10)]
    samples += [(1 + t / 10, 100.0 * t) for t in range(101)]
    samples += [(11 + t / 10, 10_000.0) for t in range(1, 10)]
    assert bt.mid_window_rate(samples) == pytest.approx(1000.0)


def test_mid_window_rejects_degenerate_windows() -> None:
    with pytest.raises(ValueError):
        bt.mid_window([(0.0, 0.0), (1.0, 0.0)])
    with pytest.raises(ValueError):
        bt.mid_window([(0.0, 0.0), (1.0, 100.0)])


def test_build_reply_matches_drain_shape() -> None:
    reply = bt.build_reply(6, 500, stream_name="p9-test")
    ends = [r for r in reply if r.HasField("version_end")]
    assert len(ends) == 6
    assert bt.reply_mutations(reply) == 3000
    cap = bt.max_record_bytes_for_key(b"p9-test")
    assert all(r.ByteSize() <= cap for r in reply)
    versions = [r.version_end.fdb_version for r in ends]
    assert versions == sorted(set(versions))


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, dict]] = []

    def produce(self, topic: str, record: object, **kwargs: object) -> None:
        self.calls.append((topic, record, kwargs))


class _Sized:
    def __init__(self, n: int) -> None:
        self.n = n

    def ByteSize(self) -> int:  # noqa: N802 - protobuf's name
        return self.n


def test_byte_counter_sums_bytesize_and_forwards() -> None:
    fake = _FakeProducer()
    counter = bt.ByteCounter(fake)
    fake.produce("t", _Sized(10), key=b"k")
    fake.produce("t", _Sized(32))
    assert counter.bytes == 42
    assert [c[2] for c in fake.calls] == [{"key": b"k"}, {}]


def test_stats_sampler_rate_between() -> None:
    s = bt.StatsSampler()
    s.samples = [(0.0, 0), (1.0, 500), (2.0, 1000), (9.0, 99_999)]
    assert s.rate_between(0.0, 2.0) == pytest.approx(500.0)
    assert s.rate_between(5.0, 6.0) is None


def test_summarize_lag() -> None:
    assert bt.summarize_lag([])["lag_max_versions"] is None
    out = bt.summarize_lag([1000, 3000])
    assert out["lag_mean_versions"] == 2000 and out["lag_max_ms_approx"] == 3.0


def test_parse_args_mode_defaults() -> None:
    a = bt.parse_args(["kafka-alone", "--bootstrap", "b:1", "--out", "o.json"])
    assert a.duration == 30.0 and a.byte_counter
    a = bt.parse_args(["sustained", "--bootstrap", "b:1", "--out", "o", "--rate", "5"])
    assert a.duration == 60.0 and a.rate == 5.0


# --- p9_table ---------------------------------------------------------------


def _run(mode: str, mps: float, **kw: object) -> dict:
    return {"mode": mode, "mutations_per_s": mps, "drained": True, **kw}


def test_med_range() -> None:
    assert p9_table.med_range([3.0, 1.0, 2.0]) == (2.0, 1.0, 3.0)


def test_table_percent_of_raw_sink_from_medians() -> None:
    runs = [_run("drain", v) for v in (50, 60, 70)] + [
        _run("kafka-alone", v, flush_at_end=False) for v in (90, 100, 110)
    ]
    out = p9_table.summarize(runs)
    assert out["drain_pct_of_raw"] == pytest.approx(60.0)


def test_undrained_runs_are_excluded() -> None:
    runs = [_run("drain", 10), _run("drain", 1000, drained=False)]
    assert p9_table.summarize(runs)["groups"]["drain"]["n"] == 1


def test_highest_flat_rate_stops_at_first_growing_rate() -> None:
    def s(rate: float, growing: bool) -> dict:
        return _run("sustained", rate, rate=rate, backlog_growing=growing)

    runs = [s(15000, False)] * 5
    runs += [s(25000, True)] * 2 + [s(25000, False)] * 3
    runs += [s(35000, True)] * 3 + [s(35000, False)] * 2
    runs += [s(45000, False)] * 5
    assert p9_table.highest_flat_rate(runs) == 25000
    assert p9_table.highest_flat_rate([s(15000, True)] * 3) is None
