"""Real end-to-end benchmark for Kafka-FDB-Streamline.

Runs a concurrent FDB write workload against the live bridge loop
(poll CDC -> produce -> flush -> acknowledge) and measures what actually
happens, with no simulated latencies.

What it measures, over a steady-state window (after warm-up):
  * FDB write rate achieved by the load generator.
  * Bridge rate (mutations/s and MB/s) acknowledged back to FDB.
  * Backlog trend (written - acked): flags a bridge that cannot keep up.
  * Client-write -> Kafka-ack latency percentiles (includes FDB commit time).
  * Mean time per bridge stage: CDC poll, produce, flush, FDB ack.
  * CDC lag in versions (approximate seconds shown as well).
  * Optional integrity check (--verify): duplicates, gaps, ordering.

Run inside the dev container, e.g.:
    PYTHONPATH=/workspace:/workspace/protobuf/gen python3 benchmarks/bench_e2e.py \
        --kafka-bootstrap-servers kafka:19092 --duration 60 --warmup 10 \
        --target-rate 20000 --output results_20000.json

To find the sustainable rate, sweep --target-rate upward and look for the
highest value where the backlog stays flat. An unthrottled writer
(--target-rate 0) will outrun the bridge and pin T-log history, so keep those
runs short on shared clusters.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import os
import platform
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

# Ensure repo root and protobuf/gen are discoverable
_ROOT = Path(__file__).resolve().parent.parent
_PROTO_DIR = _ROOT / "protobuf" / "gen"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_PROTO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROTO_DIR))

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TS_WIDTH = 19  # digits of time.time_ns() embedded in each value
VERSIONS_PER_SECOND = 1_000_000  # FDB default; used only for approximate lag


def percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile of an already sorted list.

    Args:
        sorted_values: Ascending list of measured floats.
        pct: Percentile between 0 and 100.

    Returns:
        The nearest-rank percentile value, or NaN if the list is empty.
    """
    if not sorted_values:
        return float("nan")
    rank = max(
        0, min(len(sorted_values) - 1, math.ceil(pct / 100 * len(sorted_values)) - 1)
    )
    return sorted_values[rank]


def slope(xs: list[float], ys: list[float]) -> float:
    """Least-squares slope of ys over xs (0.0 if undefined).

    Args:
        xs: Independent variable samples.
        ys: Dependent variable samples.

    Returns:
        Calculated linear regression slope.
    """
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    numer = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
    return numer / denom


def value_at(times: list[float], values: list[float], t: float) -> float:
    """Value of a cumulative series at time t (last sample at or before t).

    Args:
        times: Monotonically increasing list of timestamps.
        values: Measured cumulative values at corresponding timestamps.
        t: Target lookup timestamp.

    Returns:
        Interpolated cumulative value at time t.
    """
    i = bisect.bisect_right(times, t) - 1
    return values[i] if i >= 0 else 0


class Writer(threading.Thread):
    """Commits batches of SET mutations, optionally rate limited."""

    def __init__(
        self,
        database: Any,
        prefix: bytes,
        batch_size: int,
        value_size: int,
        target_rate: float,
        key_pool: int = 500_000,
    ) -> None:
        """Initialize the background writer thread.

        Args:
            database: FDB database connection handle.
            prefix: Key subspace prefix for benchmark keys.
            batch_size: Number of mutations per FDB transaction.
            value_size: Total payload bytes per mutation value.
            target_rate: Target write rate in mutations/s (0 = unthrottled).
            key_pool: Size of distinct key pool to cycle over (0 = unbounded).
        """
        super().__init__(daemon=True)
        self.database = database
        self.prefix = prefix
        self.batch_size = batch_size
        self.pad = b"x" * max(0, value_size - (TS_WIDTH + 1))
        self.target_rate = target_rate
        self.key_pool = key_pool
        self.stop_event = threading.Event()
        self.written = 0
        self.last_committed_version = 0
        self.behind_batches = 0
        self.error: BaseException | None = None
        self.log_t: list[float] = []
        self.log_written: list[float] = []

    def run(self) -> None:
        """Commit mutation batches until stop_event is signaled."""
        interval = self.batch_size / self.target_rate if self.target_rate > 0 else 0.0
        deadline = time.monotonic()
        next_idx = 0
        try:
            while not self.stop_event.is_set():
                tr = self.database.create_transaction()
                value = f"{time.time_ns():0{TS_WIDTH}d}|".encode() + self.pad
                for i in range(self.batch_size):
                    k = (
                        (next_idx + i) % self.key_pool
                        if self.key_pool > 0
                        else (next_idx + i)
                    )
                    tr[self.prefix + f"{k:010d}".encode()] = value
                tr.commit().wait()
                self.last_committed_version = tr.get_committed_version()
                next_idx += self.batch_size
                self.written = next_idx
                self.log_t.append(time.monotonic())
                self.log_written.append(next_idx)
                if interval:
                    deadline += interval
                    pause = deadline - time.monotonic()
                    if pause > 0:
                        self.stop_event.wait(pause)
                    else:
                        self.behind_batches += 1
        except BaseException as exc:  # noqa: BLE001 - surfaced by main thread
            self.error = exc


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    """Run the benchmark and return a results dictionary.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Structured dictionary containing benchmark metrics and environment info.
    """
    # Imported lazily so the helpers above can be unit tested without FDB.
    from fdbkafka.cdc.v1 import mutations_pb2  # noqa: F401
    from src.cdc import FDBClient, FDBMutationListener, strinc
    from src.kafka import MutationProducer

    run_id = uuid.uuid4().hex[:10]
    stream_name = f"bench-{run_id}"
    topic = f"fdb-cdc-bench-{run_id}"
    prefix = f"bench/{run_id}/".encode()
    key = stream_name.encode()

    client = FDBClient(api_version=800)
    database = client.open()
    listener = FDBMutationListener(
        client, stream_name, key_range=(prefix, strinc(prefix))
    )
    producer = MutationProducer(args.kafka_bootstrap_servers)
    writer = Writer(
        database,
        prefix,
        args.batch_size,
        args.value_size,
        args.target_rate,
        key_pool=args.key_pool,
    )

    # Per-step logs (one entry per non-empty poll that was fully acknowledged)
    step_t: list[float] = []
    step_acked: list[float] = []
    step_bytes: list[float] = []
    stage = {"poll": [], "produce": [], "flush": [], "ack": []}
    stage_t: list[float] = []
    lat_t: list[float] = []
    lat_ms: list[float] = []
    lag_t: list[float] = []
    lag_versions: list[float] = []

    acked = 0
    wire_bytes = 0
    idle_poll_s = 0.0
    seen: set[int] = set()
    duplicates = 0
    order_violations = 0
    last_index: tuple[int, int] = (-1, -1)
    drained = False
    drain_s: float | None = None
    registered = False

    try:
        listener.start()
        registered = True
        t0 = time.monotonic()
        warm_end = t0 + args.warmup
        write_end = warm_end + args.duration
        writer.start()
        stop_requested = False
        drain_target = 0
        drain_deadline = 0.0
        last_heartbeat = t0
        total_time = args.warmup + args.duration

        while True:
            now = time.monotonic()
            if now - last_heartbeat >= 15.0:
                elapsed = now - t0
                pct = min(100.0, (elapsed / total_time) * 100.0)
                if now < warm_end:
                    phase = "warmup"
                elif not stop_requested:
                    phase = "running"
                else:
                    phase = "draining"
                print(
                    f"[{elapsed:05.1f}s / {total_time:05.1f}s ({pct:4.1f}%)] "
                    f"phase={phase:<8} written={writer.written:>9,} "
                    f"acked={acked:>9,} backlog={writer.written - acked:>6,}"
                )
                last_heartbeat = now
            if writer.error is not None:
                raise RuntimeError(f"writer failed: {writer.error!r}")
            if not stop_requested and now >= write_end:
                writer.stop_event.set()
                writer.join(timeout=60)
                drain_target = writer.written
                drain_deadline = time.monotonic() + args.drain_timeout
                stop_requested = True
            if stop_requested:
                if acked >= drain_target:
                    drained = True
                    drain_s = time.monotonic() - write_end
                    break
                if time.monotonic() > drain_deadline:
                    break

            p0 = time.perf_counter()
            records = listener.poll_records()
            p1 = time.perf_counter()
            if not records:
                idle_poll_s += p1 - p0
                continue

            for record in records:
                producer.produce(topic, record, key=key)
            p2 = time.perf_counter()
            if producer.flush(args.flush_timeout) != 0:
                raise TimeoutError("Kafka did not acknowledge every queued record")
            p3 = time.perf_counter()
            flush_done_ns = time.time_ns()
            listener.acknowledge()
            p4 = time.perf_counter()

            batch_mutations = 0
            latency_sample: float | None = None
            for record in records:
                wire_bytes += record.ByteSize()
                if not record.HasField("batch"):
                    continue
                mutations = record.batch.mutations
                batch_mutations += len(mutations)
                if (
                    len(mutations)
                    and mutations[-1].WhichOneof("mutation") == "single_key_mutation"
                ):
                    val = mutations[-1].single_key_mutation.value
                    if len(val) > TS_WIDTH and val[TS_WIDTH : TS_WIDTH + 1] == b"|":
                        latency_sample = (flush_done_ns - int(val[:TS_WIDTH])) / 1e6
                if args.verify:
                    for m in mutations:
                        vi = (
                            m.version_index.fdb_version,
                            m.version_index.sequence_no,
                        )
                        if vi <= last_index:
                            order_violations += 1
                        last_index = vi
                        if m.WhichOneof("mutation") == "single_key_mutation":
                            idx = int(m.single_key_mutation.key[len(prefix) :])
                            if idx in seen:
                                duplicates += 1
                            seen.add(idx)
            acked += batch_mutations

            t_now = time.monotonic()
            step_t.append(t_now)
            step_acked.append(acked)
            step_bytes.append(wire_bytes)
            stage_t.append(t_now)
            stage["poll"].append((p1 - p0) * 1e3)
            stage["produce"].append((p2 - p1) * 1e3)
            stage["flush"].append((p3 - p2) * 1e3)
            stage["ack"].append((p4 - p3) * 1e3)
            if latency_sample is not None:
                lat_t.append(t_now)
                lat_ms.append(latency_sample)
            try:
                consumed = listener.get_position().last_consumed_version
                lag_t.append(t_now)
                lag_versions.append(max(0, writer.last_committed_version - consumed))
            except AttributeError:
                pass  # position API differs; lag is then omitted

        # ---- summarize the steady-state window -------------------------
        win_start, win_end = warm_end, write_end
        window_s = win_end - win_start
        acked_in = value_at(step_t, step_acked, win_end) - value_at(
            step_t, step_acked, win_start
        )
        bytes_in = value_at(step_t, step_bytes, win_end) - value_at(
            step_t, step_bytes, win_start
        )
        written_in = value_at(writer.log_t, writer.log_written, win_end) - value_at(
            writer.log_t, writer.log_written, win_start
        )

        backlog_x, backlog_y = [], []
        for t, a in zip(step_t, step_acked, strict=False):
            if win_start <= t <= win_end:
                backlog_x.append(t - win_start)
                backlog_y.append(value_at(writer.log_t, writer.log_written, t) - a)
        backlog_slope = slope(backlog_x, backlog_y)
        write_rate = written_in / window_s
        bridge_rate = acked_in / window_s
        backlog_growing = backlog_slope > 0.01 * max(write_rate, 1.0)

        def window_vals(ts: list[float], vals: list[float]) -> list[float]:
            return [
                v for t, v in zip(ts, vals, strict=False) if win_start <= t <= win_end
            ]

        lats = sorted(window_vals(lat_t, lat_ms))
        lags = window_vals(lag_t, lag_versions)
        stage_means = {
            name: (
                sum(window_vals(stage_t, vals))
                / max(1, len(window_vals(stage_t, vals)))
            )
            for name, vals in stage.items()
        }

        results: dict[str, Any] = {
            "config": {
                "duration_s": args.duration,
                "warmup_s": args.warmup,
                "batch_size": args.batch_size,
                "value_size": args.value_size,
                "target_rate": args.target_rate,
                "verify": args.verify,
            },
            "environment": {
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "python": platform.python_version(),
            },
            "write_rate_per_s": write_rate,
            "writer_behind_batches": writer.behind_batches,
            "bridge_rate_per_s": bridge_rate,
            "bridge_mb_per_s": bytes_in / 1024 / 1024 / window_s,
            "backlog_slope_per_s": backlog_slope,
            "backlog_growing": backlog_growing,
            "drained": drained,
            "drain_s": drain_s,
            "total_written": writer.written,
            "total_acked": acked,
            "idle_poll_s_total": idle_poll_s,
            "stage_mean_ms": stage_means,
            "latency_ms": {
                "samples": len(lats),
                "p50": percentile(lats, 50),
                "p90": percentile(lats, 90),
                "p99": percentile(lats, 99),
                "max": lats[-1] if lats else float("nan"),
            },
            "lag_versions": {
                "mean": sum(lags) / len(lags) if lags else None,
                "max": max(lags) if lags else None,
                "approx_max_seconds": (
                    (max(lags) / VERSIONS_PER_SECOND) if lags else None
                ),
            },
        }
        if args.verify:
            results["integrity"] = {
                "unique_keys_seen": len(seen),
                "duplicates": duplicates,
                "missing": max(0, writer.written - len(seen)) if drained else None,
                "order_violations": order_violations,
            }
        if args.output:
            with open(args.output, "w", encoding="utf-8") as fh:
                json.dump(results, fh, indent=2)
        return results
    finally:
        writer.stop_event.set()
        listener.close()
        try:
            tr = database.create_transaction()
            if hasattr(tr, "options") and hasattr(tr.options, "set_timeout"):
                tr.options.set_timeout(5000)
            del tr[prefix : strinc(prefix)]
            tr.commit().wait()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        if registered:
            try:
                client.remove_cdc_stream(stream_name)
            except Exception:  # noqa: BLE001
                pass
        client.close()


def run_bridge_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    """Run benchmark using the full autonomous Bridge daemon with cursor fsync.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Structured dictionary containing benchmark metrics and environment info.
    """
    from src.bridge.config import from_dict
    from src.bridge.pipeline import Bridge
    from src.cdc import FDBClient, strinc

    run_id = uuid.uuid4().hex[:10]
    stream_name = f"bench-{run_id}"
    topic = f"fdb-cdc-bench-{run_id}"
    prefix = f"bench/{run_id}/".encode()
    cursor_file = args.cursor_file or f"/tmp/bench_cursor_{run_id}.json"

    client = FDBClient(api_version=800)
    database = client.open()
    writer = Writer(
        database,
        prefix,
        args.batch_size,
        args.value_size,
        args.target_rate,
        key_pool=args.key_pool,
    )

    config = from_dict(
        {
            "fdb": {"api_version": 800},
            "cdc": {
                "stream_name": stream_name,
                "begin_key": prefix,
                "end_key": strinc(prefix),
                "cursor_file": cursor_file,
            },
            "kafka": {
                "bootstrap_servers": args.kafka_bootstrap_servers,
                "topic": topic,
            },
            "pipeline": {
                "flush_timeout_seconds": args.flush_timeout,
            },
        }
    )

    bridge = Bridge(config, client=client)
    bridge.start()

    step_t: list[float] = []
    step_acked: list[float] = []
    lag_t: list[float] = []
    lag_versions: list[float] = []

    drained = False
    drain_s: float | None = None
    registered = True
    bridge_thread = threading.Thread(target=bridge.run, daemon=True)

    try:
        t0 = time.monotonic()
        warm_end = t0 + args.warmup
        write_end = warm_end + args.duration
        last_heartbeat = t0
        total_time = args.warmup + args.duration

        writer.start()
        bridge_thread.start()
        stop_requested = False
        drain_target = 0
        drain_deadline = 0.0

        while True:
            now = time.monotonic()
            acked = bridge.metrics.mutations_published
            step_t.append(now)
            step_acked.append(acked)

            try:
                if bridge._listener is not None:
                    consumed = bridge._listener.get_position().last_consumed_version
                    lag_t.append(now)
                    lag_versions.append(
                        max(0, writer.last_committed_version - consumed)
                    )
            except Exception:
                pass

            if now - last_heartbeat >= 15.0:
                elapsed = now - t0
                pct = min(100.0, (elapsed / total_time) * 100.0)
                if now < warm_end:
                    phase = "warmup"
                elif not stop_requested:
                    phase = "running"
                else:
                    phase = "draining"
                print(
                    f"[{elapsed:05.1f}s / {total_time:05.1f}s ({pct:4.1f}%)] "
                    f"phase={phase:<8} written={writer.written:>9,} "
                    f"acked={acked:>9,} backlog={writer.written - acked:>6,} "
                    f"batches_fsynced={bridge.metrics.batches_acked:>6,}"
                )
                last_heartbeat = now

            if writer.error is not None:
                raise RuntimeError(f"writer failed: {writer.error!r}")

            if not stop_requested and now >= write_end:
                writer.stop_event.set()
                writer.join(timeout=10)
                drain_target = writer.written
                drain_deadline = time.monotonic() + args.drain_timeout
                stop_requested = True

            if stop_requested:
                if acked >= drain_target:
                    drained = True
                    drain_s = time.monotonic() - write_end
                    break
                if time.monotonic() > drain_deadline:
                    break

            time.sleep(0.05)

        bridge.stop()
        bridge_thread.join(timeout=15)

        cursor_data: dict[str, Any] = {}
        if os.path.exists(cursor_file):
            try:
                with open(cursor_file, encoding="utf-8") as f:
                    cursor_data = json.load(f)
            except Exception:
                pass

        win_start, win_end = warm_end, write_end
        window_s = win_end - win_start
        acked_in = value_at(step_t, step_acked, win_end) - value_at(
            step_t, step_acked, win_start
        )
        written_in = value_at(writer.log_t, writer.log_written, win_end) - value_at(
            writer.log_t, writer.log_written, win_start
        )

        backlog_x, backlog_y = [], []
        for t, a in zip(step_t, step_acked, strict=False):
            if win_start <= t <= win_end:
                backlog_x.append(t - win_start)
                backlog_y.append(value_at(writer.log_t, writer.log_written, t) - a)
        backlog_slope = slope(backlog_x, backlog_y)
        write_rate = written_in / window_s
        bridge_rate = acked_in / window_s
        backlog_growing = backlog_slope > 0.01 * max(write_rate, 1.0)

        def window_vals(ts: list[float], vals: list[float]) -> list[float]:
            return [
                v for t, v in zip(ts, vals, strict=False) if win_start <= t <= win_end
            ]

        lags = window_vals(lag_t, lag_versions)

        results: dict[str, Any] = {
            "mode": "autonomous_bridge",
            "config": {
                "duration_s": args.duration,
                "warmup_s": args.warmup,
                "batch_size": args.batch_size,
                "value_size": args.value_size,
                "target_rate": args.target_rate,
                "cursor_file": cursor_file,
            },
            "environment": {
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "python": platform.python_version(),
            },
            "write_rate_per_s": write_rate,
            "bridge_rate_per_s": bridge_rate,
            "backlog_slope_per_s": backlog_slope,
            "backlog_growing": backlog_growing,
            "drained": drained,
            "drain_s": drain_s,
            "total_written": writer.written,
            "total_acked": bridge.metrics.mutations_published,
            "batches_fsynced_to_disk": bridge.metrics.batches_acked,
            "errors_retried": bridge.metrics.errors_retried,
            "lag_versions": {
                "mean": sum(lags) / len(lags) if lags else None,
                "max": max(lags) if lags else None,
                "approx_max_seconds": (
                    (max(lags) / VERSIONS_PER_SECOND) if lags else None
                ),
            },
            "cursor_checkpoint": cursor_data,
        }
        if args.output:
            with open(args.output, "w", encoding="utf-8") as fh:
                json.dump(results, fh, indent=2)
        return results
    finally:
        writer.stop_event.set()
        try:
            bridge.stop()
        except Exception:
            pass
        try:
            tr = database.create_transaction()
            if hasattr(tr, "options") and hasattr(tr.options, "set_timeout"):
                tr.options.set_timeout(5000)
            del tr[prefix : strinc(prefix)]
            tr.commit().wait()
        except Exception:
            pass
        if registered:
            try:
                client.remove_cdc_stream(stream_name)
            except Exception:
                pass
        client.close()
        if not args.cursor_file and os.path.exists(cursor_file):
            try:
                os.remove(cursor_file)
            except Exception:
                pass


def print_report(r: dict[str, Any]) -> None:
    """Print a human-readable summary.

    Args:
        r: Dictionary of benchmark results.
    """
    bar = "=" * 72
    mode = r.get("mode", "core_pipeline")
    title = (
        "  KAFKA-FDB-STREAMLINE  OFFICIAL RELEASE BENCHMARK (Bridge Daemon)\n"
        if mode == "autonomous_bridge"
        else "  KAFKA-FDB-STREAMLINE  CORE PIPELINE BENCHMARK (measured)\n"
    )
    print(f"\n{bar}\n{title}{bar}")
    c = r["config"]
    print(
        f"window={c['duration_s']}s warmup={c['warmup_s']}s batch={c['batch_size']} "
        f"value={c['value_size']}B target_rate={c['target_rate'] or 'unthrottled'}"
    )
    if mode == "autonomous_bridge":
        print("Architecture            : Full Bridge daemon with atomic cursor fsync")
        print(f"Cursor checkpoint file  : {c.get('cursor_file')}")
        print(f"Batches fsynced to disk : {r.get('batches_fsynced_to_disk', 0):,}")
        print(f"Final durable checkpoint: {r.get('cursor_checkpoint')}")
        print(f"Errors retried          : {r.get('errors_retried', 0)}")
    print(f"FDB write rate achieved : {r['write_rate_per_s']:,.0f} mutations/s")
    if r.get("writer_behind_batches"):
        print(
            f"  (writer fell behind its target on {r['writer_behind_batches']} batches)"
        )
    print(f"Bridge rate (acked)     : {r['bridge_rate_per_s']:,.0f} mutations/s")
    if "bridge_mb_per_s" in r:
        print(f"Bridge wire bandwidth   : {r['bridge_mb_per_s']:.2f} MB/s")
    verdict = (
        "GROWING - bridge cannot keep up"
        if r["backlog_growing"]
        else "flat - sustainable"
    )
    slope_str = f"{r['backlog_slope_per_s']:+,.0f} mutations/s ({verdict})"
    print(f"Backlog trend           : {slope_str}")
    drain = f"{r['drain_s']:.1f}s" if r["drained"] else "NOT drained before timeout"
    print(f"Drain after writes stop : {drain}")
    print(
        f"Totals                  : written={r['total_written']:,} "
        f"acked={r['total_acked']:,}"
    )
    if "stage_mean_ms" in r:
        print("-" * 72)
        print("Mean time per bridge step (ms):")
        for name, ms in r["stage_mean_ms"].items():
            print(f"  {name:<8}: {ms:8.2f}")
        print(f"  idle CDC polls (whole run): {r.get('idle_poll_s_total', 0.0):.1f}s")
    if "latency_ms" in r:
        lat = r["latency_ms"]
        print("-" * 72)
        print(
            f"Client write -> Kafka ack latency (n={lat['samples']}): "
            f"p50={lat['p50']:.1f}ms p90={lat['p90']:.1f}ms "
            f"p99={lat['p99']:.1f}ms max={lat['max']:.1f}ms"
        )
    lag = r.get("lag_versions", {})
    if lag.get("max") is not None:
        print("-" * 72)
        print(
            f"CDC lag: mean={lag['mean']:,.0f} max={lag['max']:,.0f} versions "
            f"(~{lag['approx_max_seconds']:.2f}s at default version rate)"
        )
    if "integrity" in r:
        i = r["integrity"]
        print("-" * 72)
        print(
            f"Integrity: unique={i['unique_keys_seen']:,} duplicates={i['duplicates']} "
            f"missing={i['missing']} order_violations={i['order_violations']}"
        )
    print(bar)


def main() -> int:
    """Parse arguments, run, report.

    Returns:
        Process exit code (0 for success).
    """
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    p.add_argument("--kafka-bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--duration", type=float, default=60.0, help="measured window (s)")
    p.add_argument("--warmup", type=float, default=10.0, help="discarded warm-up (s)")
    p.add_argument(
        "--batch-size", type=int, default=500, help="mutations per FDB transaction"
    )
    p.add_argument(
        "--value-size", type=int, default=100, help="bytes per value (>= 20)"
    )
    p.add_argument(
        "--target-rate", type=float, default=0.0, help="writes/s, 0 = unthrottled"
    )
    p.add_argument(
        "--key-pool",
        type=int,
        default=500_000,
        help="number of distinct keys to cycle across (0 = unbounded)",
    )
    p.add_argument(
        "--use-bridge",
        action="store_true",
        help="run official release benchmark with autonomous Bridge and atomic fsync",
    )
    p.add_argument(
        "--cursor-file",
        default=None,
        help="optional explicit path to cursor checkpoint file",
    )
    p.add_argument("--flush-timeout", type=float, default=30.0)
    p.add_argument("--drain-timeout", type=float, default=120.0)
    p.add_argument(
        "--verify", action="store_true", help="check dupes/gaps/order (slows bridge)"
    )
    p.add_argument("--output", help="write results JSON to this path")
    args = p.parse_args()

    if args.use_bridge:
        results = run_bridge_benchmark(args)
    else:
        results = run_benchmark(args)
    print_report(results)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)
        print(f"Results written to {args.output}")
    return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
