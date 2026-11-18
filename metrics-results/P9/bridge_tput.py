"""Bridge throughput harness: FDB -> Kafka mutations/s and bytes/s.

Modes:
    drain: preload N mutations, then time the bridge draining them.
    sustained: writer and bridge run together at a fixed write rate.
    kafka-alone: replay one prebuilt reply into Kafka, no FDB (raw-sink ceiling).

Runs inside the FDB container (needs the ``fdb`` module) with ``PYTHONPATH`` set
to the repo's ``.:src:protobuf/gen``. Pure helpers import without ``fdb``.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from typing import Any, NamedTuple

import confluent_kafka
from google.protobuf import __version__ as protobuf_version
from google.protobuf.internal import api_implementation

from fdb_kafka_bridge.bridge.config import from_dict
from fdb_kafka_bridge.bridge.pipeline import Bridge
from fdb_kafka_bridge.cdc import FDBClient, strinc
from fdb_kafka_bridge.kafka import MutationProducer
from fdb_kafka_bridge.record_size import max_record_bytes_for_key
from fdb_kafka_bridge.serialization.version_group import serialize_version_group
from fdbkafka.cdc.v1 import mutations_pb2

TS_WIDTH = 19
SAMPLE_SECONDS = 0.05
SET_VALUE = 0


class _Set(NamedTuple):
    type: int
    param1: bytes
    param2: bytes


# --- pure helpers -----------------------------------------------------------


def value_for(ts_ns: int, size: int = 100) -> bytes:
    """Return ``size`` bytes: a 19-digit ``ts_ns``, ``|``, then ``x`` padding.

    Raises:
        ValueError: If ``size`` cannot hold the timestamp and separator.
    """
    head = b"%019d|" % ts_ns
    if size < len(head):
        raise ValueError(f"size must be >= {len(head)}, got {size}")
    return head + b"x" * (size - len(head))


def ols_slope(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Least-squares slope of ``ys`` over ``xs``.

    Raises:
        ValueError: If there are fewer than two distinct ``xs``.
    """
    n = len(xs)
    if n != len(ys):
        raise ValueError("xs and ys differ in length")
    if n < 2:
        raise ValueError("need at least two points")
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        raise ValueError("xs must not all be equal")
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / sxx


def backlog_growing(slope: float, write_rate: float, *, published: int) -> bool:
    """Whether the backlog grows faster than 1% of the write rate.

    A run that published nothing counts as growing.
    """
    return published == 0 or slope > 0.01 * max(write_rate, 1.0)


def mid_window(
    samples: Sequence[tuple[float, float]], lo: float = 0.1, hi: float = 0.9
) -> tuple[int, int]:
    """Indices of the first samples whose count reaches ``lo`` and ``hi`` of the final.

    Raises:
        ValueError: If the final count is zero or both crossings are one sample.
    """
    final = samples[-1][1]
    if final <= 0:
        raise ValueError("final count is zero")
    i_lo = next(i for i, (_, c) in enumerate(samples) if c >= lo * final)
    i_hi = next(i for i, (_, c) in enumerate(samples) if c >= hi * final)
    if i_hi <= i_lo:
        raise ValueError("window spans one sample; sample more often")
    return i_lo, i_hi


def mid_window_rate(
    samples: Sequence[tuple[float, float]], lo: float = 0.1, hi: float = 0.9
) -> float:
    """Rate (count/s) between the ``lo`` and ``hi`` crossings of the final count."""
    i_lo, i_hi = mid_window(samples, lo, hi)
    (t0, c0), (t1, c1) = samples[i_lo], samples[i_hi]
    return (c1 - c0) / (t1 - t0)


def build_reply(
    n_groups: int = 6,
    per_group: int = 500,
    *,
    key_prefix: bytes = b"p9/",
    value_size: int = 100,
    stream_name: str = "p9",
    base_version: int = 1_000_000,
) -> list[mutations_pb2.FDBMutationRecord]:
    """Serialize ``n_groups`` version groups of ``per_group`` SETs, as the bridge would.

    Records are capped by ``max_record_bytes_for_key`` for the stream-name key,
    the cap the listener derives by default.
    """
    cap = max_record_bytes_for_key(stream_name.encode())
    ts = time.time_ns()
    records: list[mutations_pb2.FDBMutationRecord] = []
    for g in range(n_groups):
        value = value_for(ts, value_size)
        group = [
            _Set(SET_VALUE, key_prefix + b"%010d" % (g * per_group + i), value)
            for i in range(per_group)
        ]
        for raw in serialize_version_group(
            group,
            fdb_version=base_version + g,
            max_record_bytes=cap,
            stream_name=stream_name,
            bridge_timestamp_ns=ts,
        ):
            records.append(mutations_pb2.FDBMutationRecord.FromString(raw))
    return records


def reply_mutations(records: Sequence[mutations_pb2.FDBMutationRecord]) -> int:
    """Count the mutations carried in batch records."""
    return sum(len(r.batch.mutations) for r in records if r.HasField("batch"))


class ByteCounter:
    """Wraps a producer's ``produce`` and sums each record's ``ByteSize()``.

    Bytes are the Protobuf payload before compression, without Kafka framing.
    """

    def __init__(self, producer: Any) -> None:
        """Patch ``producer.produce`` on this instance only."""
        self.bytes = 0
        inner = producer.produce

        def produce(topic: str, record: Any, **kwargs: Any) -> None:
            inner(topic, record, **kwargs)
            self.bytes += record.ByteSize()

        producer.produce = produce


class StatsSampler:
    """Collects librdkafka ``txbytes`` (on-wire, post-compression) per stats tick."""

    def __init__(self) -> None:
        """Start with no samples."""
        self.samples: list[tuple[float, int]] = []

    def __call__(self, stats_json: str) -> None:
        """Record one ``(monotonic, txbytes)`` sample."""
        self.samples.append((time.monotonic(), int(json.loads(stats_json)["txbytes"])))

    def rate_between(self, t0: float, t1: float) -> float | None:
        """Slope of ``txbytes`` over samples in ``[t0, t1]``; None if under two."""
        pts = [(t, b) for t, b in self.samples if t0 <= t <= t1]
        if len(pts) < 2:
            return None
        return ols_slope([t for t, _ in pts], [b for _, b in pts])


def summarize_lag(lags: Sequence[int]) -> dict[str, float | None]:
    """Mean and max of lag samples (FDB versions), plus ~ms at 1M versions/s."""
    if not lags:
        return {"lag_mean_versions": None, "lag_max_versions": None}
    return {
        "lag_mean_versions": statistics.fmean(lags),
        "lag_max_versions": max(lags),
        "lag_mean_ms_approx": statistics.fmean(lags) / 1000,
        "lag_max_ms_approx": max(lags) / 1000,
    }


# --- live parts ---------------------------------------------------------------


class Writer(threading.Thread):
    """Commits ``batch`` SETs per transaction under ``prefix``, paced to ``rate``.

    ``rate`` 0 means unthrottled. Stops after ``total`` mutations, or on ``stop``.
    An exception is kept in ``error`` and re-raised by :meth:`join_checked`.
    """

    def __init__(
        self,
        db: Any,
        prefix: bytes,
        *,
        rate: float,
        batch: int = 500,
        total: int | None = None,
        value_size: int = 100,
        key_pool: int = 500_000,
    ) -> None:
        """Configure; call ``start()`` to begin writing."""
        super().__init__(daemon=True)
        self.db, self.prefix, self.rate, self.batch = db, prefix, rate, batch
        self.total, self.value_size, self.key_pool = total, value_size, key_pool
        self.stop_event = threading.Event()
        self.written = 0
        self.last_committed_version = 0
        self.error: BaseException | None = None

    def run(self) -> None:
        """Write until done or stopped."""
        try:
            t0 = time.monotonic()
            while not self.stop_event.is_set():
                n = self.batch
                if self.total is not None:
                    n = min(n, self.total - self.written)
                    if n <= 0:
                        return
                tr = self.db.create_transaction()
                value = value_for(time.time_ns(), self.value_size)
                for k in range(self.written, self.written + n):
                    tr[self.prefix + b"%010d" % (k % self.key_pool)] = value
                tr.commit().wait()
                self.last_committed_version = tr.get_committed_version()
                self.written += n
                if self.rate:
                    ahead = t0 + self.written / self.rate - time.monotonic()
                    if ahead > 0:
                        self.stop_event.wait(ahead)
        except BaseException as exc:
            self.error = exc

    def join_checked(self) -> None:
        """Join, then re-raise any writer error."""
        self.join()
        if self.error is not None:
            raise self.error


def _versions() -> dict[str, Any]:
    return {
        "python": platform.python_version(),
        "machine": platform.machine(),
        "confluent_kafka": confluent_kafka.version()[0],
        "librdkafka": confluent_kafka.libversion()[0],
        "protobuf": protobuf_version,
        "protobuf_backend": api_implementation.Type(),
    }


def _names(mode: str) -> dict[str, Any]:
    run_id = f"p9-{mode}-{uuid.uuid4().hex[:10]}"
    prefix = f"{run_id}/".encode()
    return {
        "run_id": run_id,
        "stream_name": run_id,
        "topic": run_id,
        "prefix": prefix,
        "cursor_file": f"/tmp/{run_id}.cursor.json",
    }


def _producer(bootstrap: str, stats: StatsSampler) -> MutationProducer:
    return MutationProducer(
        bootstrap, extra_config={"statistics.interval.ms": 1000, "stats_cb": stats}
    )


class _Live:
    """A bridge over a fresh stream, topic, key prefix and cursor file."""

    def __init__(self, args: argparse.Namespace, mode: str) -> None:
        self.names = _names(mode)
        self.client = FDBClient()
        self.client.open()
        self.stats = StatsSampler()
        prefix = self.names["prefix"]
        cfg = from_dict(
            {
                "cdc": {
                    "stream_name": self.names["stream_name"],
                    "begin_key": prefix.hex(),
                    "end_key": strinc(prefix).hex(),
                    "cursor_file": self.names["cursor_file"],
                },
                "kafka": {
                    "bootstrap_servers": args.bootstrap,
                    "topic": self.names["topic"],
                },
            }
        )
        self.counter: ByteCounter | None = None

        def make_producer() -> MutationProducer:
            producer = _producer(args.bootstrap, self.stats)
            if args.byte_counter:
                # A rebuild replaces the producer; keep counting into one total.
                prior = self.counter.bytes if self.counter else 0
                self.counter = ByteCounter(producer)
                self.counter.bytes = prior
            return producer

        self.bridge = Bridge(
            cfg,
            client=self.client,
            producer=make_producer(),
            producer_factory=make_producer,
        )
        self.run_error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            self.bridge.run()
        except BaseException as exc:
            self.run_error = exc

    def start_bridge(self) -> None:
        self.bridge.start()

    def run_in_background(self) -> None:
        self._thread.start()

    def published(self) -> int:
        return self.bridge.mutations_published

    def bytes(self) -> int:
        return self.counter.bytes if self.counter else 0

    def check(self) -> None:
        if self.run_error is not None:
            raise self.run_error

    def lag_sample(self, writer: Writer) -> int | None:
        """Writer's committed version minus the listener's last consumed version.

        Sampled only between steps (non-blocking lock), so it reflects the last
        completed step and never stalls the run loop on a parked ``consume()``.
        """
        lock = self.bridge._step_lock
        if not lock.acquire(blocking=False):
            return None
        try:
            listener = self.bridge._listener
            if listener is None:
                return None
            consumed = listener.get_position().last_consumed_version
        finally:
            lock.release()
        return writer.last_committed_version - consumed

    def close(self) -> None:
        """Stop the bridge, remove the stream, clear the prefix and cursor file."""
        errors: list[BaseException] = []
        for step in (
            self.bridge.stop,
            lambda: self._thread.join(timeout=30) if self._thread.is_alive() else None,
            lambda: self.client.remove_cdc_stream(self.names["stream_name"]),
            lambda: self.client.db.clear_range(
                self.names["prefix"], strinc(self.names["prefix"])
            ),
            lambda: (
                os.remove(self.names["cursor_file"])
                if os.path.exists(self.names["cursor_file"])
                else None
            ),
        ):
            try:
                step()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]


def _wait_until(
    pred: Callable[[], bool], timeout: float, check: Callable[[], None]
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        check()
        if pred():
            return True
        time.sleep(SAMPLE_SECONDS)
    return pred()


def run_drain(args: argparse.Namespace) -> dict[str, Any]:
    """Preload, then time the bridge draining it (mid 10-90% window)."""
    live = _Live(args, "drain")
    try:
        live.start_bridge()  # registers the stream before any write
        writer = Writer(
            live.client.db, live.names["prefix"], rate=0, total=args.preload
        )
        t_w = time.monotonic()
        writer.start()
        writer.join_checked()
        write_s = time.monotonic() - t_w

        samples: list[tuple[float, int, int]] = []
        t0 = time.monotonic()
        live.run_in_background()
        deadline = t0 + args.timeout
        while time.monotonic() < deadline:
            live.check()
            samples.append((time.monotonic(), live.published(), live.bytes()))
            if samples[-1][1] >= writer.written:
                break
            time.sleep(SAMPLE_SECONDS)
        live.check()
        published = live.published()
        i_lo, i_hi = mid_window([(t, m) for t, m, _ in samples])
        (ta, ma, ba), (tb, mb, bb) = samples[i_lo], samples[i_hi]
        tx = live.stats.rate_between(ta, tb)
        metrics = live.bridge.metrics
        return {
            **live.names,
            "preload": args.preload,
            "written": writer.written,
            "published": published,
            "drained": published >= writer.written,
            "mutations_per_s": (mb - ma) / (tb - ta),
            "payload_mb_per_s": (
                (bb - ba) / (tb - ta) / 1e6 if args.byte_counter else None
            ),
            "tx_mb_per_s": tx / 1e6 if tx is not None else None,
            "records_per_reply": metrics.records_published
            / max(metrics.batches_acked, 1),
            "batches_acked": metrics.batches_acked,
            "errors_retried": metrics.errors_retried,
            "write_wall_s": write_s,
            "drain_wall_s": samples[-1][0] - t0,
            "window_s": tb - ta,
        }
    finally:
        live.close()


def run_sustained(args: argparse.Namespace) -> dict[str, Any]:
    """Writer at ``rate`` with the bridge running; backlog slope after warmup."""
    live = _Live(args, "sustained")
    try:
        live.start_bridge()
        live.run_in_background()
        writer = Writer(live.client.db, live.names["prefix"], rate=args.rate)
        t0 = time.monotonic()
        writer.start()
        samples: list[tuple[float, int, int]] = []
        lags: list[int] = []
        t_meas = t0 + args.warmup
        t_end = t_meas + args.duration
        while (now := time.monotonic()) < t_end:
            live.check()
            if writer.error is not None:
                writer.join_checked()
            if now >= t_meas:
                samples.append((now, writer.written, live.published()))
                lag = live.lag_sample(writer)
                if lag is not None:
                    lags.append(lag)
            time.sleep(SAMPLE_SECONDS)
        writer.stop_event.set()
        writer.join_checked()
        written = writer.written
        drained = _wait_until(
            lambda: live.published() >= written, args.timeout, live.check
        )
        ts = [t for t, _, _ in samples]
        backlog = [w - p for _, w, p in samples]
        slope = ols_slope(ts, backlog)
        write_rate = ols_slope(ts, [w for _, w, _ in samples])
        delivered = ols_slope(ts, [p for _, _, p in samples])
        published_in_window = samples[-1][2] - samples[0][2]
        metrics = live.bridge.metrics
        return {
            **live.names,
            "rate": args.rate,
            "written": written,
            "published": live.published(),
            "drained": drained,
            "write_rate": write_rate,
            "mutations_per_s": delivered,
            "backlog_slope": slope,
            "backlog_growing": backlog_growing(
                slope, write_rate, published=published_in_window
            ),
            "backlog_end": backlog[-1],
            "lag_samples": len(lags),
            **summarize_lag(lags),
            "records_per_reply": metrics.records_published
            / max(metrics.batches_acked, 1),
            "errors_retried": metrics.errors_retried,
            "warmup_s": args.warmup,
            "duration_s": args.duration,
        }
    finally:
        live.close()


def run_kafka_alone(args: argparse.Namespace) -> dict[str, Any]:
    """Replay one drain-shaped reply into Kafka for ``duration`` seconds, no FDB."""
    names = _names("kafka-alone")
    stats = StatsSampler()
    producer = _producer(args.bootstrap, stats)
    counter = ByteCounter(producer) if args.byte_counter else None
    key = names["stream_name"].encode()
    reply = build_reply(
        6, 500, key_prefix=names["prefix"], stream_name=names["stream_name"]
    )
    per_reply = reply_mutations(reply)
    replies = buffer_waits = 0
    try:
        t0 = time.monotonic()
        while time.monotonic() - t0 < args.duration:
            for record in reply:
                while True:
                    try:
                        producer.produce(names["topic"], record, key=key)
                        break
                    except BufferError:
                        # Local queue full (mostly --flush-at-end): let it drain.
                        buffer_waits += 1
                        producer.flush(timeout=1.0)
            if not args.flush_at_end:
                producer.flush_or_raise()
            replies += 1
        if args.flush_at_end:
            producer.flush_or_raise(timeout=120.0)
        elapsed = time.monotonic() - t0
    finally:
        producer.close()
    tx = stats.rate_between(t0, t0 + elapsed)
    return {
        **names,
        "flush_at_end": args.flush_at_end,
        "replies": replies,
        "records_per_reply": len(reply),
        "mutations_per_reply": per_reply,
        "published": replies * per_reply,
        "mutations_per_s": replies * per_reply / elapsed,
        "payload_mb_per_s": counter.bytes / elapsed / 1e6 if counter else None,
        "tx_mb_per_s": tx / 1e6 if tx is not None else None,
        "buffer_full_waits": buffer_waits,
        "wall_s": elapsed,
    }


MODES: dict[str, Callable[[argparse.Namespace], dict[str, Any]]] = {
    "drain": run_drain,
    "sustained": run_sustained,
    "kafka-alone": run_kafka_alone,
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("mode", choices=sorted(MODES))
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--preload", type=int, default=1_000_000)
    p.add_argument("--rate", type=float, default=15_000)
    p.add_argument("--warmup", type=float, default=10)
    p.add_argument("--duration", type=float, help="s; default 60, kafka-alone 30")
    p.add_argument("--timeout", type=float, default=600, help="drain wait, s")
    p.add_argument("--flush-at-end", action="store_true")
    p.add_argument("--no-byte-counter", dest="byte_counter", action="store_false")
    p.add_argument("--repo-sha", default="unknown")
    args = p.parse_args(argv)
    if args.duration is None:
        args.duration = 30.0 if args.mode == "kafka-alone" else 60.0
    return args


def main(argv: Sequence[str] | None = None) -> None:
    """Run one mode and write its JSON."""
    args = parse_args(argv)
    started = time.time()
    result = MODES[args.mode](args)
    result.update(
        mode=args.mode,
        byte_counter=args.byte_counter,
        repo_sha=args.repo_sha,
        versions=_versions(),
        started_unix=started,
        finished_unix=time.time(),
    )
    result["prefix"] = result["prefix"].decode()
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    print(
        json.dumps({k: result.get(k) for k in ("mode", "mutations_per_s", "drained")})
    )


if __name__ == "__main__":
    main()
