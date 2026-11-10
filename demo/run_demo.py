"""Run a FoundationDB CDC to Kafka Bridge demonstration.

Demonstrates the Kafka Bridge running as an autonomous streaming daemon:
1. Connects to FoundationDB (API 800).
2. Initializes and launches the Kafka Bridge daemon using declarative configuration.
3. Upstream application commits representative writes and atomic mutations.
4. Bridge autonomously captures CDC mutations, serializes them to Protobuf,
   delivers them to Kafka (acks=all), and advances the FDB retention watermark.
5. Downstream consumer verifies every record against the committed workload.
6. Gracefully terminates the Bridge daemon with queue drainage.

Modes:
  * ``batch`` (default): write a fixed mutation count, then wait for the bridge.
  * ``sustained``: write concurrently with the bridge for a duration at a target
    rate, measure backlog / catch-up, and verify at-least-once delivery.
"""

from __future__ import annotations

import argparse
import os
import random
import struct
import sys
import threading
import time
import uuid
from collections.abc import Sequence
from typing import Any

import fdb
from confluent_kafka import Consumer, KafkaException

from demo.sustained import SustainedStats, caught_up, default_max_backlog
from demo.workload import verify_workload, verify_workload_at_least_once
from fdb_kafka_bridge.bridge.config import from_dict
from fdb_kafka_bridge.bridge.pipeline import Bridge
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.listener import strinc
from fdbkafka.cdc.v1 import mutations_pb2

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
DEFAULT_SAMPLE_COUNT = 10000
DEFAULT_SUSTAINED_DURATION_SECONDS = 30.0
DEFAULT_SUSTAINED_TARGET_RATE = 500.0
DEFAULT_CATCHUP_TIMEOUT_SECONDS = 60.0
DEFAULT_SEED = 1

# Names for every mutation type that the native CDC API may emit.
MUTATION_TYPE_NAMES: dict[int, str] = {
    0: "SET_VALUE",
    1: "CLEAR_RANGE",
    2: "ADD (Atomic Int64)",
    6: "AND (Atomic Bitwise)",
    7: "OR (Atomic Bitwise)",
    8: "XOR (Atomic Bitwise)",
    9: "APPEND_IF_FITS (Atomic)",
    12: "MAX (Atomic Int64)",
    13: "MIN (Atomic Int64)",
    14: "SET_VERSIONSTAMPED_KEY",
    15: "SET_VERSIONSTAMPED_VALUE",
    16: "BYTE_MIN (Atomic)",
    17: "BYTE_MAX (Atomic)",
    18: "MIN_V2 (Atomic)",
    19: "AND_V2 (Atomic Bitwise)",
    20: "COMPARE_AND_CLEAR (Atomic)",
}


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command line arguments for the demonstration.

    Args:
        argv: Optional command line tokens.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("batch", "sustained"),
        default="batch",
        help="batch: fixed write-then-drain demo; sustained: concurrent workload.",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
        ),
        help="Kafka bootstrap server address.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help=(
            "Batch mode: mutations to write (default 10000). "
            "Sustained mode: optional hard cap (default unlimited for duration)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3600.0,
        help="Maximum timeout in seconds for batch streaming and consumption.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_SUSTAINED_DURATION_SECONDS,
        help="Sustained mode: seconds to keep writing concurrently.",
    )
    parser.add_argument(
        "--target-rate",
        type=float,
        default=DEFAULT_SUSTAINED_TARGET_RATE,
        help="Sustained mode: target mutations/sec (0 = unthrottled).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Sustained mode: RNG seed for pacing jitter.",
    )
    parser.add_argument(
        "--catchup-timeout",
        type=float,
        default=DEFAULT_CATCHUP_TIMEOUT_SECONDS,
        help="Sustained mode: seconds allowed for the bridge to drain after writes.",
    )
    parser.add_argument(
        "--max-backlog",
        type=int,
        default=None,
        help=(
            "Sustained mode: soft max for written-published during the write "
            "window (default max(target_rate*5, 1000))."
        ),
    )
    return parser.parse_args(argv)


def _apply_one_mutation(tr: Any, prefix: bytes, index: int) -> None:
    """Apply one representative mutation for ``index`` inside an open transaction.

    Args:
        tr: FoundationDB transaction.
        prefix: Target key prefix.
        index: Workload operation index.
    """
    mod = index % 14
    idx_bytes = f"{index:08d}".encode()
    if mod == 0:
        tr[prefix + b"set_" + idx_bytes] = f"val-{index}".encode()
    elif mod == 1:
        tr.clear_range(
            prefix + b"clr_" + idx_bytes + b"/a",
            prefix + b"clr_" + idx_bytes + b"/z",
        )
    elif mod == 2:
        delta = (index & 0xFFFFFFFF).to_bytes(8, "little")
        tr.add(prefix + b"add_" + idx_bytes, delta)
    elif mod == 3:
        tr.bit_and(prefix + b"and_" + idx_bytes, b"\x0f")
    elif mod == 4:
        tr.bit_or(prefix + b"or_" + idx_bytes, b"\xf0")
    elif mod == 5:
        tr.bit_xor(prefix + b"xor_" + idx_bytes, b"\xaa")
    elif mod == 6:
        tr.append_if_fits(prefix + b"app_" + idx_bytes, b"chunk")
    elif mod == 7:
        val = (index & 0xFFFFFFFF).to_bytes(8, "little")
        tr.max(prefix + b"max_" + idx_bytes, val)
    elif mod == 8:
        val = (index & 0xFFFFFFFF).to_bytes(8, "little")
        tr.min(prefix + b"min_" + idx_bytes, val)
    elif mod == 9:
        k_pref = prefix + b"vsk_" + idx_bytes
        vsk_key = k_pref + b"\x00" * 10 + struct.pack("<I", len(k_pref))
        tr.set_versionstamped_key(vsk_key, b"vsk_payload")
    elif mod == 10:
        v_pref = b"vsv_" + idx_bytes
        vsv_val = v_pref + b"\x00" * 10 + struct.pack("<I", len(v_pref))
        tr.set_versionstamped_value(prefix + b"vsv_" + idx_bytes, vsv_val)
    elif mod == 11:
        tr.byte_min(prefix + b"bmin_" + idx_bytes, b"\x10")
    elif mod == 12:
        tr.byte_max(prefix + b"bmax_" + idx_bytes, b"\xfe")
    elif mod == 13:
        tr[prefix + b"set_b_" + idx_bytes] = f"val-b-{index}".encode()


def _write_samples(database: Any, prefix: bytes, count: int) -> float:
    """Write representative mutations in bounded transactions.

    Generates a round-robin mix of FoundationDB mutation operations. Some legacy
    and versionstamped operations are normalized before native CDC emits them.

    Args:
        database: FoundationDB database connection.
        prefix: Target key prefix for the write batch.
        count: Total number of operations to commit.

    Returns:
        Duration of the write workload in seconds.
    """
    start_time = time.monotonic()
    batch_size = 500
    report_interval = max(100_000, count // 10)
    next_report = report_interval

    @fdb.transactional
    def _commit_chunk(tr: Any, s_idx: int, c_size: int) -> None:
        for index in range(s_idx, s_idx + c_size):
            _apply_one_mutation(tr, prefix, index)

    for start_idx in range(0, count, batch_size):
        chunk_size = min(batch_size, count - start_idx)
        _commit_chunk(database, start_idx, chunk_size)

        current_total = start_idx + chunk_size
        if current_total >= next_report and current_total < count:
            elapsed = time.monotonic() - start_time
            rate = current_total / max(elapsed, 0.001)
            print(
                f"      ... committed {current_total:,} / {count:,} mutations "
                f"({rate:,.0f} writes/sec)"
            )
            next_report += report_interval

    return time.monotonic() - start_time


def _seed_clear_targets(database: Any, prefix: bytes, indexes: Sequence[int]) -> None:
    """Create keys that disjoint clear ranges will remove.

    Args:
        database: FoundationDB database connection.
        prefix: Target key prefix for the workload.
        indexes: Workload indexes that may include clear-range ops.
    """

    @fdb.transactional
    def _seed_chunk(tr: Any, chunk: list[int]) -> None:
        for index in chunk:
            idx_bytes = f"{index:08d}".encode()
            tr[prefix + b"clr_" + idx_bytes + b"/m"] = b"seed"

    clear_indexes = [index for index in indexes if index % 14 == 1]
    for offset in range(0, len(clear_indexes), 500):
        _seed_chunk(database, clear_indexes[offset : offset + 500])


def _commit_chunk_with_seed(
    database: Any, prefix: bytes, start_idx: int, chunk_size: int
) -> None:
    """Seed clear targets then commit ``chunk_size`` mutations from ``start_idx``.

    Args:
        database: FoundationDB database connection.
        prefix: Workload key prefix.
        start_idx: First workload index.
        chunk_size: Number of mutations to commit.
    """
    indexes = list(range(start_idx, start_idx + chunk_size))
    _seed_clear_targets(database, prefix, indexes)

    @fdb.transactional
    def _commit(tr: Any) -> None:
        for index in indexes:
            _apply_one_mutation(tr, prefix, index)

    _commit(database)


def _consume_records(
    bootstrap_servers: str,
    topic: str,
    min_expected_mutations: int,
    timeout: float,
) -> list[mutations_pb2.FDBMutationRecord]:
    """Consume all Protobuf records from the Kafka topic.

    Polls Kafka until at least min_expected_mutations have been received,
    with an idle streak detector to catch end-of-stream.

    Args:
        bootstrap_servers: Kafka broker addresses.
        topic: Kafka topic to consume from.
        min_expected_mutations: Minimum total mutations expected.
        timeout: Maximum seconds to wait.

    Returns:
        List of deserialized Protobuf records.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"demo-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    records: list[mutations_pb2.FDBMutationRecord] = []
    deadline = time.monotonic() + timeout
    total_mutations = 0
    idle_streak = 0

    try:
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=1.0)
            if message is None:
                if total_mutations >= min_expected_mutations:
                    idle_streak += 1
                    if idle_streak >= 3:
                        break
                continue

            idle_streak = 0
            if message.error() is not None:
                raise KafkaException(message.error())
            value = message.value()
            if value is None:
                raise RuntimeError("Kafka returned a record without a value")

            record = mutations_pb2.FDBMutationRecord.FromString(value)
            records.append(record)
            if record.HasField("batch"):
                total_mutations += len(record.batch.mutations)
            elif record.HasField("mutation"):
                total_mutations += 1

            if total_mutations >= min_expected_mutations and idle_streak >= 2:
                break
    finally:
        consumer.close()

    if total_mutations < min_expected_mutations:
        raise TimeoutError(
            f"Kafka consumer decoded {total_mutations} of "
            f"{min_expected_mutations} expected mutations within {timeout}s"
        )

    return records


class _ConcurrentKafkaCollector:
    """Background Kafka consumer that accumulates records until stopped."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        """Create a collector for ``topic``.

        Args:
            bootstrap_servers: Kafka broker addresses.
            topic: Topic to subscribe to.
        """
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._stop = threading.Event()
        self._errors: list[BaseException] = []
        self._lock = threading.Lock()
        self._records: list[mutations_pb2.FDBMutationRecord] = []
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background poll loop."""
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="DemoKafkaCollector"
        )
        self._thread.start()

    def request_stop(self) -> None:
        """Signal the poll loop to exit without joining."""
        self._stop.set()

    def snapshot(self) -> list[mutations_pb2.FDBMutationRecord]:
        """Return a copy of records collected so far."""
        if self._errors:
            raise RuntimeError("Kafka collector failed") from self._errors[0]
        with self._lock:
            return list(self._records)

    def mutation_count(self) -> int:
        """Return decoded mutation count across collected records."""
        return _record_mutation_count(self.snapshot())

    def stop(self, timeout: float = 10.0) -> list[mutations_pb2.FDBMutationRecord]:
        """Stop the collector and return accumulated records.

        Args:
            timeout: Join timeout in seconds.

        Returns:
            Records collected so far.

        Raises:
            RuntimeError: If the collector thread raised.
        """
        self.request_stop()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._errors:
            raise RuntimeError("Kafka collector failed") from self._errors[0]
        with self._lock:
            return list(self._records)

    def _run(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": f"demo-sustained-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([self._topic])
        try:
            while not self._stop.is_set():
                message = consumer.poll(timeout=0.5)
                if message is None:
                    continue
                if message.error() is not None:
                    raise KafkaException(message.error())
                value = message.value()
                if value is None:
                    continue
                record = mutations_pb2.FDBMutationRecord.FromString(value)
                with self._lock:
                    self._records.append(record)
        except BaseException as exc:
            self._errors.append(exc)
        finally:
            consumer.close()


def _record_mutation_count(records: Sequence[mutations_pb2.FDBMutationRecord]) -> int:
    """Count individual mutations across Protobuf envelopes.

    Args:
        records: Decoded Kafka records.

    Returns:
        Total mutation count (batches + singles); VersionEnds contribute zero.
    """
    total = 0
    for record in records:
        if record.HasField("batch"):
            total += len(record.batch.mutations)
        elif record.HasField("mutation"):
            total += 1
    return total


def _drain_collector(
    collector: _ConcurrentKafkaCollector,
    *,
    min_mutations: int,
    timeout: float,
) -> list[mutations_pb2.FDBMutationRecord]:
    """Wait until the collector has at least ``min_mutations``, then stop it.

    Args:
        collector: Running concurrent Kafka collector.
        min_mutations: Minimum decoded mutations required before stop.
        timeout: Maximum seconds to wait for the collector to catch up.

    Returns:
        All records accumulated by the collector.

    Raises:
        TimeoutError: If the collector does not reach ``min_mutations`` in time.
        RuntimeError: If the collector thread failed.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if collector.mutation_count() >= min_mutations:
            return collector.stop(timeout=15.0)
        time.sleep(0.2)
    got = collector.mutation_count()
    collector.request_stop()
    raise TimeoutError(
        f"Kafka collector decoded {got} of {min_mutations} mutations "
        f"within {timeout}s drain window"
    )


def _stop_bridge(bridge: Bridge | None, thread: threading.Thread | None) -> None:
    """Best-effort bridge teardown that never raises or blocks on a stuck step.

    Args:
        bridge: Bridge to stop, if one was created.
        thread: Thread running `bridge.run()`, if one was started.
    """
    if bridge is None:
        return
    if thread is not None and thread.is_alive():
        # stop() would wait on the step lock the stuck thread holds; the daemon
        # thread dies with the process.
        print(
            "      WARNING: bridge thread still running; skipping stop()",
            file=sys.stderr,
        )
        return
    try:
        bridge.stop()
    except Exception as exc:
        print(f"      WARNING: bridge.stop() failed: {exc!r}", file=sys.stderr)


def _start_bridge(
    *,
    stream_name: str,
    prefix: bytes,
    topic: str,
    bootstrap_servers: str,
    cursor_file: str,
) -> tuple[Bridge, threading.Thread, list[BaseException]]:
    """Configure, start, and launch the bridge daemon thread.

    Args:
        stream_name: CDC stream name.
        prefix: Key-range prefix.
        topic: Kafka topic.
        bootstrap_servers: Kafka brokers.
        cursor_file: Durable cursor path.

    Returns:
        Bridge instance, daemon thread, and shared error list.
    """
    bridge_cfg = from_dict(
        {
            "fdb": {"api_version": 800},
            "cdc": {
                "stream_name": stream_name,
                "begin_key": prefix.hex(),
                "end_key": strinc(prefix).hex(),
                "max_record_bytes": 1_000_000,
                "cursor_file": cursor_file,
            },
            "kafka": {
                "bootstrap_servers": bootstrap_servers,
                "topic": topic,
            },
            "pipeline": {
                "flush_timeout_seconds": 10.0,
                "backoff_base_seconds": 0.1,
                "backoff_max_seconds": 2.0,
                "shutdown_timeout_seconds": 15.0,
            },
        }
    )
    bridge = Bridge(bridge_cfg)
    bridge.start()
    bridge_errors: list[BaseException] = []

    def _run_bridge() -> None:
        try:
            bridge.run()
        except BaseException as exc:
            bridge_errors.append(exc)

    bridge_thread = threading.Thread(
        target=_run_bridge, daemon=True, name="BridgeDaemon"
    )
    bridge_thread.start()
    return bridge, bridge_thread, bridge_errors


def _cleanup_demo(
    *,
    bridge: Bridge | None,
    bridge_thread: threading.Thread | None,
    database: Any,
    fdb_client: FDBClient | None,
    prefix: bytes,
    stream_name: str,
    cursor_file: str,
) -> None:
    """Best-effort teardown shared by batch and sustained modes."""
    if bridge is not None:
        bridge.request_shutdown()
    if bridge_thread is not None and bridge_thread.is_alive():
        bridge_thread.join(timeout=10.0)
    _stop_bridge(bridge, bridge_thread)
    if database is not None:
        try:
            del database[prefix : strinc(prefix)]
        except Exception:
            pass
    if fdb_client is not None:
        try:
            fdb_client.remove_cdc_stream(stream_name)
        except Exception:
            pass
        fdb_client.close()
    try:
        os.unlink(cursor_file)
    except (FileNotFoundError, OSError):
        pass


def run_demo(bootstrap_servers: str, timeout: float, count: int) -> None:
    """Run step-by-step FoundationDB CDC to Kafka Bridge demonstration.

    Args:
        bootstrap_servers: Kafka broker addresses.
        timeout: Maximum seconds to wait.
        count: Total number of mutations to write.
    """
    run_id = uuid.uuid4().hex[:10]
    stream_name = f"demo-{run_id}"
    topic = f"fdb-cdc-demo-{run_id}"
    prefix = f"demo/{run_id}/".encode()
    cursor_file = f"/tmp/fdb-kafka-demo-{run_id}.cursor.json"

    print("=" * 76)
    print("  FoundationDB Native CDC -> Apache Kafka Bridge Demonstration")
    print("=" * 76)

    print("\n[1/7] FoundationDB Database: Initializing Connection (API 800)")
    fdb_client = FDBClient(api_version=800)
    database = fdb_client.open()
    read_version = fdb_client.get_read_version()
    print(f"      Connected to cluster. Current read version: {read_version}")

    bridge: Bridge | None = None
    bridge_thread: threading.Thread | None = None

    try:
        _seed_clear_targets(database, prefix, range(count))
        print("      Prepared disjoint clear-range targets before CDC registration.")

        print(
            "\n[2/7] Kafka Bridge Service: Declarative Configuration & Daemon Startup"
        )
        print(f"      Stream Name       : '{stream_name}'")
        print(f"      Key Range         : [{prefix!r}, {strinc(prefix)!r})")
        print(f"      Kafka Target      : '{topic}' on {bootstrap_servers}")
        print("      Delivery Policy   : acks=all, enable.idempotence=True")
        print("      Retention Policy  : Advance minVersion only after Kafka ack")

        bridge, bridge_thread, bridge_errors = _start_bridge(
            stream_name=stream_name,
            prefix=prefix,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            cursor_file=cursor_file,
        )
        print("      Registered CDC stream in FDB transaction subsystem.")
        print("      Started autonomous Kafka Bridge daemon thread.")
        time.sleep(1.0)

        print(f"\n[3/7] Upstream Application: Committing {count:,} mutations to FDB")
        print("      Exercising representative writes and atomic operations...")
        stream_start = time.monotonic()
        write_duration = _write_samples(database, prefix, count)
        write_rate = count / max(write_duration, 0.001)
        print(
            f"      Committed {count:,} mutations in {write_duration:.3f}s "
            f"({write_rate:,.0f} writes/sec)"
        )

        print("\n[4/7] Autonomous Bridge Pipeline: CDC Ingest -> Protobuf -> Kafka")
        print("      Bridge is streaming, serializing, publishing, and acking...")
        deadline = stream_start + timeout
        stream_report_interval = max(100_000, count // 10)
        next_stream_report = stream_report_interval

        while bridge.mutations_published < count:
            if bridge_errors:
                raise RuntimeError("Bridge daemon failed") from bridge_errors[0]
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Bridge published {bridge.mutations_published} of {count} "
                    f"mutations within {timeout}s deadline"
                )
            pub = bridge.mutations_published
            if pub >= next_stream_report and pub < count:
                elapsed = time.monotonic() - stream_start
                rate = pub / max(elapsed, 0.001)
                print(
                    f"      ... bridge streamed {pub:,} / {count:,} mutations "
                    f"({rate:,.0f} mut/sec, {bridge.batches_acked} acks)"
                )
                next_stream_report += stream_report_interval
            time.sleep(0.2)

        stream_duration = time.monotonic() - stream_start
        stream_rate = bridge.mutations_published / max(stream_duration, 0.001)
        print(
            f"      Completed: {bridge.mutations_published:,} mutations streamed in "
            f"{stream_duration:.3f}s ({stream_rate:,.0f} mut/sec)"
        )
        print(
            f"      Batches durably acknowledged: {bridge.batches_acked:,} "
            f"(FDB retention watermark advanced)"
        )
        bridge.request_shutdown()
        bridge_thread.join(timeout=10.0)
        if bridge_thread.is_alive():
            raise TimeoutError("Bridge daemon did not stop within 10 seconds")
        if bridge_errors:
            raise RuntimeError("Bridge daemon failed") from bridge_errors[0]

        print("\n[5/7] Downstream Kafka Consumers: Ingestion from Topic")
        consume_start = time.monotonic()
        consumed = _consume_records(
            bootstrap_servers, topic, bridge.mutations_published, timeout
        )
        consume_duration = time.monotonic() - consume_start
        print(
            f"      Received {len(consumed):,} Kafka Protobuf records in "
            f"{consume_duration:.3f}s"
        )

        print("\n[6/7] Workload Verification: Kafka Records vs. Committed Writes")
        if bridge.mutations_published != count:
            raise AssertionError(
                f"Bridge published count mismatch: expected {count:,}, but bridge "
                f"published {bridge.mutations_published:,}"
            )
        type_counts = verify_workload(consumed, prefix, count)
        versions = sum(1 for rec in consumed if rec.HasField("version_end"))
        total_wire_bytes = sum(rec.ByteSize() for rec in consumed)
        print(
            f"      Verified all {count:,} workload mutations byte for byte "
            "(versionstamped ops on prefix, suffix and length)."
        )
        print(
            f"      Verified {versions:,} VersionEnd boundaries: unique coordinates, "
            "contiguous sequences, totals sum to the workload."
        )
        for code, n in sorted(type_counts.items()):
            label = f"{MUTATION_TYPE_NAMES.get(code, f'TYPE_{code}')} (code {code})"
            print(f"      [✓] {label:<36} : count = {n:,}")
        print(
            f"      Total wire payload: {total_wire_bytes / 1024 / 1024:.2f} MB "
            f"across {len(consumed):,} Kafka messages"
        )

        print("\n[7/7] Bridge Service: Graceful Shutdown & Drain")
        metrics = bridge.metrics
        bridge.stop()
        print(
            f"      Bridge daemon stopped. Final metrics: "
            f"published={metrics.mutations_published:,}, "
            f"records={metrics.records_published:,}, "
            f"batches_acked={metrics.batches_acked:,}, "
            f"retries={metrics.errors_retried:,}"
        )

        print("\n" + "=" * 76)
        print(
            f"  SUCCESS: {count:,} FDB mutations streamed by Bridge -> Kafka "
            f"'{topic}' and verified against the committed workload."
        )
        print("=" * 76 + "\n")

    finally:
        _cleanup_demo(
            bridge=bridge,
            bridge_thread=bridge_thread,
            database=database,
            fdb_client=fdb_client,
            prefix=prefix,
            stream_name=stream_name,
            cursor_file=cursor_file,
        )


def run_sustained_demo(
    *,
    bootstrap_servers: str,
    duration: float,
    target_rate: float,
    seed: int,
    catchup_timeout: float,
    max_backlog: int | None,
    count_cap: int | None,
) -> SustainedStats:
    """Run a sustained concurrent write + bridge + Kafka consumer workload.

    Args:
        bootstrap_servers: Kafka broker addresses.
        duration: Seconds to keep the writer running.
        target_rate: Target mutations per second (0 = unthrottled).
        seed: RNG seed for pacing jitter.
        catchup_timeout: Seconds allowed for drain after writers stop.
        max_backlog: Soft backlog threshold (default from target_rate).
        count_cap: Optional hard cap on mutations written.

    Returns:
        Accumulated sustained-run statistics.

    Raises:
        TimeoutError: If the bridge does not catch up in time.
        AssertionError: If at-least-once verification fails.
        RuntimeError: If the bridge or Kafka collector fails.
    """
    if duration <= 0:
        raise ValueError("duration must be positive")
    if catchup_timeout <= 0:
        raise ValueError("catchup_timeout must be positive")
    if target_rate < 0:
        raise ValueError("target_rate must be non-negative")
    if count_cap is not None and count_cap <= 0:
        raise ValueError("count must be positive when set in sustained mode")

    threshold = (
        max_backlog if max_backlog is not None else default_max_backlog(target_rate)
    )
    rng = random.Random(seed)
    stats = SustainedStats()

    run_id = uuid.uuid4().hex[:10]
    stream_name = f"demo-sustained-{run_id}"
    topic = f"fdb-cdc-sustained-{run_id}"
    prefix = f"demo/sustained/{run_id}/".encode()
    cursor_file = f"/tmp/fdb-kafka-sustained-{run_id}.cursor.json"

    print("=" * 76)
    print("  Sustained Concurrent CDC Ingestion Demonstration")
    print("=" * 76)
    print(f"      duration={duration}s target_rate={target_rate}/s seed={seed}")
    print(f"      catchup_timeout={catchup_timeout}s max_backlog={threshold:,}")
    if count_cap is not None:
        print(f"      count_cap={count_cap:,}")

    print("\n[1/6] FoundationDB Database: Initializing Connection (API 800)")
    fdb_client = FDBClient(api_version=800)
    database = fdb_client.open()
    print(f"      Connected. read_version={fdb_client.get_read_version()}")

    bridge: Bridge | None = None
    bridge_thread: threading.Thread | None = None
    collector: _ConcurrentKafkaCollector | None = None
    writer_errors: list[BaseException] = []
    written = 0
    written_lock = threading.Lock()

    try:
        print("\n[2/6] Kafka Bridge + Concurrent Consumer: Startup")
        bridge, bridge_thread, bridge_errors = _start_bridge(
            stream_name=stream_name,
            prefix=prefix,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            cursor_file=cursor_file,
        )
        collector = _ConcurrentKafkaCollector(bootstrap_servers, topic)
        collector.start()
        time.sleep(1.0)
        print(f"      stream='{stream_name}' topic='{topic}'")

        print("\n[3/6] Concurrent Writer: Sustained Mixed Mutations")
        e2e_start = time.monotonic()
        write_deadline = e2e_start + duration
        chunk_size = 50 if target_rate > 0 else 500
        next_index = 0
        last_published_for_sample = 0
        last_written_for_sample = 0
        sample_mark = e2e_start
        backlog_warned = False

        def _writer() -> None:
            nonlocal next_index, written
            try:
                while time.monotonic() < write_deadline:
                    if count_cap is not None and next_index >= count_cap:
                        break
                    size = chunk_size
                    if count_cap is not None:
                        size = min(size, count_cap - next_index)
                    if size <= 0:
                        break
                    _commit_chunk_with_seed(database, prefix, next_index, size)
                    next_index += size
                    with written_lock:
                        written = next_index
                    if target_rate > 0:
                        expected = next_index / target_rate
                        elapsed = time.monotonic() - e2e_start
                        sleep_for = expected - elapsed
                        if sleep_for > 0:
                            jitter = rng.uniform(0.0, min(0.01, sleep_for))
                            time.sleep(sleep_for + jitter)
            except BaseException as exc:
                writer_errors.append(exc)

        writer_thread = threading.Thread(
            target=_writer, daemon=True, name="SustainedWriter"
        )
        writer_thread.start()

        while writer_thread.is_alive():
            if bridge_errors:
                raise RuntimeError("Bridge daemon failed") from bridge_errors[0]
            if writer_errors:
                raise RuntimeError("Writer failed") from writer_errors[0]
            with written_lock:
                w = written
            pub = bridge.mutations_published
            backlog = stats.sample_backlog(written=w, published=pub)
            stats.batches_acked = bridge.batches_acked
            if pub > last_published_for_sample and w > last_written_for_sample:
                stats.monitor_sample_intervals.append(time.monotonic() - sample_mark)
                last_published_for_sample = pub
                last_written_for_sample = w
                sample_mark = time.monotonic()
            if backlog > threshold and not backlog_warned:
                backlog_warned = True
                print(
                    f"      WARNING: backlog {backlog:,} exceeds soft threshold "
                    f"{threshold:,} (continuing; catch-up is the hard gate)",
                    file=sys.stderr,
                )
            time.sleep(0.2)

        writer_thread.join(timeout=30.0)
        if writer_errors:
            raise RuntimeError("Writer failed") from writer_errors[0]
        stats.write_duration_seconds = time.monotonic() - e2e_start
        with written_lock:
            stats.written = written
        print(
            f"      Writer stopped: written={stats.written:,} in "
            f"{stats.write_duration_seconds:.3f}s "
            f"(max_backlog={stats.max_backlog:,})"
        )

        print("\n[4/6] Catch-up: Waiting for Bridge to Drain")
        catchup_start = time.monotonic()
        catchup_deadline = catchup_start + catchup_timeout
        while not caught_up(
            written=stats.written, published=bridge.mutations_published
        ):
            if bridge_errors:
                raise RuntimeError("Bridge daemon failed") from bridge_errors[0]
            if time.monotonic() >= catchup_deadline:
                raise TimeoutError(
                    f"Bridge published {bridge.mutations_published} of "
                    f"{stats.written} mutations within {catchup_timeout}s catch-up"
                )
            stats.sample_backlog(
                written=stats.written, published=bridge.mutations_published
            )
            stats.batches_acked = bridge.batches_acked
            time.sleep(0.2)
        stats.catchup_seconds = time.monotonic() - catchup_start
        stats.published = bridge.mutations_published
        stats.batches_acked = bridge.batches_acked
        print(
            f"      Caught up in {stats.catchup_seconds:.3f}s: "
            f"published={stats.published:,} acks={stats.batches_acked:,}"
        )

        bridge.request_shutdown()
        bridge_thread.join(timeout=10.0)
        if bridge_thread.is_alive():
            raise TimeoutError("Bridge daemon did not stop within 10 seconds")
        if bridge_errors:
            raise RuntimeError("Bridge daemon failed") from bridge_errors[0]

        print("\n[5/6] Verification: At-Least-Once + VersionEnd Integrity")
        assert collector is not None
        consumed = _drain_collector(
            collector, min_mutations=stats.written, timeout=max(catchup_timeout, 15.0)
        )
        result = verify_workload_at_least_once(consumed, prefix, stats.written)
        versions = sum(1 for rec in consumed if rec.HasField("version_end"))
        print(
            f"      Verified {stats.written:,} unique mutations "
            f"(duplicates={result.duplicates:,}, version_ends={versions:,})"
        )
        for code, n in sorted(result.type_counts.items()):
            label = f"{MUTATION_TYPE_NAMES.get(code, f'TYPE_{code}')} (code {code})"
            print(f"      [✓] {label:<36} : count = {n:,}")

        print("\n[6/6] Bridge Shutdown & Summary")
        metrics = bridge.metrics
        bridge.stop()
        stats.e2e_seconds = time.monotonic() - e2e_start
        avg_sample_interval = (
            sum(stats.monitor_sample_intervals) / len(stats.monitor_sample_intervals)
            if stats.monitor_sample_intervals
            else float("nan")
        )
        print(
            f"      published={metrics.mutations_published:,} "
            f"records={metrics.records_published:,} "
            f"acks={metrics.batches_acked:,} retries={metrics.errors_retried:,}"
        )
        print(
            f"      max_backlog={stats.max_backlog:,} "
            f"catchup={stats.catchup_seconds:.3f}s "
            f"e2e={stats.e2e_seconds:.3f}s "
            f"monitor_sample_interval={avg_sample_interval:.3f}s"
        )
        print(
            f"      backlog_threshold={threshold:,} "
            f"(soft warn only; hard gate is catch-up)"
        )

        print("\n" + "=" * 76)
        print(
            f"  SUCCESS: sustained run wrote {stats.written:,} mutations; "
            f"bridge caught up; Kafka verified at-least-once "
            f"(duplicates={result.duplicates})."
        )
        print("=" * 76 + "\n")
        return stats

    finally:
        if collector is not None:
            collector.request_stop()
        _cleanup_demo(
            bridge=bridge,
            bridge_thread=bridge_thread,
            database=database,
            fdb_client=fdb_client,
            prefix=prefix,
            stream_name=stream_name,
            cursor_file=cursor_file,
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line demo.

    Args:
        argv: Optional command-line arguments.

    Returns:
        Process exit code.
    """
    args = _arguments(argv)
    if args.mode == "sustained":
        run_sustained_demo(
            bootstrap_servers=args.kafka_bootstrap_servers,
            duration=args.duration,
            target_rate=args.target_rate,
            seed=args.seed,
            catchup_timeout=args.catchup_timeout,
            max_backlog=args.max_backlog,
            count_cap=args.count,
        )
        return 0

    count = DEFAULT_SAMPLE_COUNT if args.count is None else args.count
    if count <= 0:
        raise SystemExit("--count must be positive in batch mode")
    run_demo(args.kafka_bootstrap_servers, args.timeout, count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
