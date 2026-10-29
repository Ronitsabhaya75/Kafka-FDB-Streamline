"""Run a FoundationDB CDC to Kafka Bridge demonstration.

Demonstrates the Kafka Bridge running as an autonomous streaming daemon:
1. Connects to FoundationDB (API 800).
2. Initializes and launches the Kafka Bridge daemon using declarative configuration.
3. Upstream application commits representative writes and atomic mutations.
4. Bridge autonomously captures CDC mutations, serializes them to Protobuf,
   delivers them to Kafka (acks=all), and advances the FDB retention watermark.
5. Downstream consumer verifies every record against the committed workload.
6. Gracefully terminates the Bridge daemon with queue drainage.
"""

from __future__ import annotations

import argparse
import os
import struct
import sys
import threading
import time
import uuid
from collections.abc import Sequence
from typing import Any

import fdb
from confluent_kafka import Consumer, KafkaException

from demo.workload import verify_workload
from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.config import from_dict
from src.bridge.pipeline import Bridge
from src.cdc.client import FDBClient
from src.cdc.listener import strinc

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
DEFAULT_SAMPLE_COUNT = 10000

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
        "--kafka-bootstrap-servers",
        default=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
        ),
        help="Kafka bootstrap server address.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_SAMPLE_COUNT,
        help="Number of mutations to write (e.g. 100, 1000, 10000).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3600.0,
        help="Maximum timeout in seconds for streaming and consumption.",
    )
    return parser.parse_args(argv)


def _write_samples(database: Any, prefix: bytes, count: int) -> float:
    """Write representative mutations in bounded transactions.

    Generates a round-robin mix of FoundationDB mutation operations. Some legacy
    and versionstamped operations are normalized before native CDC emits them.
    - SET_VALUE (0)
    - CLEAR_RANGE (1)
    - ADD (2)
    - BIT_AND / AND_V2 (6 / 19)
    - BIT_OR (7)
    - BIT_XOR (8)
    - APPEND_IF_FITS (9)
    - MAX (12)
    - MIN / MIN_V2 (13 / 18)
    - SET_VERSIONSTAMPED_KEY (14)
    - SET_VERSIONSTAMPED_VALUE (15)
    - BYTE_MIN (16)
    - BYTE_MAX (17)

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


def _seed_clear_targets(database: Any, prefix: bytes, count: int) -> None:
    """Create keys that the demo's disjoint clear ranges will remove.

    Args:
        database: FoundationDB database connection.
        prefix: Target key prefix for the workload.
        count: Total number of operations in the upcoming workload.
    """

    @fdb.transactional
    def _seed_chunk(tr: Any, indexes: list[int]) -> None:
        for index in indexes:
            idx_bytes = f"{index:08d}".encode()
            tr[prefix + b"clr_" + idx_bytes + b"/m"] = b"seed"

    clear_indexes = [index for index in range(count) if index % 14 == 1]
    for offset in range(0, len(clear_indexes), 500):
        _seed_chunk(database, clear_indexes[offset : offset + 500])


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

    # 1. FoundationDB Database Initialization
    print("\n[1/7] FoundationDB Database: Initializing Connection (API 800)")
    fdb_client = FDBClient(api_version=800)
    database = fdb_client.open()
    read_version = fdb_client.get_read_version()
    print(f"      Connected to cluster. Current read version: {read_version}")

    bridge: Bridge | None = None
    bridge_thread: threading.Thread | None = None

    try:
        _seed_clear_targets(database, prefix, count)
        print("      Prepared disjoint clear-range targets before CDC registration.")

        # 2. Bridge Service Configuration & Daemon Launch
        print(
            "\n[2/7] Kafka Bridge Service: Declarative Configuration & Daemon Startup"
        )
        print(f"      Stream Name       : '{stream_name}'")
        print(f"      Key Range         : [{prefix!r}, {strinc(prefix)!r})")
        print(f"      Kafka Target      : '{topic}' on {bootstrap_servers}")
        print("      Delivery Policy   : acks=all, enable.idempotence=True")
        print("      Retention Policy  : Advance minVersion only after Kafka ack")

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
        print("      Registered CDC stream in FDB transaction subsystem.")

        # Run the Bridge daemon loop in a background thread
        bridge_errors: list[BaseException] = []

        def _run_bridge() -> None:
            try:
                assert bridge is not None
                bridge.run()
            except BaseException as exc:
                bridge_errors.append(exc)

        bridge_thread = threading.Thread(
            target=_run_bridge, daemon=True, name="BridgeDaemon"
        )
        bridge_thread.start()
        print("      Started autonomous Kafka Bridge daemon thread.")

        # Brief warm-up to ensure CDC consumer handle is armed
        time.sleep(1.0)

        # 3. Transactional Writes to FoundationDB
        print(f"\n[3/7] Upstream Application: Committing {count:,} mutations to FDB")
        print("      Exercising representative writes and atomic operations...")
        stream_start = time.monotonic()
        write_duration = _write_samples(database, prefix, count)
        write_rate = count / max(write_duration, 0.001)
        print(
            f"      Committed {count:,} mutations in {write_duration:.3f}s "
            f"({write_rate:,.0f} writes/sec)"
        )

        # 4. Bridge Streaming & Telemetry
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

        # 5. Downstream Kafka Consumer Ingestion
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

        # 6. Workload Verification
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

        # 7. Graceful Bridge Shutdown
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


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line demo.

    Args:
        argv: Optional command-line arguments.

    Returns:
        Process exit code.
    """
    args = _arguments(argv)
    run_demo(args.kafka_bootstrap_servers, args.timeout, args.count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
