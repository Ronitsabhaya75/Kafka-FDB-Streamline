# Kafka-FDB-Streamline v0.1.0 Benchmark Suite

This directory contains the complete end-to-end performance, endurance, and durability benchmarks for the **Kafka-FDB-Streamline v0.1.0** release.

The benchmark harness ([`bench_e2e.py`](bench_e2e.py)) runs a live concurrent writer workload against the FoundationDB CDC stream and the Kafka bridge pipeline, measuring real-path throughput, commit-to-ack latencies, CDC read-head lag, and disk checkpoint durability without simulated delays or mocked network calls.

Each results JSON is a **single run** on the environment below. Latency and lag vary across runs at the same target rate; the tables quote the committed artifact for that cell, not a multi-run aggregate.

---

## 1. Executive Summary

| Category | Result | Significance |
|---|---|---|
| **Peak Unthrottled Throughput** | **56,683 mutations/sec** (8.00 MB/s) | Achieved by a single writer thread without pipeline saturation (backlog slope -52 mut/s). |
| **10-Min Sustained Run** | **9,450,500 mutations total** (15,000 mut/s) | 600s steady-state window with flat backlog (+0.016 mut/s) in the core pipeline loop. Includes 30s warm-up in the total. |
| **End-to-End Latency (10-min)** | **p50 = 12.2 ms**, **p90 = 26.1 ms**, **p99 = 115.5 ms** | Client `tr.commit()` in FDB to Kafka broker ACK (`acks=all`). Max ≈ 0.99 s. |
| **CDC Read-Head Lag (10-min)** | **1,571 versions (~1.6 ms)** mean, **0.98 s** max | Polling loop trails the writer's last commit version during continuous ingestion. |
| **Full Bridge Checkpointing** | **15,917 atomic disk fsyncs** | Maintained ~15k mut/s with zero errors while fsyncing cursor on every batch. |
| **CDC Stream Ordering (`--verify`)** | **0 duplicates, 0 missing, 0 order violations** | Monotonic ordering verified across 1,050,000 polled mutations (`--key-pool 0`). |

---

## 2. Test Environment

* **Platform**: Linux x86_64 (`ubuntu:24.04` on Docker Desktop / linuxkit), 8 CPU Cores.
* **FoundationDB**: Version **8.0.0** (API version 800) with native CDC enabled (`--knob_enable_native_cdc=1`).
* **Apache Kafka**: Version **3.9.0** (`apache/kafka:3.9.0`), KRaft controller, single partition topic (single broker, `acks=all` — no multi-replica ISR).
* **Producer Durability**: `acks=all`, `enable.idempotence=true`, `enable.gapless.guarantee=true`, `compression.type=lz4`, `linger.ms=5`.
* **Payload Size**: 100-byte mutation values (stamping 19-digit UNIX nanosecond commit timestamp) + 27-byte keys (17-byte prefix + 10-byte index). Wire bandwidth is measured pre-compression.
* **Batch Size**: 500 mutations per FoundationDB transaction.

---

## 3. Metrics & Mathematical Formulas

The benchmark harness ([`bench_e2e.py`](bench_e2e.py)) calculates all operational metrics using deterministic mathematical formulas:

### 3.1. Backlog Definition

$$
\text{Backlog}(t) = \text{Written}(t) - \text{Acked}(t)
$$

Represents the exact count of committed FoundationDB mutations currently in-flight across the CDC proxy, serializer, Kafka broker queues, or unacknowledged by the bridge.

### 3.2. Backlog Trend Slope (Ordinary Least Squares Regression)

To determine whether the bridge is accumulating lag over time, linear regression is performed over $n$ sampling points $(t_i, B_i)$ within the steady-state evaluation window:

$$
\text{Slope} = \frac{\sum_{i=1}^n (t_i - \bar{t})(B_i - \bar{B})}{\sum_{i=1}^n (t_i - \bar{t})^2}
$$

Where:
* $t_i$: Timestamp of sample $i$ relative to the start of the measured window.
* $B_i = \text{Written}(t_i) - \text{Acked}(t_i)$: Backlog count at timestamp $t_i$.
* $\bar{t}$ and $\bar{B}$: Arithmetic means of sample timestamps and backlog counts.

**Backlog Stability Classification:**

$$
\text{Verdict} = \begin{cases}
\text{GROWING (Cannot keep up)} & \text{if } \text{Slope} > 0.01 \times \max(\text{WriteRate}, 1.0) \\
\text{Flat (Sustainable)} & \text{if } \text{Slope} \le 0.01 \times \max(\text{WriteRate}, 1.0)
\end{cases}
$$

### 3.3. Real-Path End-to-End Latency

The writer thread stamps a 19-digit UNIX nanosecond timestamp $t_{\text{commit}}$ into the first 19 bytes of the mutation value payload before calling `tr.commit().wait()`. Upon successful Kafka broker delivery (`flush()` completes at $t_{\text{flush}}`), latency is calculated:

$$
\text{Latency}_{\text{ms}} = \frac{t_{\text{flush}} - t_{\text{commit}}}{10^6}
$$

This measures the complete real-world latency chain from client commit to Kafka broker acknowledgment:

$$
\text{Latency}_{\text{E2E}} = \Delta t_{\text{FDB}} + \Delta t_{\text{CDC}} + \Delta t_{\text{Serialize}} + \Delta t_{\text{Kafka}}
$$

### 3.4. CDC Read-Head Version Lag

FoundationDB version lag measures the distance between the writer's latest committed transaction version ($V_{\text{committed}}$) and the bridge's latest acknowledged consumer position ($V_{\text{consumed}}$):

$$
\text{Lag}_{\text{versions}} = V_{\text{committed}} - V_{\text{consumed}}
$$

Approximate lag in seconds (based on FoundationDB's default rate of $10^6$ versions/sec):

$$
\text{Lag}_{\text{seconds}} \approx \frac{\text{Lag}_{\text{versions}}}{10^6}
$$

### 3.5. Wire Bandwidth

$$
\text{Bandwidth}_{\text{MB/s}} = \frac{\Delta \text{Bytes}}{1024 \times 1024 \times \Delta t}
$$

### 3.6. Nearest-Rank Latency Percentiles

For a sorted list of $N$ latency samples and target percentile $P \in [0, 100]$:

$$
\text{Rank}(P) = \max\left(0, \min\left(N - 1, \left\lceil \frac{P}{100} \times N \right\rceil - 1\right)\right)
$$

---

## 4. Results Matrix

| Benchmark Test | Mode | Total Volume | Target Rate | Measured Rate | Wire Bandwidth | Latency (p50 / p90 / p99) | Backlog Trend | Result File |
|---|---|---|---|---|---|---|---|---|
| **15k Baseline** | Core Pipeline | 1,050,000 | 15,000 mut/s | 15,000 mut/s | 2.11 MB/s | 13.0ms / 64.4ms / 316.2ms | Flat (+2 mut/s) | [`results_15k.json`](results_15k.json) |
| **20k Baseline** | Core Pipeline | 1,400,500 | 20,000 mut/s | 19,992 mut/s | 2.84 MB/s | 11.9ms / 17.5ms / 49.8ms | Flat (-0 mut/s) | [`results_20k.json`](results_20k.json) |
| **25k Baseline** | Core Pipeline | 1,750,500 | 25,000 mut/s | 25,000 mut/s | 3.54 MB/s | 12.1ms / 24.1ms / 59.3ms | Flat (+1 mut/s) | [`results_25k.json`](results_25k.json) |
| **Unthrottled Peak** | Core Pipeline (Single Writer) | 2,077,500 | Unthrottled | **56,683 mut/s** | **8.00 MB/s** | 36.7ms / 62.4ms / 116.6ms | Flat (-52 mut/s) | [`results_unthrottled.json`](results_unthrottled.json) |
| **Stream Verification** | Core Pipeline (`--verify --key-pool 0`) | 1,050,000 | 15,000 mut/s | 15,000 mut/s | 2.13 MB/s | 12.0ms / 15.4ms / 39.4ms | Flat (-0 mut/s) | [`results_15k_verified.json`](results_15k_verified.json) |
| **10-Min Sustained Run** | Core Pipeline (Loop only) | **9,450,500** | 15,000 mut/s | **15,000 mut/s** | **2.13 MB/s** | **12.2ms / 26.1ms / 115.5ms** | **Flat (+0.016 mut/s)** | [`results_10min_sustained.json`](results_10min_sustained.json) |
| **Autonomous Bridge** | Full Daemon (`--use-bridge`) | 1,050,500 | 15,000 mut/s | 14,992 mut/s | n/a | n/a | Flat (+1 mut/s) | [`results_bridge_15k.json`](results_bridge_15k.json) |

---

## 5. Key Engineering Insights

### 5.1. 10-Minute Sustained Ingestion (Core Pipeline Loop)
Evaluates the core ingestion loop without cursor disk persistence against continuous foreground writers for 10 minutes (600.0s steady-state window + 30.0s warmup) at 15,000 mut/s:

* Over the 600.0-second window, the backlog trend slope was **`+0.016 mut/s`** (linear regression; treated as flat by the 1% write-rate threshold).
* Totals for the whole run (warmup included): written `9,450,500`; acknowledged `9,450,500`. The 600s measured window alone is about 9.0M mutations at 15k/s.
* **Drain time after writes stopped was 0.059 seconds**, confirming that the core ingestion loop kept pace with the foreground writers through the 10-minute window.
* Latency in this artifact: p50 **12.2 ms**, p90 **26.1 ms**, p99 **115.5 ms**, max **990 ms**. Mean CDC lag **1,571 versions** (~1.6 ms); max lag ≈ **0.98 s**.

### 5.2. Atomic Checkpoint Durability Overhead
In **Benchmark 2** (`--use-bridge`), every CDC batch was processed through the complete `fdb_kafka_bridge.bridge.Bridge` daemon lifecycle:
```
[FDB Poll] ──> [Protobuf Serialize] ──> [Kafka Broker Flush] 
           ──> [f.flush() + os.fsync(cursor_file)] ──> [atomic os.replace()] 
           ──> [os.fsync(parent_dir)] ──> [FDB acknowledge()]
```
* **15,917 discrete synchronous disk fsync operations** were completed before advancing the FDB watermark (`batches_acked`; watermark-only replies also count).
* The bridge maintained **14,992 mutations/sec** with **zero errors** and an average version lag of **1,624 versions (~1.6 ms)**.
* The committed cursor file recorded `stream_id` and `last_consumed_version`. The harness now compares that version to the listener position before teardown (`cursor_matches_listener` in new runs).

### 5.3. CDC Stream Sequence and Key Monotonicity
Under `--verify` mode with an unbounded key pool (`--key-pool 0`), the harness tracked sequence numbers and key existence across **1,050,000** mutations polled from the CDC stream:
* `duplicates`: **0**
* `missing`: **0**
* `order_violations`: **0**
* Verifies monotonic key and version sequence continuity at the CDC listener interface. Full end-to-end downstream Kafka consumer payload verification is demonstrated in `demo/run_demo.py` via `verify_workload`.

---

## 6. How to Reproduce

All benchmarks run inside the project's devcontainer against live FoundationDB 8.0 and Kafka 3.9.0 containers.

### Start the Stack
```bash
docker compose -f .devcontainer/docker-compose.yml up -d --build
docker compose -f .devcontainer/docker-compose.yml exec app \
  pip install --break-system-packages -e .
```

Wait until FDB and Kafka are accepting connections before running benchmarks.

### 1. Run the 10-Minute Sustained Benchmark
```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
    python3 benchmarks/0.1.0v/bench_e2e.py \
        --kafka-bootstrap-servers kafka:19092 \
        --duration 600 --warmup 30 --target-rate 15000 \
        --output benchmarks/0.1.0v/results_10min_sustained.json
```

### 2. Run the Full Autonomous Bridge Checkpoint Benchmark
```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
    python3 benchmarks/0.1.0v/bench_e2e.py \
        --use-bridge \
        --kafka-bootstrap-servers kafka:19092 \
        --duration 60 --warmup 10 --target-rate 15000 \
        --output benchmarks/0.1.0v/results_bridge_15k.json
```

### 3. Run CDC Stream Ordering Verification
```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
    python3 benchmarks/0.1.0v/bench_e2e.py \
        --kafka-bootstrap-servers kafka:19092 \
        --duration 60 --warmup 10 --target-rate 15000 \
        --key-pool 0 --verify \
        --output benchmarks/0.1.0v/results_15k_verified.json
```

### 4. Run Unthrottled Throughput Test
```bash
docker compose -f .devcontainer/docker-compose.yml exec app \
    python3 benchmarks/0.1.0v/bench_e2e.py \
        --kafka-bootstrap-servers kafka:19092 \
        --duration 30 --warmup 5 --target-rate 0 \
        --output benchmarks/0.1.0v/results_unthrottled.json
```

---

## 7. Artifact Files

* [`bench_e2e.py`](bench_e2e.py): Unified benchmark runner supporting pipeline mode, bridge mode, live heartbeat monitoring, and integrity verification.
* [`results_15k.json`](results_15k.json): 15,000 mut/s 1-minute baseline test.
* [`results_20k.json`](results_20k.json): 20,000 mut/s 1-minute baseline test.
* [`results_25k.json`](results_25k.json): 25,000 mut/s 1-minute baseline test.
* [`results_unthrottled.json`](results_unthrottled.json): 56,683 mut/s unthrottled single-writer run.
* [`results_15k_verified.json`](results_15k_verified.json): 1,050,000 mutation CDC ordering verification run.
* [`results_10min_sustained.json`](results_10min_sustained.json): 9,450,500 mutation 10-minute sustained core-loop run.
* [`results_bridge_15k.json`](results_bridge_15k.json): 15,917 disk fsync autonomous daemon run.
