# Kafka-FDB-Streamline Benchmarks

Automated benchmarks measuring throughput, commit-to-ack latency, CDC lag, and crash-resilience across releases of Kafka-FDB-Streamline.

---

## Releases & Results

| Release Version | Focus | Benchmark Summary | Documentation |
|---|---|---|---|
| **v0.1.0** | Core Pipeline & Autonomous Bridge | • **56,683 mut/sec** peak saturation ceiling.<br>• **9,450,500 mutations** over 10-min sustained NF1 run with 0 backlog growth.<br>• **15,917 atomic disk checkpoints** with zero error retries.<br>• Verified zero duplicates, zero gaps, zero order violations. | [`0.1.0v/README.md`](0.1.0v/README.md) |
| **v0.2.0** *(Planned)* | Directory Layer, FDB Watch Failover & Multi-Client Sharding | Target: Subspace auto-mapping, sub-3s active-passive failover via `tr.watch()`, multi-worker 100k+ mut/sec scaling. | *In development* |

---

## Directory Layout

* [`0.1.0v/`](0.1.0v/): Complete v0.1.0 benchmark results, JSON artifacts, and harness script.

