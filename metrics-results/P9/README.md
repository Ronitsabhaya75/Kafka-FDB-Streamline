# P9: bridge throughput vs a raw Kafka producer

**Status:** harness and unit tests written; no live runs yet. Results, table and
review date are filled in after the runs.

Question: how fast does `Bridge` (main @ `8fc47ec`, unmodified) move mutations
FDB -> Kafka, and what fraction of a raw `MutationProducer` is that, on the same
broker and record shape?

Authorship: harness agent-written and agent-run to Robbie McLaughlin's design;
reviewed by him on _<date, after runs>_. The bridge run loop is jay4617's (PR #31).

## Files

| file | what |
|---|---|
| `bridge_tput.py` | harness: `drain`, `sustained`, `kafka-alone` modes; one JSON per run |
| `test_bridge_tput.py` | unit tests for the pure helpers and `p9_table.py` |
| `p9_table.py` | `python3 p9_table.py raw/<phase>/*.json > table.md` |
| `run_p9.sh` | run sequence (`smoke`, `aa`, `drain`, `sustained`) |
| `live-up.sh` / `live-down.sh` | private throwaway Kafka 3.9 + FDB 8.0 (native CDC) |

Imports only the public package API (`from_dict`, `Bridge`, `FDBClient`, `strinc`,
`MutationProducer`, `serialize_version_group`, `max_record_bytes_for_key`,
`mutations_pb2`). Nothing from `benchmarks/0.1.0v/`. Lag sampling reads
`Bridge._step_lock` / `Bridge._listener` (non-blocking, between steps only).

## Definitions

- **Workload:** 500 SETs per FDB transaction, key = run prefix + 10-digit index
  (pool 500k), 100 B value = 19-digit `time_ns` + `|` + `x` padding.
- **drain:** `bridge.start()` (registers the stream), write `--preload` (1M),
  then `bridge.run()`; sample `(t, mutations_published, payload bytes)` every
  50 ms. Rate = slope between the 10% and 90% crossings of the final count.
- **sustained:** writer paced to `--rate` with the bridge running; after a 10 s
  warmup, 60 s of samples. Backlog = written - published; `backlog_growing` =
  OLS slope > 1% of the measured write rate (or nothing published). Lag = writer's
  last committed version - listener's `last_consumed_version`, sampled between
  steps; ms ~ versions / 1000. `drained` = published >= written after stop.
- **kafka-alone:** no FDB. One prebuilt reply of 6 version groups x 500 SETs
  (the shape a drain reply has: the proxy caps replies at 3,000 mutations),
  produced under the stream-name key, `flush_or_raise()` per reply as the
  bridge does. `--flush-at-end` flushes once at the end. Same producer defaults
  (acks=all, idempotent, linger 5 ms, lz4).
- **payload MB/s:** sum of `record.ByteSize()` at `produce()` (pre-lz4, no Kafka
  framing). **tx MB/s:** librdkafka `txbytes` slope (on-wire, compressed). Values
  are mostly `x`, so lz4 shrinks the wire a lot; quote payload MB/s.
- **records/reply:** `records_published / batches_acked` (drain) vs
  `len(reply)` (kafka-alone); must match within +-1.
- Per run: fresh stream, topic, key prefix and cursor file; stream removed and
  prefix cleared at the end.

Setup caveats: one partition, one broker, single-process memory-engine FDB under
Rosetta (linux/amd64 on an M1 Pro). Cite ratios first; absolute rates are
setup-specific. Drain ceiling is not steady-state ingest; label which is which.

## How to run

```bash
WT=<this worktree>
bash metrics-results/P9/live-up.sh $WT
bash metrics-results/P9/run_p9.sh smoke      # ~1-2 min, not a result
bash metrics-results/P9/run_p9.sh aa         # ~4 x 1M drains
bash metrics-results/P9/run_p9.sh drain
bash metrics-results/P9/run_p9.sh sustained  # 20 x ~75 s
python3 metrics-results/P9/p9_table.py metrics-results/P9/raw/drain/*.json metrics-results/P9/raw/sustained/*.json > metrics-results/P9/table.md
bash metrics-results/P9/live-down.sh
```

Acceptance: A/A (byte counter on vs off) inside run noise, else count bytes in
delivery callbacks; >=5 valid runs per mode/rate; records/reply match;
kafka-alone below drain means a measurement bug.

## Robbie's PR #31 review (2026-10-29) vs the merged `766b69e`

15 inline comments on `src/bridge/pipeline.py`, `docs/BRIDGE-PIPELINE-SPEC.md`
and `tests/bridge/test_pipeline.py`. The merge includes a commit titled
"address Robbie review comments on recovery, errors, and checkpointing".

| # | severity | suggestion | landed in `766b69e`? |
|---|---|---|---|
| 1 | major | non-fatal transactional abort left the pipeline stuck (listener still awaiting ack); document recovery or re-produce | yes, documented option: `step()` Raises says close the listener and resume from the last checkpoint |
| 2 | minor | module docstring wrongly claimed a later flush can't publish the partial batch | yes: docstring now says close flushes then purges, duplicates possible, at-least-once kept |
| 3 | minor | ack-only retry re-ran checkpoint; track `_checkpointed` | yes: `_checkpointed` flag |
| 4 | minor | spec items 5/7 conflicted on shutdown with a commit outstanding; `_commit_outstanding` private | yes: public `commit_outstanding`, spec item 7 defines shutdown |
| 5 | minor | non-bytes `partition_key` silently ignored | yes: `TypeError` for non-bytes |
| 6 | minor | `RuntimeError` from a fatal producer not caught on produce/flush | yes: caught, producer closed |
| 7 | minor | `step()` Raises incomplete | yes: CDC/serialization/checkpoint errors listed |
| 8 | nit | `_kafka_error`/`_flag` duplicate producer's fatal check | partly: uses the now-public `is_fatal_kafka_error`; helpers kept |
| 9 | nit | always-raising helper typed `-> None` | yes: `NoReturn`, renamed `_abort_and_raise` |
| 10 | nit | extract a shared `_retry` helper | no (marked low priority) |
| 11 | nit | `getattr` fallbacks for duck-typed listeners | no: fallbacks kept |
| 12 | minor | fake listener enforcing ack-before-poll + post-abort test | no |
| 13 | minor | checkpoint-failure retry test only covered idempotent mode | yes: parametrized over `transactional` |
| 14 | minor | add terminal-ack-error test | yes: `test_terminal_ack_error_propagates_immediately` |
| 15 | minor | assert checkpoint/get_position call counts on retry | yes |

Landed: 11 of 15 in full, 1 in part, 3 not. Robbie's review ended in "LGTM".

## Results

_Pending runs._
