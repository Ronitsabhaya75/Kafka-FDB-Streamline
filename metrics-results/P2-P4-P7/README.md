# P2-P4-P7: idle-ack coalescing in the Bridge daemon (acks vs latency)

Status: harness and tests only. **No grid runs yet**; `table.md` and `raw/` come later.

## Branch and credit

- Base: `origin/dev/cdc-collosacing` @ `726af1a`. Its `cdc.idle_ack_max_replies` /
  `cdc.idle_ack_max_seconds` `CDCConfig` wiring and graceful-shutdown flush are teammates'
  work (`ae1b524`, `b2f454f`; PR #47 / #50, unmerged). This branch does not change them.
- Added here: the run-loop fix (own commit) and this measurement harness.
- Not merged anywhere; prototype on a pushed branch.

## Run-loop fix

With coalescing on, `poll_records()` returns `[]` when it withholds an idle reply,
`step()` returns 0, and `Bridge.run()` used to sleep its empty-step backoff
(>= `backoff_base_seconds` 0.1 s, x1.5 per empty step, cap 2 s). At 15k mut/s that
meant several naps per second, data batched behind each nap, and the coalescing
thresholds rarely fired: ack cuts measured that way were mostly sleep. The fix:
`FDBMutationListener.withheld_idle_reply`, and `run()` resets backoff and polls again
when it is `True`. A genuinely empty reply still backs off. The control cells below
(base build, same config, old loop) quantify what changed.

## Method

Real `Bridge` daemon (idempotent producer, file cursor with fsync) driven by the local
stage-profile tool `profile.py` (not in the repo; `profile.diff` is the change to it).
Writer: 500 SETs/txn, 100 B values, key pool 500k (`benchmarks/0.1.0v/bench_e2e.py`
`Writer`, used as a load tool only).

- **Knobs** via `BridgeConfig` (`--idle-ack-max-replies`, `--idle-ack-max-seconds`),
  no monkeypatch, in both builds.
- **Latency**: newest mutation's client-write timestamp (embedded in the value) ->
  `BridgePipeline._produce_and_flush` return (`latency_probe.py`). Includes FDB commit
  time. Same definition as `bench_e2e.py` listener mode.
- **Acks/s**: steps where `FDBMutationListener.acknowledge` ran (idle flushes count).
- **Per run**: acks/s, ack+checkpoint share of wall, zero-publish steps/s, gap share,
  consume p50 (evidence for/against the unverified hypothesis that proxy reply cadence
  is ack-driven), delivered mut/s, latency p50/p90/p99. Window: after warmup.
- **Valid run**: `drained` (bridge published everything written) and delivered mut/s
  within 2% of offered (at low rates, within two writer txns). `run_grid.sh` retries
  an invalid run up to 3 times; rejects go to `raw/invalid/`.

| cell | build | setting |
|---|---|---|
| off | this branch | none |
| R2, R4, R10 | this branch | `idle_ack_max_replies` 2 / 4 / 10 |
| S0.02, S0.05, S0.1 | this branch | `idle_ack_max_seconds` 0.02 / 0.05 / 0.1 |
| ctl-off, ctl-R10 | base `726af1a` (old loop) | none / replies 10 |
| low-off, low-<cell> | this branch, 100 mut/s | none / chosen cell |

15k mut/s, `--warmup 10 --duration 60`, 5 trials per cell, cell order rotated per trial.

## Picking a setting (G4, applied by `grid_table.py`)

**Neutral** = the cell's median p50 *and* median p99 both inside the same-build `off`
runs' [min, max]. "Within observed noise", not "proven equal". Among neutral cells,
cite the biggest ack cut vs the off median, with the whole table. If none is neutral:
report the biggest ack cut as a trade-off with its p50 cost. If off's p99 spread is
> 2x, re-run with `DURATION=120`. Check the chosen cell at low load too.

## How to run (one benchmark session at a time on the machine)

```bash
WT=<this worktree>; R=$WT/metrics-results/P2-P4-P7
KFS=<checkout with the local stage-profile tool> $R/run_grid.sh setup   # + control worktree $WT-ctl
$R/live-up.sh $WT && $R/run_grid.sh smoke           # ~3 min incl. broker start
$R/run_grid.sh grid                                 # ~50 min unattended
$R/live-down.sh && $R/live-up.sh $WT-ctl && $R/run_grid.sh control   # ~17 min
$R/live-down.sh && $R/live-up.sh $WT
$R/run_grid.sh table                                # prints the chosen cell
$R/run_grid.sh lowload <cell>                       # ~8 min; R10 if none neutral
$R/run_grid.sh table && $R/live-down.sh && $R/run_grid.sh teardown
```

Tests (no FDB/Kafka): `PYTHONPATH=metrics-results/P2-P4-P7:src:protobuf/gen pytest metrics-results/P2-P4-P7`.

## Caveats

Single-process memory-engine FDB 8.0 under Rosetta, one Kafka broker, one partition.
Lead with ratios; absolute ms and mut/s are setup-specific. Record machine, Docker
CPU/RAM, versions (printed by `live-up.sh`) and SHAs here with the results.
