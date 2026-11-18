#!/usr/bin/env bash
# P9 run sequence. Needs live-up.sh already run against this worktree (containers
# kfsbench-fdb + kfsbench-kafka). Usage: run_p9.sh <phase> where phase is one of
#   smoke | aa | drain | sustained | all
# Raw JSON lands in metrics-results/P9/raw/<phase>/.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
wt=$(cd "$here/../.." && pwd)
out_host=$wt/.wayfinder/concurrent-cdc/research/stage-profile   # mounted at /out
sha=$(git -C "$wt" rev-parse HEAD)
boot=kfsbench-kafka:19092
cp "$here/bridge_tput.py" "$out_host/bridge_tput.py"

run() {  # run <phase-dir> <name> <args...>
  local dir=$1 name=$2; shift 2
  mkdir -p "$out_host/raw/p9/$dir" "$here/raw/$dir"
  echo "== $dir/$name: $*"
  docker exec -w /out -e PYTHONPATH=/repo:/repo/src:/repo/protobuf/gen kfsbench-fdb \
    python3 bridge_tput.py "$@" --bootstrap "$boot" --repo-sha "$sha" \
    --out "raw/p9/$dir/$name.json"
  cp "$out_host/raw/p9/$dir/$name.json" "$here/raw/$dir/"
}

fdb_status() { docker exec kfsbench-fdb fdbcli --exec "status minimal" --timeout 10; }

phase=${1:?phase: smoke|aa|drain|sustained|all}
case $phase in
  smoke)  # ~1 min: proves every mode runs end to end; not a result
    run smoke drain drain --preload 3000 --timeout 60
    run smoke sustained sustained --rate 2000 --warmup 1 --duration 3
    run smoke kafka kafka-alone --duration 2 ;;
  aa)  # A/A: byte counter off vs on
    for i in 1 2; do
      run aa "off_$i" drain --no-byte-counter
      run aa "on_$i" drain
    done; fdb_status ;;
  drain)  # interleaved drain / kafka-alone, then flush-at-end
    for i in 1 2 3 4 5; do
      run drain "drain_$i" drain
      run drain "kafka_$i" kafka-alone
    done
    for i in 1 2 3 4 5; do run drain "kafka_fae_$i" kafka-alone --flush-at-end; done
    fdb_status ;;
  sustained)  # rotate rate order per trial
    rates=(15000 25000 35000 45000)
    for i in 0 1 2 3 4; do
      for j in 0 1 2 3; do
        r=${rates[$(( (i + j) % 4 ))]}
        run sustained "s${r}_$((i + 1))" sustained --rate "$r"
      done
      fdb_status
    done ;;
  all) "$0" aa; "$0" drain; "$0" sustained ;;
  *) echo "unknown phase $phase" >&2; exit 2 ;;
esac
