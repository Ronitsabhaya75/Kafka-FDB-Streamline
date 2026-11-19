#!/usr/bin/env bash
# S3 coalescing grid in the real Bridge daemon. Usage: run_grid.sh <phase> [cell]
#   setup           copy the local stage-profile tool into this worktree and the
#                   control worktree ($CTL, created at $BASE), apply profile.diff
#   smoke           ~1 min sanity run (off + R10, 3 s each); not a result
#   grid            7 cells x 5 trials, order rotated per trial (~50 min)
#   control         ctl-off / ctl-R10 x5 on $BASE, the same code minus the
#                   run-loop fix (~15 min); run live-up.sh against $CTL first
#   lowload <cell>  low-off vs low-<cell> x5 at 100 mut/s (~8 min)
#   table           grid_table.py raw/*.json > table.md
#   teardown        remove the control worktree
# Needs live-up.sh run against the worktree the phase measures (this one, or $CTL
# for control). Raw JSON lands in metrics-results/P2-P4-P7/raw/.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
wt=$(cd "$here/../.." && pwd)
BASE=${BASE:-726af1a34fa1b3cbd1fc8a3c65c0ff41020d6f10}  # origin/dev/cdc-collosacing
CTL=${CTL:-$wt-ctl}
SP_REL=.wayfinder/concurrent-cdc/research/stage-profile
boot=kfsbench-kafka:19092
mkdir -p "$here/raw/invalid"

flags() {  # cell name -> profile.py flags
  local c=${1#ctl-}; c=${c#low-}; c=${c#smoke-}
  case $c in
    off) ;;
    R*) echo "--idle-ack-max-replies ${c#R}" ;;
    S*) echo "--idle-ack-max-seconds ${c#S}" ;;
    *) echo "unknown cell $1" >&2; exit 2 ;;
  esac
}

prepare() {  # prepare <worktree>; $KFS = checkout holding the local tool (read-only)
  local d=$1/$SP_REL
  : "${KFS:?set KFS to the checkout holding the local stage-profile tool}"
  mkdir -p "$(dirname "$d")"
  rm -rf "$d"
  cp -R "$KFS/$SP_REL" "$d"
  rm -rf "$d/raw" && mkdir "$d/raw"
  patch -s "$d/profile.py" "$here/profile.diff"
  cp "$here/latency_probe.py" "$d/"
}

preflight() {
  uptime
  docker ps --format '{{.Names}}\t{{.Status}}'
  if pgrep -f "python3 (profile|bridge_tput|bench_e2e)\.py" >/dev/null; then
    echo "another benchmark is running; wait until it finishes" >&2; exit 1
  fi
}

runs=0
run() {  # run <repo> <cell> <trial> <profile args...>: up to 3 attempts until valid
  local repo=$1 cell=$2 i=$3; shift 3
  local sha name=${cell}_${i}
  sha=$(git -C "$repo" rev-parse HEAD)
  for attempt in 1 2 3; do
    echo "== $name (attempt $attempt): $*"
    # shellcheck disable=SC2046  # flags are intentionally word-split
    docker exec -w /out -e PYTHONPATH=/repo:/repo/src:/repo/protobuf/gen kfsbench-fdb \
      python3 profile.py sustained "$@" $(flags "$cell") --bootstrap "$boot" \
      --cell "$cell" --repo-sha "$sha" --out "raw/$name.json"
    cp "$repo/$SP_REL/raw/$name.json" "$here/raw/$name.json"
    if python3 "$here/grid_table.py" --check "$here/raw/$name.json"; then break; fi
    mv "$here/raw/$name.json" "$here/raw/invalid/$name.$attempt.json"
  done
  runs=$((runs + 1))
  if ((runs % 10 == 0)); then
    docker exec kfsbench-fdb fdbcli --exec "status minimal" --timeout 10
  fi
}

load=(--rate 15000 --warmup 10 --duration "${DURATION:-60}")
phase=${1:?phase: setup|smoke|grid|control|lowload <cell>|table|teardown}
case $phase in
  setup)
    [ -d "$CTL" ] || git -C "$wt" worktree add --detach "$CTL" "$BASE"
    prepare "$wt"; prepare "$CTL" ;;
  smoke)
    preflight
    for c in off R10; do run "$wt" "smoke-$c" 1 --rate 2000 --warmup 1 --duration 3; done
    mkdir -p "$here/raw/smoke" && mv "$here"/raw/smoke-*.json "$here/raw/smoke/" ;;
  grid)
    preflight
    cells=(off R2 R4 R10 S0.02 S0.05 S0.1)
    for i in 0 1 2 3 4; do
      for j in "${!cells[@]}"; do
        run "$wt" "${cells[$(( (i + j) % ${#cells[@]} ))]}" $((i + 1)) "${load[@]}"
      done
    done ;;
  control)
    preflight
    for i in 1 2 3 4 5; do
      if ((i % 2)); then order=(ctl-off ctl-R10); else order=(ctl-R10 ctl-off); fi
      for c in "${order[@]}"; do run "$CTL" "$c" "$i" "${load[@]}"; done
    done ;;
  lowload)
    cell=${2:?lowload needs the chosen cell, e.g. R10}
    preflight
    for i in 1 2 3 4 5; do
      if ((i % 2)); then order=(low-off "low-$cell"); else order=("low-$cell" low-off); fi
      for c in "${order[@]}"; do run "$wt" "$c" "$i" --rate 100 --warmup 5 --duration 30; done
    done ;;
  table)
    python3 "$here/grid_table.py" "$here"/raw/*.json > "$here/table.md"
    cat "$here/table.md" ;;
  teardown)
    git -C "$wt" worktree remove --force "$CTL" ;;
  *) echo "unknown phase $phase" >&2; exit 2 ;;
esac
