#!/usr/bin/env bash
# Failover: SIGKILL the leader under load and measure (a) time until another
# node is leader and (b) the longest append latency clients saw, which is
# the write unavailability window. Repeated per tick setting.
#
#   bench/failover.sh [OUTDIR] [TRIALS]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-bench/results/failover}
TRIALS=${2:-5}
mkdir -p "$OUT"
go build -o bin/ ./cmd/...
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
for tick in 100ms 20ms; do
  for trial in $(seq 1 "$TRIALS"); do
    scripts/cluster.sh wipe >/dev/null 2>&1 || true
    MLOGD_FLAGS="--tick $tick" scripts/cluster.sh start 3 >/dev/null
    eps=$(scripts/cluster.sh endpoints)
    bin/mlogctl --endpoints "$eps" load --producers 4 --events 4000 --batch 20 >"$OUT/load-$tick-$trial.txt" 2>&1 &
    load=$!
    sleep 1.5
    old=$(bin/mlogctl --endpoints "$eps" leader)
    t0=$(now_ms)
    scripts/cluster.sh kill "$old" >/dev/null
    until new=$(bin/mlogctl --endpoints "$eps" --timeout 1s leader 2>/dev/null) && [ "$new" != "$old" ]; do sleep 0.02; done
    t1=$(now_ms)
    wait "$load" || true
    maxlat=$(sed -n 's/.*max=\(.*\)$/\1/p' "$OUT/load-$tick-$trial.txt")
    echo "tick=$tick trial=$trial new_leader_after_ms=$((t1 - t0)) max_append_latency=$maxlat" | tee -a "$OUT/summary.txt"
  done
done
scripts/cluster.sh wipe >/dev/null 2>&1 || true
