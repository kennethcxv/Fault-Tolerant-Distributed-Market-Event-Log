#!/usr/bin/env bash
# Recovery: (1) how long a restarted follower takes to catch up when it only
# needs log entries, and (2) when the leader has compacted past it and it
# needs a snapshot of N events. Also reports snapshot size on disk.
#
#   bench/recovery.sh [OUTDIR] [EVENTS]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-bench/results/recovery}
EVENTS=${2:-1000000}
mkdir -p "$OUT"
go build -o bin/ ./cmd/...
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
applied_of() { bin/mlogctl --endpoints "127.0.0.1:$((7000 + $1))" --timeout 2s status 2>/dev/null | awk -v id="$1" '$2==id{print $7}'; }

for mode in log snapshot; do
  scripts/cluster.sh wipe >/dev/null 2>&1 || true
  if [ $mode = log ]; then flags="--snapshot-entries 0"; else flags="--snapshot-entries 200 --snapshot-keep 50"; fi
  MLOGD_FLAGS="$flags" scripts/cluster.sh start 3 >/dev/null
  eps=$(scripts/cluster.sh endpoints)
  leader=$(bin/mlogctl --endpoints "$eps" leader)
  victim=$((leader % 3 + 1))
  scripts/cluster.sh stop "$victim" >/dev/null
  bin/mlogctl --endpoints "$eps" load --producers 4 --events $((EVENTS / 4)) --batch 1000 >"$OUT/load-$mode.txt"
  target=$(applied_of "$leader")
  t0=$(now_ms)
  MLOGD_FLAGS="$flags" scripts/cluster.sh restart "$victim" >/dev/null
  until [ "$(applied_of "$victim")" = "$target" ]; do sleep 0.05; done
  t1=$(now_ms)
  installed=$(grep -c '"installed snapshot"' ".cluster/n$victim.log" || true)
  disk=$(du -sk ".cluster/n$victim" | cut -f1)
  echo "mode=$mode events=$EVENTS catch_up_ms=$((t1 - t0)) snapshots_installed=$installed follower_disk_kb=$disk" | tee -a "$OUT/summary.txt"
  grep -h '"received snapshot"\|"installed snapshot"\|"snapshot sent"' .cluster/n*.log | cut -c1-200 >>"$OUT/snapshot-log-$mode.txt" || true
done
scripts/cluster.sh wipe >/dev/null 2>&1 || true
