#!/usr/bin/env bash
# Publisher throughput by batch size: the C++ engine writes EVENTS records
# to a spool (unpaced, before the publisher starts), then mlog-publisher
# drains it into a 3-node cluster.
#
#   bench/publisher.sh [OUTDIR] [EVENTS]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-bench/results/publisher}
EVENTS=${2:-500000}
mkdir -p "$OUT"
go build -o bin/ ./cmd/...
work=$(mktemp -d)
c++ -std=c++17 -O2 -pthread -o "$work/engine" integration/cpp/example_engine.cpp
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
for batch in 100 500 2000 10000; do
  scripts/cluster.sh wipe >/dev/null 2>&1 || true
  scripts/cluster.sh start 3 >/dev/null
  eps=$(scripts/cluster.sh endpoints)
  rm -f "$work/s.spool"
  "$work/engine" "$work/s.spool" 7 "$EVENTS" 1000000 >/dev/null   # paced at 1M/s so nothing drops
  t0=$(now_ms)
  bin/mlog-publisher --spool "$work/s.spool" --producer 7 --endpoints "$eps" --batch "$batch" 2>/dev/null &
  pub=$!
  until [ "$(bin/mlogctl --endpoints "$eps" producer 7 | awk -F': ' '/last_seq/{gsub(",","",$2); print $2}')" = "$EVENTS" ]; do sleep 0.1; done
  t1=$(now_ms)
  kill $pub
  ms=$((t1 - t0))
  echo "batch=$batch events=$EVENTS drain_ms=$ms events_per_s=$((EVENTS * 1000 / ms))" | tee -a "$OUT/summary.txt"
done
scripts/cluster.sh wipe >/dev/null 2>&1 || true
