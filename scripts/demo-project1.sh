#!/usr/bin/env bash
# The Project 1 pipeline end to end: a C++ engine writes market events to a
# spool through the non-blocking ring, mlog-publisher tails the spool into a
# three-node cluster, and the leader is killed midway. The engine never
# waits; the spool absorbs the outage; nothing is lost or duplicated.
set -euo pipefail
cd "$(dirname "$0")/.."
EVENTS=${EVENTS:-1000000}
RATE=${RATE:-100000}
work=$(mktemp -d)
c++ -std=c++17 -O2 -pthread -o "$work/engine" integration/cpp/example_engine.cpp
scripts/cluster.sh wipe >/dev/null 2>&1 || true
scripts/cluster.sh start 3
export MLOG_ENDPOINTS=$(scripts/cluster.sh endpoints)

: >"$work/engine.spool"
bin/mlog-publisher --spool "$work/engine.spool" --producer 7 --endpoints "$MLOG_ENDPOINTS" \
  --metrics-listen 127.0.0.1:9101 2>"$work/publisher.log" &
pub=$!
echo ">>> engine: $EVENTS events at $RATE/s into $work/engine.spool"
"$work/engine" "$work/engine.spool" 7 "$EVENTS" "$RATE" >"$work/engine.out" &
engine=$!
sleep 3
leader=$(bin/mlogctl leader)
echo ">>> SIGKILL leader $leader while the engine is writing"
scripts/cluster.sh kill "$leader"
sleep 2
curl -s 127.0.0.1:9101/metrics | grep '^mlog_publisher' || true
scripts/cluster.sh restart "$leader"
wait "$engine"
cat "$work/engine.out"
written=$(sed -n 's/.*written=\([0-9]*\).*/\1/p' "$work/engine.out")
echo ">>> waiting for the publisher to drain $written records"
# Prometheus prints large values as floats (1e+06), so compare numerically.
acked() { curl -s 127.0.0.1:9101/metrics | awk '/^mlog_publisher_acked_seq/{printf "%d", $2}'; }
start=$(date +%s)
until [ "$(acked)" -ge "$written" ]; do sleep 0.5; done
echo ">>> drained in $(( $(date +%s) - start ))s after the engine finished"
kill "$pub"
curl -s 127.0.0.1:9101/metrics 2>/dev/null | grep '^mlog_publisher' || true
bin/mlogctl producer 7
bin/mlogctl verify
echo "publisher retry warnings: $(grep -c '"append failing' "$work/publisher.log" || true)"
