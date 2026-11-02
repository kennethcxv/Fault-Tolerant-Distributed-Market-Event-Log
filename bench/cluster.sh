#!/usr/bin/env bash
# Cluster benchmarks: throughput and client-side append latency for each
# configuration and batch size, using `mlogctl load` (closed-loop producers,
# synthetic schema-v1 order-book events of ~25-40 bytes each).
#
#   bench/cluster.sh [OUTDIR]
#
# Everything runs on one machine: nodes, load generator and network
# (loopback). Results are raw tool output plus one summary line per run.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-bench/results/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
go build -o bin/ ./cmd/...
{
  echo "date: $(date -u +%FT%TZ)"
  echo "commit: $(git rev-parse --short HEAD)"
  echo "go: $(go version)"
  echo "os: $(uname -srm)"
  sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize 2>/dev/null || nproc
} >"$OUT/environment.txt"

run() { # name nodes fsync producers batch events_per_producer
  local name=$1 nodes=$2 fsync=$3 producers=$4 batch=$5 events=$6
  scripts/cluster.sh wipe >/dev/null 2>&1 || true
  if [ "$nodes" = standalone ]; then
    mkdir -p .cluster/n1 && echo 1 >.cluster/size
    nohup bin/mlogd --id 1 --standalone --listen 127.0.0.1:7001 --data .cluster/n1 --fsync "$fsync" \
      >>.cluster/n1.log 2>&1 &
    echo $! >.cluster/n1.pid
    eps=127.0.0.1:7001
    sleep 0.5
  else
    MLOGD_FLAGS="--fsync $fsync" scripts/cluster.sh start "$nodes" >/dev/null
    eps=$(scripts/cluster.sh endpoints)
  fi
  local log="$OUT/$name.txt"
  bin/mlogctl --endpoints "$eps" load --producers "$producers" --events "$events" --batch "$batch" >"$log" 2>&1
  local leader_ep
  leader_ep=$(bin/mlogctl --endpoints "$eps" status | awk '$3=="leader"||$3=="standalone"{print $1; exit}')
  curl -s "$leader_ep/debug/storage" >>"$log"; echo >>"$log"
  du -sk .cluster/n1 | awk '{print "disk_kb_node1 " $1}' >>"$log"
  local tput lat
  tput=$(sed -n 's/.*throughput=\([0-9]*\).*/\1/p' "$log")
  lat=$(sed -n 's/append latency //p' "$log")
  printf "%-28s nodes=%-10s fsync=%-6s producers=%-3s batch=%-5s events/s=%-8s %s\n" \
    "$name" "$nodes" "$fsync" "$producers" "$batch" "$tput" "$lat" | tee -a "$OUT/summary.txt"
}

# Configuration x batch size, 8 producers.
for cfg in "standalone always" "standalone never" "1 always" "3 always" "3 never" "5 always"; do
  set -- $cfg
  for batch in 1 10 100 1000; do
    case $batch in 1) ev=1500 ;; 10) ev=10000 ;; 100) ev=40000 ;; 1000) ev=100000 ;; esac
    run "n$1-$2-b$batch" "$1" "$2" 8 "$batch" "$ev"
  done
done
# Concurrency sweep on the default deployment.
for p in 1 4 16 64; do
  run "n3-always-b100-p$p" 3 always "$p" 100 $((320000 / p))
done
scripts/cluster.sh wipe >/dev/null 2>&1 || true
echo "results in $OUT"
