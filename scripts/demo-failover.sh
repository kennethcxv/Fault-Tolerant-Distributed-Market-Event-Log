#!/usr/bin/env bash
# SIGKILL the leader while four producers are writing, restart it, and show
# that the producers finish without losing or duplicating anything and every
# node ends with the same log.
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/cluster.sh wipe >/dev/null 2>&1 || true
scripts/cluster.sh start 3
export MLOG_ENDPOINTS=$(scripts/cluster.sh endpoints)

echo ">>> 4 producers x 10000 events, batches of 50"
bin/mlogctl --timeout 300s load --producers 4 --events 10000 --batch 50 &
load=$!
sleep 1
leader=$(bin/mlogctl leader)
echo ">>> SIGKILL leader $leader"
scripts/cluster.sh kill "$leader"
sleep 2
bin/mlogctl status
echo ">>> restart node $leader"
scripts/cluster.sh restart "$leader"
wait "$load"
sleep 2
echo ">>> after recovery"
bin/mlogctl status
bin/mlogctl verify
bin/mlogctl replay
