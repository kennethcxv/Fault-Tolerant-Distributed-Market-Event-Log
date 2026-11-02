#!/usr/bin/env bash
# Isolate the leader with the fault-injection endpoint. The isolated node
# cannot acknowledge a write; the majority elects a new leader and keeps
# accepting writes; after healing, the old leader catches up.
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/cluster.sh wipe >/dev/null 2>&1 || true
scripts/cluster.sh start 3
export MLOG_ENDPOINTS=$(scripts/cluster.sh endpoints)
bin/mlogctl load --producers 2 --events 1000 --batch 50

old=$(bin/mlogctl leader)
old_ep=127.0.0.1:$((7000 + old))
echo ">>> isolate leader $old"
bin/mlogctl fault isolate "$old"
sleep 2
bin/mlogctl status

echo ">>> write directly to the isolated node (must not be acknowledged)"
if bin/mlogctl --endpoints "$old_ep" --timeout 3s append --producer 999 --seq 1 "minority write"; then
  echo "UNEXPECTED: the isolated node acknowledged a write" >&2
  exit 1
fi

echo ">>> the majority keeps accepting writes"
bin/mlogctl load --producers 2 --producer-base 10 --events 1000 --batch 50

echo ">>> heal"
bin/mlogctl fault heal
sleep 3
bin/mlogctl status
bin/mlogctl verify
