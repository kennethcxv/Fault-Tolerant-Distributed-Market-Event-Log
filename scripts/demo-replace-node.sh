#!/usr/bin/env bash
# Replace a node whose disk is gone: SIGKILL node 3 and delete its data,
# remove it from the membership, start node 4 with an empty directory as a
# joining member, add it, and check every node holds the same log. Writes
# continue throughout. (Never restart the old id with an empty disk: it has
# forgotten its vote and could vote twice in one term.)
set -euo pipefail
cd "$(dirname "$0")/.."
MLOGD_FLAGS="--snapshot-entries 100 --snapshot-keep 20" scripts/cluster.sh wipe >/dev/null 2>&1 || true
MLOGD_FLAGS="--snapshot-entries 100 --snapshot-keep 20" scripts/cluster.sh start 3
export MLOG_ENDPOINTS=$(scripts/cluster.sh endpoints)
bin/mlogctl load --producers 2 --events 20000 --batch 100 | sed -n 2p

echo ">>> node 3's disk is lost"
scripts/cluster.sh kill 3
rm -rf .cluster/n3
bin/mlogctl member remove 3
bin/mlogctl load --producers 2 --producer-base 10 --events 20000 --batch 100 | sed -n 2p

echo ">>> add node 4 and start it with an empty directory"
add_out=$(bin/mlogctl member add 4 127.0.0.1:7004)
echo "$add_out"
cluster_id=$(bin/mlogctl member list | sed -n 's/cluster id: \([^ ]*\).*/\1/p')
mkdir -p .cluster/n4
nohup bin/mlogd --id 4 --listen 127.0.0.1:7004 --data .cluster/n4 --join --cluster-id "$cluster_id" \
  --peers "1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,4=http://127.0.0.1:7004" \
  --snapshot-entries 100 --snapshot-keep 20 >>.cluster/n4.log 2>&1 &
echo $! >.cluster/n4.pid
export MLOG_ENDPOINTS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7004
bin/mlogctl load --producers 2 --producer-base 20 --events 5000 --batch 100 | sed -n 2p
sleep 2
bin/mlogctl member list
bin/mlogctl status
bin/mlogctl verify
grep -h '"installed snapshot"' .cluster/n4.log | cut -c1-160 || true
kill "$(cat .cluster/n4.pid)"
scripts/cluster.sh wipe >/dev/null
