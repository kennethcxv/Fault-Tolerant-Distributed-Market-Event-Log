#!/usr/bin/env bash
# Run a local multi-process cluster.
#
#   scripts/cluster.sh start [3|5]   build and start N nodes (default 3)
#   scripts/cluster.sh status        mlogctl status
#   scripts/cluster.sh kill ID       SIGKILL a node (crash, no shutdown)
#   scripts/cluster.sh stop ID       SIGTERM a node (clean shutdown)
#   scripts/cluster.sh restart ID    start a stopped or killed node again
#   scripts/cluster.sh down          stop every node
#   scripts/cluster.sh wipe          stop every node and delete all data
#
# Data and logs live in $MLOG_CLUSTER_DIR (default .cluster/). Extra mlogd
# flags can be passed through MLOGD_FLAGS, e.g. MLOGD_FLAGS="--fsync=never".
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=${MLOG_CLUSTER_DIR:-$ROOT/.cluster}
BASE_PORT=${MLOG_BASE_PORT:-7001}

size() { cat "$DIR/size" 2>/dev/null || echo 3; }
port() { echo $((BASE_PORT + $1 - 1)); }
endpoints() { for i in $(seq 1 "$(size)"); do printf "%s127.0.0.1:%d" "$([ "$i" -gt 1 ] && echo ,)" "$(port "$i")"; done; }
peers() { for i in $(seq 1 "$(size)"); do printf "%s%d=http://127.0.0.1:%d" "$([ "$i" -gt 1 ] && echo ,)" "$i" "$(port "$i")"; done; }
pid_of() { cat "$DIR/n$1.pid" 2>/dev/null || true; }
running() { local p; p=$(pid_of "$1"); [ -n "$p" ] && kill -0 "$p" 2>/dev/null; }

start_node() {
  local id=$1
  if running "$id"; then echo "node $id already running"; return; fi
  mkdir -p "$DIR/n$id"
  # shellcheck disable=SC2086
  nohup "$ROOT/bin/mlogd" --id "$id" --listen "127.0.0.1:$(port "$id")" --data "$DIR/n$id" \
    --peers "$(peers)" ${MLOGD_FLAGS:-} >>"$DIR/n$id.log" 2>&1 &
  echo $! >"$DIR/n$id.pid"
  echo "node $id started (pid $!, http://127.0.0.1:$(port "$id"), log $DIR/n$id.log)"
}

signal_node() {
  local p; p=$(pid_of "$2")
  if [ -z "$p" ] || ! kill -0 "$p" 2>/dev/null; then echo "node $2 is not running"; return; fi
  kill "-$1" "$p"
  while kill -0 "$p" 2>/dev/null; do sleep 0.05; done
  echo "node $2 stopped with SIG$1"
}

case "${1:-}" in
  start)
    n=${2:-3}
    if [ -f "$DIR/size" ] && [ "$(size)" != "$n" ]; then
      echo "existing cluster in $DIR has $(size) nodes; run wipe first" >&2; exit 1
    fi
    mkdir -p "$DIR" && echo "$n" >"$DIR/size"
    (cd "$ROOT" && go build -o bin/ ./cmd/...)
    for i in $(seq 1 "$n"); do start_node "$i"; done
    for _ in $(seq 1 100); do
      if leader=$("$ROOT/bin/mlogctl" --endpoints "$(endpoints)" --timeout 1s leader 2>/dev/null); then
        echo "leader is node $leader"; break
      fi
      sleep 0.1
    done
    echo "export MLOG_ENDPOINTS=$(endpoints)"
    ;;
  status) "$ROOT/bin/mlogctl" --endpoints "$(endpoints)" status ;;
  kill) signal_node KILL "$2" ;;
  stop) signal_node TERM "$2" ;;
  restart) start_node "$2" ;;
  down) for i in $(seq 1 "$(size)"); do signal_node TERM "$i"; done ;;
  wipe)
    for i in $(seq 1 "$(size)"); do signal_node TERM "$i"; done
    rm -rf "$DIR"; echo "removed $DIR"
    ;;
  endpoints) endpoints; echo ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
