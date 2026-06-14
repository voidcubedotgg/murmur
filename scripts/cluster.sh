#!/usr/bin/env bash
# cluster.sh — boot a local murmur gossip cluster for demos.
#
# It first KILLS any orphaned murmur processes (the #1 cause of "missing nodes":
# a leftover process holding a port makes the new agent fail to bind and exit,
# which silently drops it — or the whole bootstrap — out of the cluster).
#
# Usage:
#   scripts/cluster.sh up [N]     boot control + N agents (default 3, fake VMM)
#   scripts/cluster.sh down        kill everything
#   scripts/cluster.sh nodes       show membership
#   scripts/cluster.sh ps          show placements
#
# Ports: agent RPC 910X, gossip 810X; control gossip 8100, socket via
# XDG_RUNTIME_DIR. Seed = host-a's gossip address.
set -euo pipefail

cd "$(dirname "$0")/.."
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp}"

BIN=/tmp/murmur-bin
CTL_GOSSIP=127.0.0.1:8100
SEED=127.0.0.1:8101 # host-a's gossip addr

kill_orphans() {
  pkill -9 -f "$BIN/murmur" 2>/dev/null || true
  pkill -9 -f 'cmd/murmurd' 2>/dev/null || true
  pkill -9 -f 'cmd/murmur-control' 2>/dev/null || true
  # belt-and-suspenders: free our ports
  for p in 9101 9102 9103 9104 9105; do
    lsof -nP -iTCP:"$p" -t 2>/dev/null | xargs kill -9 2>/dev/null || true
  done
  sleep 1
}

build() {
  mkdir -p "$BIN"
  go build -o "$BIN/murmurd" ./cmd/murmurd
  go build -o "$BIN/murmur-control" ./cmd/murmur-control
  go build -o "$BIN/murmurctl" ./cmd/murmurctl
}

node_id()    { echo "host-$(printf "\\$(printf '%03o' $((96 + $1)))")"; } # 1->host-a
rpc_addr()   { echo "127.0.0.1:910$1"; }
gossip_addr(){ echo "127.0.0.1:810$1"; }

up() {
  local n="${1:-3}"
  kill_orphans
  build

  local registry=""
  for i in $(seq 1 "$n"); do
    local id rpc gos seedflag=""
    id="$(node_id "$i")"; rpc="$(rpc_addr "$i")"; gos="$(gossip_addr "$i")"
    [ "$i" -ne 1 ] && seedflag="--seeds $SEED" # everyone seeds host-a
    "$BIN/murmurd" --fake --node "$id" --listen "$rpc" --gossip-addr "$gos" $seedflag \
      2>"/tmp/$id.log" &
    registry="${registry:+$registry,}$id=$rpc"
  done

  "$BIN/murmur-control" --nodes "$registry" --gossip-addr "$CTL_GOSSIP" --seeds "$SEED" \
    --interval 1s 2>/tmp/control.log &

  echo "booted control + $n agents (logs in /tmp/host-*.log, /tmp/control.log)"
  echo "registry: $registry"
  sleep 3
  nodes
}

down()  { kill_orphans; echo "cluster down"; }
nodes() { "$BIN/murmurctl" nodes; }
ps()    { "$BIN/murmurctl" ps; }

cmd="${1:-up}"; shift || true
case "$cmd" in
  up)    up "$@" ;;
  down)  down ;;
  nodes) nodes ;;
  ps)    ps ;;
  *) echo "usage: $0 {up [N]|down|nodes|ps}" >&2; exit 2 ;;
esac
