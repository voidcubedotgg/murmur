#!/usr/bin/env bash
# cluster.sh — boot a local murmur peer cluster (CRDT path: no leader, no
# control plane). Each peer runs the VMM, SWIM membership, and a gossiped
# replicated state store. murmurctl talks to any peer via --peer.
#
# It KILLS orphaned murmur processes first (the #1 demo footgun: a leftover
# process holding a port makes the new peer fail to bind and exit).
#
# Usage:
#   scripts/cluster.sh up [N]      boot N peers (default 3, fake VMM)
#   scripts/cluster.sh down         kill everything
#   scripts/cluster.sh nodes        membership (via host-a)
#   scripts/cluster.sh ps           converged placements (via host-a)
set -euo pipefail

cd "$(dirname "$0")/.."
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp}"

BIN=/tmp/murmur-bin
SWIM_SEED=127.0.0.1:8101  # host-a SWIM gossip
STATE_SEED=127.0.0.1:8201 # host-a state gossip

kill_orphans() {
  pkill -9 -f "$BIN/murmurd" 2>/dev/null || true
  pkill -9 -f 'cmd/murmurd' 2>/dev/null || true
  rm -f "$XDG_RUNTIME_DIR"/murmurd-*.sock 2>/dev/null || true
  sleep 1
}

build() {
  mkdir -p "$BIN"
  go build -o "$BIN/murmurd" ./cmd/murmurd
  go build -o "$BIN/murmurctl" ./cmd/murmurctl
}

node_id() { printf "host-%b" "$(printf '\\x%02x' $((96 + $1)))"; } # 1 -> host-a

up() {
  local n="${1:-3}"
  kill_orphans
  build
  for i in $(seq 1 "$n"); do
    local id swim state swim_seedflag="" state_seedflag=""
    id="$(node_id "$i")"
    swim="127.0.0.1:810$i"
    state="127.0.0.1:820$i"
    if [ "$i" -ne 1 ]; then
      swim_seedflag="--seeds $SWIM_SEED"
      state_seedflag="--state-seeds $STATE_SEED"
    fi
    "$BIN/murmurd" --fake --node "$id" --capacity 2 \
      --gossip-addr "$swim" $swim_seedflag \
      --state-addr "$state" $state_seedflag \
      2>"/tmp/$id.log" &
  done
  echo "booted $n peers (logs: /tmp/host-*.log)"
  sleep 3
  nodes
}

down()  { kill_orphans; echo "cluster down"; }
nodes() { "$BIN/murmurctl" --peer host-a nodes; }
ps()    { "$BIN/murmurctl" --peer "${1:-host-a}" ps; }

# kill one peer (out-of-band failure) to demo re-claim + restore.
killnode() {
  local id="${1:?usage: cluster.sh kill host-X}"
  pkill -9 -f "node $id " 2>/dev/null || pkill -9 -f "node $id$" 2>/dev/null || true
  echo "killed $id"
}

cmd="${1:-up}"; shift || true
case "$cmd" in
  up)    up "$@" ;;
  down)  down ;;
  nodes) nodes ;;
  ps)    ps "$@" ;;
  kill)  killnode "$@" ;;
  *) echo "usage: $0 {up [N]|down|nodes|ps [peer]|kill host-X}" >&2; exit 2 ;;
esac
