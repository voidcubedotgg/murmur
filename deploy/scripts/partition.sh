#!/usr/bin/env bash
# partition.sh — real-host equivalent of sim.Partition / SimNet on the cloud
# cluster. Severs one peer from the others by dropping packets on the PRIVATE
# IPs (where gossip rides), so your public-IP SSH session is never affected.
#
# Safety: all DROPs live in a dedicated chain (MURMUR_PART) jumped from INPUT.
# `heal` flushes only that chain — it never touches your SSH/base rules, so no
# lockout (the footgun of `iptables -F`).
#
#   partition.sh isolate host-a     # {host-a} | {the rest}, both gossip planes
#   partition.sh heal               # remove all blocks on every node
set -euo pipefail
cd "$(dirname "$0")/.."

TF="terraform -chdir=terraform"
PEERS_JSON="$($TF output -json peers)"
CHAIN=MURMUR_PART

nodes()      { jq -r 'keys[]'              <<<"$PEERS_JSON"; }
pub()        { jq -r ".\"$1\".public_ip"   <<<"$PEERS_JSON"; }
priv()       { jq -r ".\"$1\".private_ip"  <<<"$PEERS_JSON"; }
on()         { ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null "root@$(pub "$1")" "$2"; }

# Idempotently ensure the dedicated chain exists and is jumped from INPUT.
ensure_chain() {
  on "$1" "iptables -nL $CHAIN >/dev/null 2>&1 || iptables -N $CHAIN; \
           iptables -C INPUT -j $CHAIN 2>/dev/null || iptables -I INPUT 1 -j $CHAIN"
}
drop_from() { on "$1" "iptables -C $CHAIN -s $2 -j DROP 2>/dev/null || iptables -A $CHAIN -s $2 -j DROP"; }

cmd="${1:-}"; target="${2:-}"
case "$cmd" in
  isolate)
    [ -n "$target" ] || { echo "usage: partition.sh isolate <node>"; exit 2; }
    tip="$(priv "$target")"
    ensure_chain "$target"
    for n in $(nodes); do
      [ "$n" = "$target" ] && continue
      # block both directions: target drops the peer, peer drops the target.
      drop_from "$target" "$(priv "$n")"
      ensure_chain "$n"
      drop_from "$n" "$tip"
    done
    echo "isolated $target ($tip). minority should self-fence; watch: make nodes; make ps"
    ;;
  heal)
    for n in $(nodes); do
      on "$n" "iptables -F $CHAIN 2>/dev/null || true"
    done
    echo "healed. survivors resurrect $(nodes | tr '\n' ' ')via first-hand SWIM contact."
    ;;
  *)
    echo "usage: partition.sh {isolate <node>|heal}"; exit 2;;
esac
