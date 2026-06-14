#!/usr/bin/env bash
# northstar.sh — the whole point of the project, live and narrated.
#
# A 3-peer cluster runs a stateful workload (a counter). We kill the peer hosting
# it. A survivor detects the death (SWIM), re-claims it (market), restores it from
# snapshot (state intact, RPO-bounded), and it resumes — no leader anywhere.
set -uo pipefail
cd "$(dirname "$0")"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp}"
M=/tmp/murmur-bin/murmurctl
ALL="host-a host-b host-c"

say() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

# runner VM [exclude] — the peer that actually OBSERVES the VM running is the
# authoritative owner. Optionally skip an excluded (killed) peer.
runner() {
  local vm="$1" excl="${2:-}"
  for id in $ALL; do
    [ "$id" = "$excl" ] && continue
    local st
    st=$("$M" --peer "$id" ps 2>/dev/null | awk -v v="$vm" '$1==v{print $3}') || true
    [ "$st" = "running" ] && { echo "$id"; return 0; }
  done
  return 0
}
counterAt() { "$M" --peer "$1" ps | awk -v v="$2" '$1==v{print $4}'; }

say "boot 3-peer cluster"
./cluster.sh up 3 >/dev/null 2>&1
sleep 3
./cluster.sh nodes

say "submit a stateful workload (counter)"
"$M" run --name counter
owner=""
for _ in $(seq 1 30); do owner=$(runner counter); [ -n "$owner" ] && break; sleep 0.5; done
[ -z "$owner" ] && { echo "FAILED: counter never started"; ./cluster.sh down >/dev/null 2>&1; exit 1; }
echo "counter is running on: $owner"

say "watch the counter accrue state on its owner ($owner)"
for _ in 1 2 3; do sleep 2; "$M" --peer "$owner" ps; done
before=$(counterAt "$owner" counter)

say "KILL the owner ($owner) — physical death (counter was ~$before)"
./cluster.sh kill "$owner"

say "survivors detect death, re-claim, restore from snapshot"
newowner=""
for _ in $(seq 1 30); do newowner=$(runner counter "$owner"); [ -n "$newowner" ] && break; sleep 0.5; done
[ -z "$newowner" ] && { echo "FAILED: no survivor restored counter"; ./cluster.sh down >/dev/null 2>&1; exit 1; }
sleep 1
"$M" --peer "$newowner" ps
restored=$(counterAt "$newowner" counter)
echo
echo ">>> counter migrated $owner -> $newowner, restored counter=$restored"
echo ">>> (resumed from the last snapshot, not zero; the gap vs ~$before is the RPO)"

say "the causal chain, in the logs"
grep -hE "declared DEAD|re-claimed|restoring from snapshot" /tmp/host-*.log | tail -6 || true

./cluster.sh down >/dev/null 2>&1
