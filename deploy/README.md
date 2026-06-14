# murmur cloud deploy (Hetzner)

A throwaway multi-node cluster for running the north-star / fencing demo on real
hosts with a real network. **Not production** — see caveats. Tear it down when
done (`make destroy`); it costs money while it runs.

## Shape

```
            ┌─────────── private net 10.0.0.0/24 (unfiltered) ───────────┐
  host-a .10 ── gossip 8101/8201 ── host-b .11 ── … ── host-c .12         │
       │  └──────────── NFS 2049 ──────────┴──────────────┘  │           │
       └──────────────────────► murmur-nfs .5 ◄──────────────┘           │
                         (shared snapshot store)                          │
            └────────────── public: SSH from your IP only ───────────────┘
```

- **3 peers** (`host-a/b/c`), each runs `murmurd --fake` via systemd, `--cluster-size 3 --fencing`.
- **1 NFS box** (`murmur-nfs`) exports the shared snapshot dir (`/srv/murmursnap`).
  All peers mount it, so a survivor re-claiming a dead peer's VM **restores its
  real state** — the cross-host gap noted in the original plan, closed.
- Hetzner firewalls filter only the **public** interface → SSH-only rule. Gossip
  and NFS ride the private net, unfiltered. No UDP rules needed.

### Why a separate NFS box (not NFS on host-a)?
The snapshot store must outlive any peer. If the peer running the workload also
served snapshots, killing it would destroy the snapshots too and the survivor
would restore nothing. **Durability must not die with compute.**

## Prereqs
```bash
export TF_VAR_hcloud_token=<your hetzner token>
export TF_VAR_admin_cidr=$(curl -s ifconfig.me)/32
# key defaults to ~/.ssh/id_ed25519.pub — override with TF_VAR_ssh_public_key_path
terraform=…, jq=…, ssh=…   # installed
```

## Run it
```bash
make init                 # one-time
make apply                # provision 4 VMs (~1 min) + cloud-init OS prep
sleep 60                  # let cloud-init finish (nfs mount, systemd unit)
make deploy               # scp the binary to peers, start murmurd
make nodes                # SWIM membership — all alive
make run NAME=counter     # submit a stateful workload
make ps                   # market placed it on some peer; counter climbing
```

## The demos

**Plain failover (kill):**
```bash
make ssh NODE=<owner>     # find owner from `make ps`; then on the box:
#   systemctl stop murmurd        (or: destroy the VM)
make ps                   # survivor re-claims, RESTORES from NFS snapshot —
                          # counter resumes non-zero (state intact)
```

**Partition + fencing (the payoff):**
```bash
make partition NODE=host-a   # {host-a} | {host-b,host-c}, both planes
make nodes                   # host-a → suspect → dead (from the majority's view)
make ps                      # minority host-a self-fenced (its copy stopped);
                             # majority runs exactly one copy — no split-brain
make heal                    # remove blocks; host-a rejoins, rebalances
```
Toggle fencing off (set `--fencing=false` in the unit, redeploy) to *watch the
split-brain happen* — the Stage-6 lesson, live.

## Caveats (learning cluster, by design)
- `--fake` VMM, not smolvm: the "workload" is an in-memory counter snapshotted to
  a file. Real substrate is out of scope here.
- NFS export is `no_root_squash`, dir `0777`, private-net only. Convenient, not
  secure. Don't reuse this for anything real.
- No TLS, no auth on gossip; trust = the private network. Fine for a demo VPC.
- State is the snapshot cadence's RPO behind (`--snapshot-interval`); a kill
  between snapshots loses the delta. That's the durability tradeoff, on purpose.

## Cost control
`make destroy` removes everything. Don't leave it running.
