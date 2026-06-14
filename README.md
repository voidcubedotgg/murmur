# murmur

A small, honest microVM swarm — built to learn distributed systems by hand.

`murmur` schedules lightweight Linux microVMs across a fleet of hosts: it decides
where each VM runs, notices when a host dies, and brings the lost workloads back
to life on a survivor — restoring them from a snapshot so the process picks up
mid-heap, as if nothing happened.

It is **not** trying to be Kubernetes, Nomad, or Fly.io. It is a teaching
vehicle. The goal is to understand membership, failure detection, consensus,
reconciliation, and partition behaviour by *building* them, on top of a problem
that makes those abstractions tangible: you pull the network cable on a node and
watch a stateful VM reappear, alive, somewhere else.

> **Status:** early. See [GOALS.md](./GOALS.md) for the build ladder and where we
> currently are. This is a personal learning project and is **not** production
> software — do not run untrusted multi-tenant workloads on it.

---

## The premise

The microVM layer is a solved problem and a *different* subject. Building a VMM
teaches you about page tables, KVM, and virtio — computer architecture, not
distributed systems. So `murmur` treats the VMM as a **black box** behind a thin
adapter and spends all of its effort on the layer above, because that layer *is*
distributed systems.

Concretely, the swarm only needs four verbs from the substrate:

```
boot(image) -> vm        snapshot(vm) -> bundle
kill(vm)                 restore(bundle) -> vm
```

Any runtime that can do those works. The default substrate is
[smolvm](https://github.com/smol-machines/smolvm) (mature, OCI images, Linux +
macOS). [machinen](https://github.com/redwoodjs/machinen.dev) is the more exciting
target because its whole reason for existing is `snapshot`/`restore`/`fork`
*across hosts* — which is exactly the primitive that makes failover and
migration real. The adapter interface lets you swap between them.

---

## Architecture (target)

```
                    ┌─────────────────────────────┐
                    │        control plane         │
                    │  scheduler · reconciler ·    │
                    │  desired-state API           │
                    └───────────────┬──────────────┘
                                    │ RPC
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
        ┌─────┴─────┐         ┌─────┴─────┐         ┌─────┴─────┐
        │  agent A  │         │  agent B  │         │  agent C  │
        │  (host)   │ ◄─────► │  (host)   │ ◄─────► │  (host)   │
        └─────┬─────┘ gossip  └─────┬─────┘ gossip  └─────┬─────┘
              │                     │                     │
        ┌─────┴─────┐         ┌─────┴─────┐         ┌─────┴─────┐
        │ VMM adapter│        │ VMM adapter│        │ VMM adapter│
        │ (black box)│        │ (black box)│        │ (black box)│
        └───────────┘         └───────────┘         └───────────┘
```

- **Agent** — long-running daemon on each host. Owns the VMs on its node, drives
  the VMM adapter, reports health and capacity, runs a local reconciliation loop.
- **Control plane** — holds desired state ("I want VM X running with these
  constraints"), schedules placement, and reconciles desired vs. observed.
- **Gossip layer** — membership and failure detection between agents (SWIM-style),
  so the fleet agrees on who is alive without a central bottleneck.
- **VMM adapter** — the four-verb interface above. The *only* place that knows
  whether the substrate is smolvm, machinen, or raw Firecracker.

How these pieces split responsibility, what's strongly consistent vs. eventually
consistent, and where the source of truth lives are the actual lessons — see
[GOALS.md](./GOALS.md).

---

## Repo layout (target)

```
cmd/
  murmurd/        # the agent daemon (runs on every host)
  murmurctl/      # the CLI you drive the cluster with
internal/
  vmm/            # the VMM adapter + implementations (smolvm, machinen, fake)
  agent/          # node-local lifecycle + reconciliation loop
  control/        # scheduler, desired-state store, reconciler
  cluster/        # membership + failure detection (gossip / SWIM)
  consensus/      # leader election + replicated log (Raft)
  rpc/            # transport between control plane and agents
docs/             # design notes written as you learn each concept
GOALS.md          # the learning ladder
CLAUDE.md         # working agreement for AI-assisted coding
```

The `vmm/fake` implementation is important: an in-memory substrate that "boots"
and "snapshots" instantly so you can develop and test the distributed layer
without a real hypervisor in the loop.

---

## Quickstart

> Fills in as the ladder progresses. The shape it's heading toward:

```bash
# start an agent on each host
murmurd --join host-a:7946

# from anywhere, declare what you want running
murmurctl run --name counter --image ./counter.tar.gz --replicas 1

# watch it survive a host dying
murmurctl ps
ssh host-b 'pkill murmurd'        # kill the node running it
murmurctl ps                      # ...it comes back on a survivor
```

---

## Reading alongside

Building distributed systems without reading the field means reinventing bugs
that were solved in 1985. In rough order of leverage:

- **MIT 6.5840** (formerly 6.824) — its labs have you build Raft and a sharded
  fault-tolerant KV store from the spec. Do the Raft lab before tackling the
  scheduler. Highest-leverage exercise in the field.
- **Designing Data-Intensive Applications**, Kleppmann — read the
  replication / consistency / consensus chapters as you hit each stage.
- The **Raft paper** — "In Search of an Understandable Consensus Algorithm."
- The **SWIM paper** — for membership and failure detection.
- **Fly.io's engineering blog** — *Corrosion* and *Carving the Scheduler Out of
  Our Orchestrator* are, almost exactly, a postcard from this project's future.
- **Jepsen** (Kyle Kingsbury) and **TigerBeetle / FoundationDB** on deterministic
  simulation testing — for when you want to test the thing seriously.

---

## Prior art (and how murmur differs)

| | murmur | Kubernetes | Nomad | Fly.io (flyd) |
|---|---|---|---|---|
| Purpose | learning | production | production | production |
| Unit | microVM | container/pod | task | microVM |
| Source of truth | TBD (the lesson) | central (etcd) | central (raft) | workers + gossip |
| Scale target | a few laptops | thousands | thousands | global |

The point isn't to compete — it's to make the same decisions these systems made,
feel why they're hard, and read those projects' postmortems with recognition.

---

## License

For learning. Pick a license before you publish anything; until then, all rights
reserved by default.
