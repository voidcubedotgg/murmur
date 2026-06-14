# GOALS.md

## The meta-goal

Learn distributed systems deeply by building a microVM swarm by hand. The system
is the means; the understanding is the end. Each stage is designed to make exactly
one hard concept *unavoidable* — something you can only get right by feeling it
break first.

Success is measured in understanding, not features. A stage is "done" when you can
explain — to yourself, without notes — why the naive version failed and why the
real version works.

## Non-goals

- Building a VMM or touching hypervisor internals (that's a different subject).
- Production readiness, multi-tenancy security, or scale beyond a handful of hosts.
- Beating Kubernetes/Nomad/Fly at anything. We're re-deriving their decisions, not
  out-engineering them.
- Breadth. Better to deeply understand failover than to shallowly touch ten
  features.

## The concept map

| Stage | Distributed-systems concept you actually learn |
|-------|------------------------------------------------|
| 0 | Reconciliation loops; desired vs. observed state |
| 1 | The network is unreliable; idempotency; at-least-once delivery |
| 2 | Membership; failure detection; "down" vs. "unreachable" |
| 3 | Consensus; leader election; split-brain |
| 4 | Replicated state; reconciling failure; where the source of truth lives |
| 5 | **End-to-end fault tolerance** (the north-star demo) |
| 6 | Network partitions; fencing; deterministic simulation testing |

---

## The ladder

Each stage lists the deliverable, the demo that proves it, the success criterion
(the thing you must be able to *explain*), and the reading that pairs with it.

### Stage 0 — Single-host plumbing

**Build:** the agent daemon driving the `fake` VMM and the real one (smolvm)
through the four-verb adapter. A local reconciliation loop: given "I want VM X
running," make reality match, repeatedly and idempotently.

**Demo:** `murmurctl run --name counter`; kill the VM out-of-band; the reconciler
notices and brings it back.

**You can explain:** why an orchestrator is a *loop that converges state*, not a
script that runs once. Why reconcile must be idempotent.

**Read:** DDIA intro; the Kubernetes "reconciliation"/controller pattern (concept,
not the code).

### Stage 1 — Two nodes, manual placement

**Build:** control plane + agents over RPC. Place a VM on a node you name.

**Demo:** `murmurctl run --name counter --node host-b` boots it on the right host;
`murmurctl ps` shows it.

**You can explain:** every failure mode of a single RPC — timeout, partial
failure, "did it land?" — and how idempotency keys / at-least-once semantics make
retries safe.

**Read:** DDIA ch. on unreliable networks; "Notes on Distributed Systems for Young
Bloods" (Hodges).

### Stage 2 — Membership & failure detection

**Build:** gossip between agents. Start with naive periodic heartbeats; watch them
produce false positives under load; replace with SWIM-style gossip and/or a
phi-accrual detector.

**Demo:** start 3 agents; kill one; the survivors agree it's gone within a bounded
time; restart it; it rejoins.

**You can explain:** why "node is down" is undecidable, only suspectable; why
SWIM separates failure *detection* from failure *dissemination*; what a false
positive costs you.

**Read:** the SWIM paper; Serf/memberlist design docs; phi-accrual paper.

### Stage 3 — Leader election & the scheduler

**Build:** a Raft group for the control plane (hand-rolled first, per CLAUDE.md
directive 4) — leader election + a replicated log of scheduling decisions. Then a
scheduler that bin-packs VMs onto live nodes under simple constraints
(cpu/mem/labels).

**Demo:** kill the leader; a new one is elected; scheduling continues. Submit more
VMs than one node holds; they spread.

**You can explain:** why you can't "just pick a leader"; how Raft avoids two
leaders; what a quorum buys you; why the log is replicated *before* it's applied.

**Read:** the Raft paper + do the **MIT 6.5840 Raft lab** here — this is the
single highest-leverage exercise in the project.

**Decision to surface (don't pre-decide):** central Raft-backed scheduler
(Kubernetes/Nomad shape) vs. Fly.io's market/bidding model where workers own
their state. Build one, but write down why, and what you'd lose.

### Stage 4 — Replicated state & reconciling failure

**Build:** durable desired-state, replicated via the Raft log. The reconciler now
spans the cluster: when membership reports a node gone, its VMs are rescheduled
onto survivors and **restored from their last snapshot**.

**Demo:** a stateful VM (the in-memory counter) running on host C; snapshot it
periodically; `pkill murmurd` on host C; the VM is restored on host A with its
counter intact.

**You can explain:** exactly where the source of truth lives and why; the gap
between "desired state agreed" and "reality converged"; why snapshot cadence is a
durability/RPO tradeoff.

**Read:** DDIA replication + consistency chapters; Fly.io's *Corrosion* post
(eventual consistency + CRDTs) and *Carving the Scheduler* post.

### Stage 5 — The north-star demo

**Build:** nothing new — make stages 0–4 robust enough to do this reliably, end to
end, unattended.

**Demo (the whole point of the project):**
> A 3-node cluster runs a stateful workload. You physically kill the node hosting
> it. Within seconds, the survivors detect the failure, agree on it, elect/keep a
> leader, choose a new home, restore the workload from snapshot, and it resumes
> mid-heap — and `murmurctl ps` told a coherent story the entire time.

That single demo exercises membership, failure detection, consensus,
reconciliation, and state transfer simultaneously. When it works and you can
narrate every step, you have learned the core of the field.

**You can explain:** the full causal chain from "cable pulled" to "workload alive
elsewhere," naming the algorithm responsible for each link.

---

## Hard mode (optional, where the person-years live)

Past stage 5 is where real orchestrators spend most of their effort. Pick these à
la carte, for the lesson, not for completeness.

### Stage 6 — Partitions & fencing

**Build:** partition testing. Then confront the killer question: during a netsplit,
how do you avoid restoring a *second* live copy of a stateful VM while the
original might still be running on the other side?

**You can explain:** split-brain at the *workload* level; fencing / STONITH;
leases and why they need a fencing token; why "the dead node might not be dead."

**Read:** Kleppmann's "How to do distributed locking"; the fencing-token discussion
in DDIA.

### Stage 7 — Deterministic simulation testing

**Build:** drive the whole cluster on a simulated clock and network so you can
replay exact failure sequences and shrink bugs deterministically.

**You can explain:** why wall-clock-and-real-network tests can't catch the bugs
that matter, and how FoundationDB/TigerBeetle find theirs.

**Read:** FoundationDB's testing talk; TigerBeetle's simulation-testing writeups;
Jepsen analyses.

### Stage 8+ — only if curious

Overlay networking across hosts (WireGuard mesh), service discovery, the
`fork`-as-instant-scale primitive (smolvm COW fork → N warm clones, if supported),
eventually-consistent state à la Corrosion (CRDT + gossip instead of Raft for the
routing view). Each is a real topic; none is required to have learned the core.

---

## Definition of "done enough"

Stage 5, reliably, on three machines (laptops are fine; the `fake` VMM plus a
simulated network is fine for most of it). If you reach that and can explain every
link in the failover chain, the project has done its job — anything past it is
bonus, not obligation.
