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
| 3 | Eventually-consistent replicated state; CRDTs; convergence; *no leader* |
| 4 | Market/bidding scheduling; ownership claims & lease expiry; reconciling failure |
| 5 | **End-to-end fault tolerance** (the north-star demo) |
| 6 | Split-brain & fencing — why eventual consistency can't give mutual exclusion |
| 7 | Deterministic simulation testing |

> **Path note.** murmur follows the **eventually-consistent branch** (CRDTs +
> gossip, the Fly/Corrosion shape): no leader, no central control plane, peers
> that converge. The consensus/Raft branch is the documented Stage-8 alternative.
> The choice is deliberate — the lesson we're chasing is *"eventual consistency
> is wonderful until you need mutual exclusion, and then it bites"* (Stage 6).

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

### Stage 3 — Eventually-consistent replicated state (CRDTs)

**Build:** a hand-rolled CRDT layer (Lamport clock + LWW-Register + LWW-Map with
tombstones, per CLAUDE.md directive 4) and a replicated desired-state store that
peers **anti-entropy-gossip** to each other over the Stage-2 transport. No leader,
no central control plane: every agent is a peer, converges on the same desired
state, and runs its own share. Placement is still manual (you name the node); the
market that *decides* placement is Stage 4.

**Demo:** `murmurctl run` a VM via one peer; read `ps` from a *different* peer and
see the same converged assignment; make concurrent/partitioned writes and watch
them converge deterministically (LWW) after heal.

**You can explain:** why a CRDT merge being commutative, associative, and
idempotent makes gossip order and duplication irrelevant; what "last-write-wins"
actually resolves (and what it silently drops); why there is no single source of
truth — only convergence.

**Read:** Shapiro et al. CRDT paper; DDIA's replication/consistency chapters;
Fly.io's *Corrosion* post (eventual consistency + CRDTs).

### Stage 4 — Market scheduling & reconciling failure

**Build:** placement as a *market*: a VM with no owner is up for grabs; peers
claim it by writing an ownership claim (a CRDT register, node-id + lease/heartbeat
stamp) and the merge rule deterministically picks one winner. Claims expire when a
node's heartbeat stops (membership says it's gone). An expired claim is re-claimed
by a survivor, which **restores the workload from its last snapshot**.

**Demo:** a stateful VM (the in-memory counter) claimed by host C; snapshot it
periodically; `pkill murmurd` on host C; a survivor re-claims it and restores it
on host A with its counter intact — no leader involved.

**You can explain:** how mutual *exclusion is only approximate* here (two peers can
briefly both think they own a VM); how lease expiry drives re-claim; the gap
between "desired state converged" and "reality converged"; why snapshot cadence is
a durability/RPO tradeoff. (The exclusion hole is the setup for Stage 6.)

**Read:** Fly.io's *Carving the Scheduler* post; DDIA on leases & clocks.

### Stage 5 — The north-star demo

**Build:** nothing new — make stages 0–4 robust enough to do this reliably, end to
end, unattended.

**Demo (the whole point of the project):**
> A 3-node cluster runs a stateful workload. You physically kill the node hosting
> it. Within seconds, the survivors detect the failure (SWIM), its ownership claim
> expires, a survivor re-claims it via CRDT convergence, chooses to restore it
> from snapshot, and it resumes mid-heap — and `murmurctl ps`, asked of *any*
> surviving peer, told a coherent story the entire time.

That single demo exercises membership, failure detection, CRDT convergence,
reconciliation, and state transfer simultaneously — with no leader anywhere. When
it works and you can narrate every step, you have learned the core of the field.

**You can explain:** the full causal chain from "cable pulled" to "workload alive
elsewhere," naming the algorithm responsible for each link.

---

## Hard mode (optional, where the person-years live)

Past stage 5 is where real orchestrators spend most of their effort. Pick these à
la carte, for the lesson, not for completeness.

### Stage 6 — Split-brain & fencing (the payoff of this path)

**Build:** partition the cluster and watch the eventual-consistency model hit its
wall. With no leader and only CRDTs, **both sides of a netsplit will independently
re-claim and run the same stateful VM** — and their claim registers *both look
valid* and merge cleanly on heal. Convergence does not save you: two live copies
already corrupted the world. Confront the killer question: how do you get mutual
exclusion when you deliberately have no consensus?

This is the whole reason for taking the CRDT path: you *feel* that CRDTs converge
but cannot decide. The fixes all reintroduce a sliver of linearizability —
fencing tokens, a lease backed by *some* single-decision-maker, STONITH — none of
which a pure CRDT can provide.

**Chosen mechanism:** *quorum-gated self-fencing*. A peer runs a claimed VM only
while it sees a majority of the FIXED cluster (`--cluster-size`); a partitioned
minority self-fences (stops its VMs) so the majority can own them safely. No
leader. This is CAP made concrete — we trade the minority's availability for
safety. Residual hole we *name, not hide*: a detection-lag window where both
sides still run briefly, because true instant fencing needs the substrate to
reject a stale token and our VMM is a black box (directive 1). Demonstrated by
deterministic SimNet partition tests, toggled by `--fencing` (off → split-brain,
on → safe).

**You can explain:** split-brain at the *workload* level; why "the dead node might
not be dead"; why a CRDT register can't be a lock; what a fencing token buys that
last-write-wins cannot; the precise boundary where you're forced to pay for
consensus.

**Read:** Kleppmann's "How to do distributed locking" (the fencing-token piece);
the fencing/leases discussion in DDIA.

### Stage 7 — Deterministic simulation testing

**Build:** drive the whole cluster on a simulated clock and network so you can
replay exact failure sequences and shrink bugs deterministically.

**Done (`internal/sim`):** single-threaded simulator — every node's SWIM, store,
market, reconciler, and fake VMM run on ONE goroutine, on a virtual `SimClock`,
over the `SimNet`, stepped in a seed-shuffled order. The three nondeterminism
sources are removed: clock (virtual), network (SimNet), and goroutine scheduling
(single-threaded seeded order) — plus every behaviour-affecting **map iteration
is sorted** (Go map order is randomized; that was the last hidden source). A run
is a pure function of its seed: `TestSim_Replayable` runs the same seed twice and
asserts byte-identical observable state; `TestSim_ManySeedsFencingSafe` checks the
no-two-owners invariant across 40 seeds and reports the failing seed. Live `Run`
loops were refactored into thin pacing wrappers over the same `Tick`/`Deliver`
core the simulator drives, so sim and production share one implementation.

**You can explain:** why wall-clock-and-real-network tests can't catch the bugs
that matter (you can't replay the interleaving that triggered them), why even a
single goroutine isn't deterministic until map iteration is ordered, and how
FoundationDB/TigerBeetle find theirs.

**Read:** FoundationDB's testing talk; TigerBeetle's simulation-testing writeups;
Jepsen analyses.

### Stage 8 — The consensus path not taken (contrast)

Having felt Stage 6's wall, build the *other* branch to compare: a small
hand-rolled Raft group providing a strongly-consistent control plane (or just a
fencing service / lease authority), and rework one stateful failover to go
through it. Write down, concretely, what consensus buys (safe mutual exclusion,
a real lock with a fencing token) and what it costs (a leader, quorum latency,
unavailability during election). This is where the eventually-consistent vs.
strongly-consistent fork stops being abstract.

**Read:** the Raft paper + the **MIT 6.5840 Raft lab**.

### Stage 9+ — only if curious

Overlay networking across hosts (WireGuard mesh), service discovery, the
`fork`-as-instant-scale primitive (smolvm COW fork → N warm clones, if supported),
delta-state CRDTs and gossip efficiency à la Corrosion. Each is a real topic; none
is required to have learned the core.

---

## Definition of "done enough"

Stage 5, reliably, on three machines (laptops are fine; the `fake` VMM plus a
simulated network is fine for most of it). If you reach that and can explain every
link in the failover chain, the project has done its job — anything past it is
bonus, not obligation.
