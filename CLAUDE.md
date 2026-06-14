# CLAUDE.md

Guidance for AI-assisted work on `murmur`. Read this before writing code.

## What this project is

`murmur` is a microVM swarm built **as a vehicle for the author to learn
distributed systems by hand**. The author maintains distributed systems
professionally and wants depth, not a shipped product. That goal changes how you
should help: the measure of success is *the human understanding the code and the
concepts*, not lines shipped or cleverness demonstrated.

If you ever have to choose between "more done" and "the human learns more," choose
learning.

## Prime directives

1. **Never build, modify, or vendor the VMM.** The hypervisor / microVM runtime
   (smolvm) is a black box behind `internal/vmm`. smolvm is the one real
   substrate; the only other adapter is the `fake` VMM used for tests/simulation.
   Building
   a VMM teaches computer architecture, not distributed systems — it is explicitly
   out of scope. If a task seems to require touching VMM internals, stop and say
   so; the fix is almost always in the adapter, not the VMM.

2. **The four-verb interface is sacred.** The swarm depends on exactly
   `boot / kill / snapshot / restore`. Don't leak substrate-specific concepts
   (smolvm internals, snapshot image formats, runtime flags) above `internal/vmm`. The rest of the
   system must not care which substrate is underneath.

3. **One distributed-systems concept per stage.** The build follows the ladder in
   [GOALS.md](./GOALS.md). Don't jump ahead. Don't add consensus before membership
   works; don't add an overlay network before failover works. Each stage ends with
   a runnable demo.

4. **Implement the hard algorithms by hand the first time.** When a stage
   introduces Raft, SWIM, a phi-accrual detector, or a CRDT — write it explicitly
   from the paper, with comments explaining *why*, before reaching for a library.
   The learning is in the implementation. Once it works and is understood, the
   human may choose to swap in a battle-tested library; flag that as a follow-up,
   don't do it preemptively.

5. **Keep the demo green.** `main` should always boot a cluster and run the
   current stage's demo. Never leave the repo in a "doesn't compile / half-migrated"
   state across a session boundary.

## How to write code here

- **Optimize for reading, not abstraction.** Prefer explicit, boring, traceable
  code over generic frameworks. A reconciliation loop you can read top-to-bottom
  beats a clever event bus. No premature interfaces — `internal/vmm` is the one
  interface that earns its keep; be skeptical of others.

- **Comment the *why*, especially the distributed-systems reasoning.** "We wait
  for a quorum here because a single ack can't distinguish a slow node from a dead
  one" is the kind of comment that makes this project worth building.

- **Make failure visible.** Log state transitions, leader changes, membership
  events, and reconciliation decisions clearly. The author should be able to
  *watch* the system think. Structured logs with a node id on every line.

- **Determinism where you can get it.** Inject the clock, the network, and
  randomness. Code that takes a `Clock` and a `Transport` interface can be tested
  with a simulated, controllable version — this pays off enormously at the
  partition-testing stage.

## Conventions

- **Language:** Go. Standard module layout (`cmd/`, `internal/`).
- **Errors:** wrap with context (`fmt.Errorf("...: %w", err)`); never swallow.
- **Concurrency:** prefer channels and a single owning goroutine per resource
  (the "actor" shape) over shared mutexes where it keeps reasoning local. Always
  consider: what happens to this goroutine on shutdown? on partition?
- **Dependencies:** minimal. The point is to build the interesting parts. A gossip
  or raft library may be introduced *after* a hand-rolled version exists and is
  understood (see directive 4).
- **Tests:** every distributed component gets a test that runs it against the
  `fake` VMM and a simulated network, including a fault-injection case (drop
  messages, partition the cluster, kill a node).

## Architecture boundaries you must respect

- `internal/vmm` — the only package that imports substrate-specific code.
- `internal/cluster` — membership/failure detection. Knows nothing about VMs.
- `internal/consensus` — leader election + replicated log. Knows nothing about VMs.
- `internal/control` — uses cluster + consensus to make placement decisions.
- `internal/agent` — node-local; drives the VMM, runs the local reconcile loop.

If a change blurs one of these lines, that's a signal to stop and discuss the
design, not to push through it.

## When to stop and ask the human

- Any design fork with real consequences: strongly-consistent vs. eventually-
  consistent state, central scheduler vs. market/bidding model, push vs. pull
  reconciliation. These *are* the lessons — surface the tradeoff and let the human
  decide. Offer the options and the consequences; don't silently pick one.
- Anything that would require touching the VMM.
- Anything that skips a rung on the ladder.

## Never

- Never present a hand-wavy distributed algorithm as correct. If a design has a
  split-brain or lost-update hole, say so plainly — finding those holes is the
  whole point.
- Never reach for `localStorage`-style shortcuts that hide where state actually
  lives; the location of the source of truth is a core lesson here.
- Never "just use a library" to skip a concept the human hasn't built yet.
