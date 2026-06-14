// Package sim is murmur's deterministic simulator. It runs the WHOLE cluster —
// every node's SWIM, state store, market, reconciler, and fake VMM — on a single
// goroutine, a virtual clock, and the in-memory SimNet, stepping everyone in a
// seed-shuffled order. With the three sources of nondeterminism removed (clock,
// network, goroutine scheduling) a run is a pure function of its seed: it
// replays bit-for-bit, and a failing schedule can be reported by seed.
//
// This is the FoundationDB/TigerBeetle lesson: wall-clock + real-network +
// scheduler tests can't catch the bugs that matter, because you can't replay the
// exact interleaving that triggered them. Here we can.
package sim

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/market"
	"github.com/voidcubedotgg/murmur/internal/state"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// node is one peer's full stack, wired exactly like cmd/murmurd but driven by
// Tick/Deliver instead of goroutines.
type node struct {
	id        string
	swimAddr  string
	stateAddr string

	swim  *cluster.SWIM
	swimT cluster.Transport
	store *state.Store
	stT   cluster.Transport
	fake  *vmm.Fake
	recon *agent.Reconciler
	sched *market.Scheduler

	dead bool
}

// Config tunes the simulated cluster.
type Config struct {
	Seed           int64
	N              int
	Capacity       int
	Fencing        bool
	Dt             time.Duration // virtual time per step
	SwimCfg        cluster.Config
	GossipEvery    time.Duration
	MarketEvery    time.Duration
	ReconcileEvery time.Duration
	SnapshotEvery  time.Duration
	WorkloadEvery  time.Duration
	SnapDir        string
}

// DefaultConfig is a small, fast, deterministic setting.
func DefaultConfig(seed int64, snapDir string) Config {
	return Config{
		Seed: seed, N: 3, Capacity: 2, Fencing: true, Dt: 5 * time.Millisecond,
		SwimCfg: cluster.Config{
			Period: 40 * time.Millisecond, AckTimeout: 12 * time.Millisecond,
			SuspicionTimeout: 200 * time.Millisecond, IndirectK: 2, GossipFanout: 6,
		},
		GossipEvery: 30 * time.Millisecond, MarketEvery: 50 * time.Millisecond,
		ReconcileEvery: 50 * time.Millisecond, SnapshotEvery: 100 * time.Millisecond,
		WorkloadEvery: 50 * time.Millisecond, SnapDir: snapDir,
	}
}

// Sim is the deterministic cluster.
type Sim struct {
	cfg      Config
	clk      *clock.SimClock
	swimNet  *cluster.SimNet
	stateNet *cluster.SimNet
	nodes    map[string]*node
	ids      []string
	rnd      *rand.Rand // drives node-stepping order

	start        time.Time
	nextWorkload time.Time
	nextSnapshot time.Time
	faults       []fault
}

type fault struct {
	at      time.Duration // since start
	fn      func(*Sim)
	applied bool
}

// New builds an N-node simulated cluster, all seeded from cfg.Seed.
func New(cfg Config) *Sim {
	clk := clock.NewSimClock()
	s := &Sim{
		cfg:      cfg,
		clk:      clk,
		swimNet:  cluster.NewSimNet(0, rand.New(rand.NewSource(cfg.Seed^0x5117))),
		stateNet: cluster.NewSimNet(0, rand.New(rand.NewSource(cfg.Seed^0x57a7e))),
		nodes:    map[string]*node{},
		rnd:      rand.New(rand.NewSource(cfg.Seed)),
		start:    clk.Now(),
	}
	quorum := cfg.N/2 + 1
	for i := 0; i < cfg.N; i++ {
		id := nodeID(i)
		s.ids = append(s.ids, id)
		n := &node{id: id, swimAddr: "swim-" + id, stateAddr: "state-" + id}
		n.swimT = s.swimNet.Endpoint(n.swimAddr)
		n.stT = s.stateNet.Endpoint(n.stateAddr)

		var seedsSwim, seedsState []string
		if i != 0 {
			seedsSwim = []string{"swim-" + nodeID(0)}
			seedsState = []string{"state-" + nodeID(0)}
		}
		n.store = state.New(id, n.stateAddr, seedsState, n.stT, clk,
			rand.New(rand.NewSource(cfg.Seed+int64(i)*7+1)), quiet())
		n.store.SetGossipInterval(cfg.GossipEvery)

		n.swim = cluster.NewSWIM(id, n.swimAddr, cfg.SwimCfg, n.swimT, clk,
			rand.New(rand.NewSource(cfg.Seed+int64(i)*7+2)), quiet())
		n.swim.Join(context.Background(), seedsSwim)

		n.fake = vmm.NewFakeWithSnapDir(cfg.SnapDir)
		n.recon = agent.NewReconciler(id, n.fake, clk, cfg.ReconcileEvery, quiet())

		// Same desired source + self-fence as cmd/murmurd.
		nn := n
		hasQuorum := func() bool {
			if !cfg.Fencing {
				return true
			}
			return nn.swim.AliveCount() >= quorum
		}
		n.recon.SetSource(func() []agent.DesiredVM {
			if !hasQuorum() {
				return nil
			}
			desired := map[string]state.Spec{}
			for _, sp := range nn.store.Desired() {
				desired[sp.Name] = sp
			}
			var out []agent.DesiredVM
			for name, c := range nn.store.Claims() {
				if c.Owner != id {
					continue
				}
				if sp, ok := desired[name]; ok {
					out = append(out, agent.DesiredVM{
						Spec:        vmm.Spec{Name: name, Image: sp.Image},
						SnapshotRef: vmm.SnapshotRef(c.SnapshotRef),
					})
				}
			}
			return out
		})
		n.sched = market.New(id, cfg.Capacity, n.store, n.swim, hasQuorum, cfg.MarketEvery, clk, quiet())
		s.nodes[id] = n
	}
	return s
}

// SetDesired submits a workload via a node (any peer works).
func (s *Sim) SetDesired(viaNode, vm string) { s.nodes[viaNode].store.SetDesired(state.Spec{Name: vm}) }

// At schedules fn to run once when virtual elapsed time reaches d.
func (s *Sim) At(d time.Duration, fn func(*Sim)) { s.faults = append(s.faults, fault{at: d, fn: fn}) }

// Kill returns a fault fn that severs a node on both planes and stops stepping
// it — a physical death the survivors must detect.
func (s *Sim) Kill(id string) func(*Sim) {
	return func(sm *Sim) {
		sm.nodes[id].dead = true
		others := []string{}
		for _, o := range sm.ids {
			if o != id {
				others = append(others, o)
			}
		}
		partition(sm.swimNet, sm.stateNet, []string{id}, others)
	}
}

// Partition returns a fault fn that severs groupA from groupB on both planes.
func (s *Sim) Partition(groupA, groupB []string) func(*Sim) {
	return func(sm *Sim) { partition(sm.swimNet, sm.stateNet, groupA, groupB) }
}

// Heal returns a fault fn that removes all link blocks.
func (s *Sim) Heal() func(*Sim) {
	return func(sm *Sim) { sm.swimNet.Heal(); sm.stateNet.Heal() }
}

// Run advances the simulation for the given virtual duration.
func (s *Sim) Run(d time.Duration) { s.RunWithInvariant(d, nil) }

// RunWithInvariant advances the simulation, calling check(s) after every step.
// If check returns a non-empty string, the sim stops and returns it (a violated
// invariant). Returns "" if the whole run was clean.
func (s *Sim) RunWithInvariant(d time.Duration, check func(*Sim) string) string {
	end := s.clk.Now().Add(d)
	for s.clk.Now().Before(end) {
		s.step()
		if check != nil {
			if msg := check(s); msg != "" {
				return msg
			}
		}
	}
	return ""
}

func (s *Sim) step() {
	s.clk.Advance(s.cfg.Dt)
	now := s.clk.Now()

	// Fire any due faults (in schedule order).
	elapsed := now.Sub(s.start)
	for i := range s.faults {
		if !s.faults[i].applied && elapsed >= s.faults[i].at {
			s.faults[i].fn(s)
			s.faults[i].applied = true
		}
	}

	order := s.stepOrder()

	// 1) deliver packets sent in prior steps (1-step latency, deterministic).
	for _, id := range order {
		n := s.nodes[id]
		if n.dead {
			continue
		}
		drain(n.swimT, n.swim.Deliver)
		drain(n.stT, n.store.Deliver)
	}
	// 2) tick each live node's components.
	ctx := context.Background()
	for _, id := range order {
		n := s.nodes[id]
		if n.dead {
			continue
		}
		n.swim.Tick(now)
		n.store.Tick(now)
		n.sched.Tick(now)
		n.recon.Tick(ctx, now)
	}
	// 3) workload accrues state on running VMs.
	if !now.Before(s.nextWorkload) {
		s.nextWorkload = now.Add(s.cfg.WorkloadEvery)
		for _, id := range order {
			if !s.nodes[id].dead {
				s.nodes[id].fake.WorkloadTick()
			}
		}
	}
	// 4) owners snapshot their VMs so survivors can restore them.
	if !now.Before(s.nextSnapshot) {
		s.nextSnapshot = now.Add(s.cfg.SnapshotEvery)
		s.snapshotOwned(ctx)
	}
}

func (s *Sim) snapshotOwned(ctx context.Context) {
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.dead {
			continue
		}
		for name, c := range n.store.Claims() {
			if c.Owner != id {
				continue
			}
			ref, err := n.fake.Snapshot(ctx, name)
			if err != nil {
				continue
			}
			c.SnapshotRef = string(ref)
			n.store.SetClaim(name, c)
		}
	}
}

// stepOrder returns the node ids in a seed-shuffled order — this is the
// deterministic stand-in for goroutine scheduling.
func (s *Sim) stepOrder() []string {
	order := append([]string(nil), s.ids...)
	s.rnd.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

func drain(tr cluster.Transport, deliver func(cluster.Packet)) {
	for {
		select {
		case p := <-tr.Receive():
			deliver(p)
		default:
			return
		}
	}
}

func partition(swimNet, stateNet *cluster.SimNet, a, b []string) {
	var sa, sb, ta, tb []string
	for _, id := range a {
		sa = append(sa, "swim-"+id)
		ta = append(ta, "state-"+id)
	}
	for _, id := range b {
		sb = append(sb, "swim-"+id)
		tb = append(tb, "state-"+id)
	}
	swimNet.Partition(sa, sb)
	stateNet.Partition(ta, tb)
}

func nodeID(i int) string { return "host-" + string(rune('a'+i)) }
