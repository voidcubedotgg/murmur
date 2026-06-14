package control

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// Liveness is the controller's view of cluster membership. It's an interface so
// the controller can be tested without a real gossip layer, and so the cluster
// package stays ignorant of control (the dependency points one way).
type Liveness interface {
	// Alive reports whether the node is currently believed up. Remember this is
	// a belief from SWIM, not ground truth — acting on a false "dead" costs a
	// needless reschedule, which is why we only *skip* dead nodes here, never do
	// anything destructive on their behalf.
	Alive(node string) bool
	Members() []cluster.Member
}

// AlwaysAlive is a trivial Liveness for tests / single-node runs: everyone is up.
type AlwaysAlive struct{}

func (AlwaysAlive) Alive(string) bool         { return true }
func (AlwaysAlive) Members() []cluster.Member { return nil }

// Controller is the cluster brain. It owns global desired state and converges
// the cluster toward it by pushing assignments to agents — the same
// reconcile-loop idea as Stage 0, but now one rung up and across the network.
//
// The Stage 1 lesson lives in reconcileOnce: it pushes at-least-once and never
// trusts the Apply ack. "Did it land?" is answered by re-observing the agent,
// not by the reply to the push — because a lost reply and a lost request are
// indistinguishable to the sender.
type Controller struct {
	client   AgentClient
	clock    agent.Clock
	interval time.Duration
	log      *slog.Logger
	live     Liveness

	mu       sync.Mutex
	registry map[string]string         // node id -> address
	desired  map[string]api.Assignment // vm name -> assignment
	gen      map[string]int            // vm name -> generation (for idempotency keys)
}

// NewController builds a controller over a static node registry.
func NewController(registry map[string]string, client AgentClient, clock agent.Clock, interval time.Duration, live Liveness, log *slog.Logger) *Controller {
	if log == nil {
		log = slog.Default()
	}
	if live == nil {
		live = AlwaysAlive{}
	}
	reg := make(map[string]string, len(registry))
	for k, v := range registry {
		reg[k] = v
	}
	return &Controller{
		client:   client,
		clock:    clock,
		interval: interval,
		live:     live,
		log:      log.With("component", "control"),
		registry: reg,
		desired:  make(map[string]api.Assignment),
		gen:      make(map[string]int),
	}
}

// Place declares a VM desired on a named node. Re-placing the same VM with a
// changed spec bumps its generation, producing a fresh idempotency key so agents
// treat it as new work.
func (c *Controller) Place(a api.Assignment) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry[a.Node]; !ok {
		return fmt.Errorf("unknown node %q", a.Node)
	}
	prev, existed := c.desired[a.Name]
	c.desired[a.Name] = a
	if !existed || prev != a {
		c.gen[a.Name]++
	}
	c.log.Info("placed", "vm", a.Name, "node", a.Node, "gen", c.gen[a.Name])
	return nil
}

// Unplace removes a VM from global desired and tells its node to drop it.
func (c *Controller) Unplace(ctx context.Context, name string) {
	c.mu.Lock()
	a, ok := c.desired[name]
	delete(c.desired, name)
	delete(c.gen, name)
	addr := c.registry[a.Node]
	c.mu.Unlock()
	if !ok {
		return
	}
	c.log.Info("unplaced", "vm", name, "node", a.Node)
	// Best-effort immediate remove; if it fails the agent's own reconcile won't
	// re-create it (control no longer desires it) so this is just promptness.
	if addr != "" {
		if err := c.client.Remove(ctx, addr, name); err != nil {
			c.log.Warn("remove rpc failed (agent will not be re-pushed)", "vm", name, "err", err)
		}
	}
}

// snapshot copies desired + key material so RPCs don't hold the lock.
func (c *Controller) snapshot() (map[string]api.Assignment, map[string]string, map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := make(map[string]api.Assignment, len(c.desired))
	for k, v := range c.desired {
		d[k] = v
	}
	r := make(map[string]string, len(c.registry))
	for k, v := range c.registry {
		r[k] = v
	}
	g := make(map[string]int, len(c.gen))
	for k, v := range c.gen {
		g[k] = v
	}
	return d, r, g
}

// Run drives the control reconcile loop until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	c.log.Info("controller started", "interval", c.interval)
	for {
		c.reconcileOnce(ctx)
		select {
		case <-ctx.Done():
			c.log.Info("controller stopped")
			return
		case <-c.clock.After(c.interval):
		}
	}
}

// Members exposes the cluster membership view for `murmurctl nodes`.
func (c *Controller) Members() []cluster.Member { return c.live.Members() }

// reconcileOnce pushes every assignment to its node and verifies by observation.
func (c *Controller) reconcileOnce(ctx context.Context) {
	desired, registry, gens := c.snapshot()

	// Observe every node first so we can decide what still needs pushing.
	observed := c.observeAll(ctx, registry)

	for name, a := range desired {
		addr := registry[a.Node]
		if addr == "" {
			c.log.Error("assignment references unknown node", "vm", name, "node", a.Node)
			continue
		}
		// Stage 2: stop hammering nodes membership believes are gone. Before
		// this, a dead node drew an infinite retry storm every tick. We can't
		// reschedule the VM elsewhere yet (that's Stage 4) — we just stop the
		// pointless spam and let `ps`/`nodes` tell the truth.
		if !c.live.Alive(a.Node) {
			c.log.Warn("skipping apply: node not alive", "vm", name, "node", a.Node)
			continue
		}
		// Don't trust acks: if we can already observe it running on its node,
		// there's nothing to do. We converge on observation, not on replies.
		if observed[a.Node][name] == vmm.Running {
			continue
		}
		key := fmt.Sprintf("%s:%d", name, gens[name])
		req := api.ApplyRequest{Spec: vmm.Spec{Name: name, Image: a.Image}, IdempotencyKey: key}
		resp, err := c.client.Apply(ctx, addr, req)
		if err != nil {
			// At-least-once: a failed push (timeout, refused, lost reply) is not
			// fatal. We simply try again next tick. Because Apply is idempotent
			// on the agent, retrying — even if the previous one secretly landed —
			// is safe.
			c.log.Warn("apply failed; will retry next tick", "vm", name, "node", a.Node, "err", err)
			continue
		}
		c.log.Info("apply pushed", "vm", name, "node", a.Node, "key", key, "applied", resp.Applied)
	}
}

// ReconcileOnce runs a single pass synchronously (for tests).
func (c *Controller) ReconcileOnce(ctx context.Context) { c.reconcileOnce(ctx) }

// observeAll lists every node, tolerating per-node failures (a down node just
// yields an empty view — its VMs will look not-running and get re-pushed).
func (c *Controller) observeAll(ctx context.Context, registry map[string]string) map[string]map[string]vmm.State {
	out := make(map[string]map[string]vmm.State, len(registry))
	for node, addr := range registry {
		states := map[string]vmm.State{}
		obs, err := c.client.List(ctx, addr)
		if err != nil {
			c.log.Warn("list failed; treating node as empty", "node", node, "err", err)
		} else {
			for _, o := range obs {
				states[o.Name] = o.State
			}
		}
		out[node] = states
	}
	return out
}

// PS joins global desired with per-node observation for `murmurctl ps`.
func (c *Controller) PS(ctx context.Context) []api.PSRow {
	desired, registry, _ := c.snapshot()
	observed := c.observeAll(ctx, registry)

	rows := make([]api.PSRow, 0, len(desired))
	for name, a := range desired {
		rows = append(rows, api.PSRow{
			Name:     name,
			Node:     a.Node,
			Desired:  true,
			Image:    a.Image,
			Observed: stateOf(observed, a.Node, name),
		})
	}
	// Also surface VMs observed somewhere but not desired (drift visibility).
	for node, states := range observed {
		for name, st := range states {
			if _, want := desired[name]; want {
				continue
			}
			rows = append(rows, api.PSRow{Name: name, Node: node, Desired: false, Observed: st})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

func stateOf(obs map[string]map[string]vmm.State, node, name string) vmm.State {
	if states, ok := obs[node]; ok {
		if st, ok := states[name]; ok {
			return st
		}
	}
	return vmm.Missing
}
