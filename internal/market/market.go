// Package market is murmur's leaderless scheduler. Each peer runs one and they
// collectively decide placement with no coordinator: a VM that nobody live owns
// is up for grabs, and any peer with spare capacity claims it by writing into the
// replicated claims CRDT. Races are resolved after the fact by the CRDT's
// last-write-wins merge — which means mutual exclusion here is only
// *approximate*: for a gossip-convergence window (or during a partition) two
// peers can both believe they own a VM. That hole is deliberate; confronting it
// is the whole point of Stage 6 (fencing).
package market

import (
	"context"
	"log/slog"
	"time"

	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/state"
)

// Membership is the liveness oracle (SWIM). A claim is honoured only while its
// owner is believed Alive — that belief is the "lease". Note it's a belief, not
// truth: a false "dead" causes a double-claim, the cost we pay for no leader.
type Membership interface {
	Alive(node string) bool
}

// Scheduler is one peer's market participant.
type Scheduler struct {
	self     string
	capacity int
	store    *state.Store
	live     Membership
	clk      clock.Clock
	log      *slog.Logger
}

func New(self string, capacity int, store *state.Store, live Membership, clk clock.Clock, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	if capacity < 1 {
		capacity = 1
	}
	return &Scheduler{
		self: self, capacity: capacity, store: store, live: live, clk: clk,
		log: log.With("component", "market", "node", self),
	}
}

// Run drives the market loop until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context, interval time.Duration) {
	s.log.Info("market started", "capacity", s.capacity)
	next := s.clk.After(interval)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("market stopped")
			return
		case <-next:
			s.scheduleOnce()
			next = s.clk.After(interval)
		}
	}
}

// owned reports whether claim c is held by a peer we believe alive.
func (s *Scheduler) liveOwner(c state.Claim) bool {
	return c.Owner != "" && s.live.Alive(c.Owner)
}

// ScheduleOnce makes one pass of placement decisions. Exported for tests.
func (s *Scheduler) ScheduleOnce() { s.scheduleOnce() }

func (s *Scheduler) scheduleOnce() {
	desired := s.store.Desired()
	desiredNames := make(map[string]bool, len(desired))
	for _, d := range desired {
		desiredNames[d.Name] = true
	}
	claims := s.store.Claims()

	// Count what I currently, livingly own — that's my load against capacity.
	mine := 0
	for name, c := range claims {
		if c.Owner == s.self && desiredNames[name] {
			mine++
		}
		// Housekeeping: drop my claim on VMs no longer desired so I stop running
		// them (the reconciler will kill the VM; releasing the claim keeps the
		// shared view honest).
		if c.Owner == s.self && !desiredNames[name] {
			s.store.SetClaim(name, state.Claim{Owner: ""})
		}
	}

	// Claim claimable VMs until I'm at capacity. Claimable = desired with no live
	// owner (unclaimed, or owned by a node SWIM now believes dead).
	for _, d := range desired {
		if mine >= s.capacity {
			break
		}
		c := claims[d.Name]
		if s.liveOwner(c) {
			continue // someone alive already has it
		}
		if c.Owner == s.self {
			continue // already mine (shouldn't happen given count, but be safe)
		}
		// Re-claim: keep any SnapshotRef so the new owner restores rather than
		// boots fresh. We take ownership; LWW + gossip sorts out concurrent claims.
		newClaim := state.Claim{Owner: s.self, SnapshotRef: c.SnapshotRef}
		s.store.SetClaim(d.Name, newClaim)
		if c.Owner == "" {
			s.log.Info("claimed", "vm", d.Name)
		} else {
			s.log.Warn("re-claimed from dead owner", "vm", d.Name, "was", c.Owner, "snapshot", c.SnapshotRef)
		}
		mine++
	}
}
