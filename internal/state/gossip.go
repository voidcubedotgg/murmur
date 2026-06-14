package state

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/voidcubedotgg/murmur/internal/cluster"
)

// Run drives anti-entropy gossip under the real clock until ctx is cancelled —
// a thin pacing loop over Tick/Deliver, the same logic the simulator drives.
// Each period it pushes its full CRDT state to one random peer; received state
// is merged. Because the CRDT merge is commutative/associative/idempotent, this
// random pairwise exchange converges the whole cluster without any coordinator —
// losing, reordering, or duplicating a packet only delays convergence.
func (s *Store) Run(ctx context.Context, period time.Duration) {
	s.gossipEvery = period
	s.log.Info("state gossip started", "addr", s.selfAddr, "period", period)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("state gossip stopped")
			return
		case <-s.pick.After(period):
			s.Tick(s.pick.Now())
		case pkt := <-s.tr.Receive():
			s.Deliver(pkt)
		}
	}
}

// Tick pushes state to a random peer if the gossip interval has elapsed at
// virtual time now. SetGossipInterval (or Run) must have set the period.
func (s *Store) Tick(now time.Time) {
	if s.gossipEvery <= 0 {
		return
	}
	if s.nextAt.IsZero() {
		s.nextAt = now
	}
	if now.Before(s.nextAt) {
		return
	}
	s.nextAt = now.Add(s.gossipEvery)
	s.gossipOnce(context.Background())
}

// Deliver merges one inbound gossip packet.
func (s *Store) Deliver(pkt cluster.Packet) { s.onPacket(pkt.Payload) }

// SetGossipInterval sets the gossip period for Tick-driven (simulator) use.
func (s *Store) SetGossipInterval(d time.Duration) { s.gossipEvery = d }

func (s *Store) gossipOnce(ctx context.Context) {
	s.mu.Lock()
	peers := make([]string, 0, len(s.peers))
	for p := range s.peers {
		peers = append(peers, p)
	}
	sort.Strings(peers) // deterministic peer order before the seeded random pick
	payload := wire{From: s.selfAddr, Desired: s.desired.Raw(), Claims: s.claims.Raw(), Peers: peers}
	s.mu.Unlock()

	if len(peers) == 0 {
		return
	}
	target := peers[s.rnd.Intn(len(peers))]
	b, _ := json.Marshal(payload)
	_ = s.tr.Send(ctx, target, b)
}

func (s *Store) onPacket(b []byte) {
	var w wire
	if json.Unmarshal(b, &w) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Merge both CRDTs and advance our Lamport clock past everything we just saw,
	// so our future local writes order strictly after observed ones.
	s.desired.Merge(w.Desired)
	s.claims.Merge(w.Claims)
	for _, e := range w.Desired {
		s.clk.Witness(e.Stamp)
	}
	for _, e := range w.Claims {
		s.clk.Witness(e.Stamp)
	}
	// Learn peers transitively so the gossip graph becomes fully connected
	// rather than a star around the seeds.
	if w.From != "" && w.From != s.selfAddr {
		s.peers[w.From] = true
	}
	for _, p := range w.Peers {
		if p != "" && p != s.selfAddr {
			s.peers[p] = true
		}
	}
}
