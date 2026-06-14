package state

import (
	"context"
	"encoding/json"
	"time"
)

// Run drives anti-entropy gossip until ctx is cancelled. Each period it pushes
// its full CRDT state to one random peer; received state is merged. Because the
// CRDT merge is commutative/associative/idempotent, this random pairwise
// exchange converges the whole cluster without any coordinator — losing,
// reordering, or duplicating a packet only delays convergence, never breaks it.
func (s *Store) Run(ctx context.Context, period time.Duration) {
	s.log.Info("state gossip started", "addr", s.selfAddr, "period", period)
	next := s.pick.After(period)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("state gossip stopped")
			return
		case <-next:
			s.gossipOnce(ctx)
			next = s.pick.After(period)
		case pkt := <-s.tr.Receive():
			s.onPacket(pkt.Payload)
		}
	}
}

func (s *Store) gossipOnce(ctx context.Context) {
	s.mu.Lock()
	peers := make([]string, 0, len(s.peers))
	for p := range s.peers {
		peers = append(peers, p)
	}
	payload := wire{From: s.selfAddr, Entries: s.m.Raw(), Peers: peers}
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
	// Merge the CRDT and advance our Lamport clock past everything we just saw,
	// so our future local writes order strictly after observed ones.
	s.m.Merge(w.Entries)
	for _, e := range w.Entries {
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
