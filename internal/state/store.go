// Package state is murmur's replicated desired-state store. It wraps a CRDT
// (internal/crdt LWWMap) of VM assignments and anti-entropy-gossips it to peers
// over a cluster.Transport. There is no leader and no central copy: every peer
// holds the full map and they converge by exchanging state. It knows nothing
// about VMs beyond treating an Assignment as opaque data to replicate.
package state

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"sync"

	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/crdt"
)

// Assignment is one desired VM and the node meant to run it. (Stage 3 sets Node
// manually; Stage 4 will let the market decide it via a claim.)
type Assignment struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
	Node  string `json:"node"`
}

// wire is the anti-entropy gossip payload: a full dump of the CRDT plus the
// sender's known peers (so the peer graph becomes fully connected over time,
// not just a star around the seeds).
type wire struct {
	From    string                `json:"from"`
	Entries map[string]crdt.Entry `json:"entries"`
	Peers   []string              `json:"peers,omitempty"`
}

// Store is one peer's replica.
type Store struct {
	node     string
	selfAddr string

	mu    sync.Mutex
	clk   *crdt.Lamport
	m     *crdt.LWWMap
	peers map[string]bool // peer state-gossip addresses

	tr   cluster.Transport
	pick clock.Clock
	rnd  *rand.Rand
	log  *slog.Logger
}

// New builds a store. selfAddr is this peer's state-gossip address; seeds are
// other peers' state-gossip addresses to bootstrap from.
func New(node, selfAddr string, seeds []string, tr cluster.Transport, pick clock.Clock, rnd *rand.Rand, log *slog.Logger) *Store {
	if log == nil {
		log = slog.Default()
	}
	if rnd == nil {
		rnd = rand.New(rand.NewSource(1))
	}
	peers := map[string]bool{}
	for _, s := range seeds {
		if s != "" && s != selfAddr {
			peers[s] = true
		}
	}
	return &Store{
		node:     node,
		selfAddr: selfAddr,
		clk:      crdt.NewLamport(node),
		m:        crdt.NewLWWMap(),
		peers:    peers,
		tr:       tr,
		pick:     pick,
		rnd:      rnd,
		log:      log.With("component", "state", "node", node),
	}
}

// Set records a desired assignment, stamping it with our Lamport clock. Local
// writes converge with everyone else's by stamp ordering.
func (s *Store) Set(a Assignment) {
	b, _ := json.Marshal(a)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m.Set(a.Name, b, s.clk.Tick())
	s.log.Info("desired set", "vm", a.Name, "node", a.Node)
}

// Remove tombstones an assignment.
func (s *Store) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m.Delete(name, s.clk.Tick())
	s.log.Info("desired removed", "vm", name)
}

// Snapshot returns all live assignments (converged view).
func (s *Store) Snapshot() []Assignment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decodeLocked()
}

// AssignmentsFor returns the live assignments targeted at a given node.
func (s *Store) AssignmentsFor(node string) []Assignment {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Assignment
	for _, a := range s.decodeLocked() {
		if a.Node == node {
			out = append(out, a)
		}
	}
	return out
}

func (s *Store) decodeLocked() []Assignment {
	out := make([]Assignment, 0)
	for _, v := range s.m.Entries() {
		var a Assignment
		if json.Unmarshal(v, &a) == nil {
			out = append(out, a)
		}
	}
	return out
}
