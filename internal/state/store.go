// Package state is murmur's replicated desired-state store. It holds two CRDTs —
// the *desired* set (what VMs the user wants) and the *claims* map (who owns/runs
// each, decided by the market) — and anti-entropy-gossips both to peers over a
// cluster.Transport. No leader, no central copy: peers converge by exchanging
// state. It knows nothing about VMs beyond replicating opaque records.
package state

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/crdt"
)

// Spec is a desired VM (user intent). No node: placement is the market's job.
type Spec struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
}

// Claim is the market's decision about a VM: which peer owns (runs) it, and the
// snapshot a new owner should restore from. Owner "" means unclaimed/up-for-grabs.
type Claim struct {
	Owner       string `json:"owner"`
	SnapshotRef string `json:"snapshot_ref,omitempty"`
}

// wire is the anti-entropy gossip payload: full dumps of both CRDTs plus known
// peers (so the gossip graph self-connects beyond the seed star).
type wire struct {
	From    string                `json:"from"`
	Desired map[string]crdt.Entry `json:"desired"`
	Claims  map[string]crdt.Entry `json:"claims"`
	Peers   []string              `json:"peers,omitempty"`
}

// Store is one peer's replica of desired + claims.
type Store struct {
	node     string
	selfAddr string

	mu      sync.Mutex
	clk     *crdt.Lamport
	desired *crdt.LWWMap
	claims  *crdt.LWWMap
	peers   map[string]bool

	tr          cluster.Transport
	pick        clock.Clock
	rnd         *rand.Rand
	log         *slog.Logger
	gossipEvery time.Duration // set by Run or SetGossipInterval
	nextAt      time.Time     // next virtual time to gossip
}

// New builds a store. selfAddr is this peer's state-gossip address; seeds bootstrap.
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
		desired:  crdt.NewLWWMap(),
		claims:   crdt.NewLWWMap(),
		peers:    peers,
		tr:       tr,
		pick:     pick,
		rnd:      rnd,
		log:      log.With("component", "state", "node", node),
	}
}

// --- desired set --------------------------------------------------------

// SetDesired records that the user wants this VM (stamped via our clock).
func (s *Store) SetDesired(spec Spec) {
	b, _ := json.Marshal(spec)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired.Set(spec.Name, b, s.clk.Tick())
	s.log.Info("desired set", "vm", spec.Name)
}

// RemoveDesired tombstones a VM (and its claim, so survivors stop running it).
func (s *Store) RemoveDesired(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired.Delete(name, s.clk.Tick())
	s.claims.Delete(name, s.clk.Tick())
	s.log.Info("desired removed", "vm", name)
}

// Desired returns all live desired specs, sorted by name. The sort matters for
// determinism: callers (e.g. the market) make capacity-limited decisions by
// iterating this, and Go map order is randomized — unsorted, two replays of the
// same seed would pick different VMs.
func (s *Store) Desired() []Spec {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Spec, 0)
	for _, v := range s.desired.Entries() {
		var sp Spec
		if json.Unmarshal(v, &sp) == nil {
			out = append(out, sp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// --- claims -------------------------------------------------------------

// SetClaim writes a claim for a VM (stamped via our clock). Used by the market to
// claim/re-claim, and by the owner to record a fresh SnapshotRef.
func (s *Store) SetClaim(name string, c Claim) {
	b, _ := json.Marshal(c)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims.Set(name, b, s.clk.Tick())
}

// Claim returns the current claim for a VM.
func (s *Store) Claim(name string) (Claim, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.claims.Get(name)
	if !ok {
		return Claim{}, false
	}
	var c Claim
	if json.Unmarshal(v, &c) != nil {
		return Claim{}, false
	}
	return c, true
}

// Claims returns the converged claim per live-claimed VM.
func (s *Store) Claims() map[string]Claim {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Claim{}
	for k, v := range s.claims.Entries() {
		var c Claim
		if json.Unmarshal(v, &c) == nil {
			out[k] = c
		}
	}
	return out
}
