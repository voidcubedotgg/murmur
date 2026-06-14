// Package crdt is murmur's hand-rolled conflict-free replicated data types.
//
// It is pure: it knows nothing about VMs, gossip, or the network. Its only job
// is to let independent replicas accept writes in any order, exchange state in
// any order (even duplicated or partially), and still end up identical. That
// property — convergence without coordination — is what lets murmur have no
// leader at all (see the CRDT path note in GOALS.md/CLAUDE.md).
//
// Everything here rests on three merge laws. A CRDT merge must be:
//   - commutative:  merge(a,b) == merge(b,a)        — order of gossip doesn't matter
//   - associative:  merge(merge(a,b),c) == merge(a,merge(b,c))
//   - idempotent:   merge(a,a) == a                 — re-delivering state is harmless
//
// If those hold, the network can reorder, duplicate, and delay messages freely
// and all replicas still converge. We prove them in the tests.
package crdt

// Stamp orders writes across the whole cluster. A Lamport counter gives a
// logical "happens-after" ordering; the Node id is a deterministic tiebreaker so
// two writes with the same counter still have a total order every replica agrees
// on. Without the tiebreaker, concurrent equal-counter writes would resolve
// differently on different replicas and never converge.
type Stamp struct {
	Lamport uint64 `json:"l"`
	Node    string `json:"n"`
}

// After reports whether s should win over other under last-write-wins. Higher
// Lamport wins; ties break on the larger Node id. Total and deterministic.
func (s Stamp) After(other Stamp) bool {
	if s.Lamport != other.Lamport {
		return s.Lamport > other.Lamport
	}
	return s.Node > other.Node
}

// Lamport is a logical clock owned by one node. It is NOT wall-clock time: it
// only captures causality ("this write came after one I'd already seen"), which
// is all last-write-wins needs to be deterministic.
type Lamport struct {
	node string
	t    uint64
}

// NewLamport builds a clock tagged with this node's id.
func NewLamport(node string) *Lamport { return &Lamport{node: node} }

// Tick advances the clock for a local write and returns the stamp to attach.
func (c *Lamport) Tick() Stamp {
	c.t++
	return Stamp{Lamport: c.t, Node: c.node}
}

// Witness advances our clock past a stamp we received, so our next local write
// is ordered strictly after anything we've observed. This is the rule that makes
// Lamport clocks track happens-before across nodes.
func (c *Lamport) Witness(s Stamp) {
	if s.Lamport > c.t {
		c.t = s.Lamport
	}
}
