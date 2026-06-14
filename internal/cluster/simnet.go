package cluster

import (
	"context"
	"math/rand"
	"sync"
)

// SimNet is an in-memory network used to test the failure detector
// deterministically. It is NOT test-only: the same controllable network is the
// foundation for the Stage 7 simulation testing, where we replay exact failure
// sequences. It can drop links, partition the cluster, and lose packets at a
// configurable rate, all driven by a seeded rand.
type SimNet struct {
	mu        sync.Mutex
	endpoints map[string]*simEndpoint
	blocked   map[[2]string]bool // directional from->to blocks
	loss      float64
	rnd       *rand.Rand
}

// NewSimNet builds an empty simulated network. loss is per-packet drop
// probability [0,1]; rnd makes drops reproducible.
func NewSimNet(loss float64, rnd *rand.Rand) *SimNet {
	if rnd == nil {
		rnd = rand.New(rand.NewSource(1))
	}
	return &SimNet{
		endpoints: make(map[string]*simEndpoint),
		blocked:   make(map[[2]string]bool),
		loss:      loss,
		rnd:       rnd,
	}
}

// Endpoint returns (creating if needed) the Transport for addr.
func (n *SimNet) Endpoint(addr string) Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	ep, ok := n.endpoints[addr]
	if !ok {
		ep = &simEndpoint{net: n, addr: addr, recv: make(chan Packet, 256)}
		n.endpoints[addr] = ep
	}
	return ep
}

// Block drops all packets from->to (one direction).
func (n *SimNet) Block(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[[2]string{from, to}] = true
}

// Partition isolates two groups from each other (both directions).
func (n *SimNet) Partition(groupA, groupB []string) {
	for _, a := range groupA {
		for _, b := range groupB {
			n.Block(a, b)
			n.Block(b, a)
		}
	}
}

// Heal removes all blocks.
func (n *SimNet) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[[2]string]bool)
}

func (n *SimNet) deliver(from, to string, b []byte) {
	n.mu.Lock()
	if n.blocked[[2]string{from, to}] {
		n.mu.Unlock()
		return
	}
	if n.loss > 0 && n.rnd.Float64() < n.loss {
		n.mu.Unlock()
		return
	}
	ep := n.endpoints[to]
	n.mu.Unlock()
	if ep == nil {
		return
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	// Non-blocking: a full inbox drops the packet, just like a real lossy net.
	select {
	case ep.recv <- Packet{From: from, Payload: cp}:
	default:
	}
}

type simEndpoint struct {
	net  *SimNet
	addr string
	recv chan Packet
}

func (e *simEndpoint) Send(_ context.Context, to string, b []byte) error {
	e.net.deliver(e.addr, to, b)
	return nil
}
func (e *simEndpoint) Receive() <-chan Packet { return e.recv }
func (e *simEndpoint) LocalAddr() string      { return e.addr }
