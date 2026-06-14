// Package cluster is murmur's membership + failure detector. It is a pure
// distributed-systems library: it knows NOTHING about VMs (CLAUDE.md boundary).
// It answers one hard question — "who is in the cluster, and who is probably
// dead?" — using a hand-rolled SWIM protocol.
//
// The central honesty of this package: "dead" is never a fact. The network
// can't distinguish a crashed node from a slow one or a partitioned one. SWIM
// only ever produces a *suspicion* that, after enough unanswered probes,
// hardens into a decision. Every "Dead" here is a decision under uncertainty.
package cluster

import "context"

// Packet is one datagram received from the network.
type Packet struct {
	From    string // source address as the transport sees it
	Payload []byte
}

// Transport is the injectable network. SWIM never touches a socket directly —
// it sends and receives opaque bytes through this interface — so tests can swap
// in a simulated network that drops, delays, and partitions deterministically.
// That substitution is what makes failure sequences replayable.
type Transport interface {
	// Send delivers b to addr. Best-effort and unreliable by contract: it may
	// silently fail, which is exactly the world SWIM is designed for.
	Send(ctx context.Context, addr string, b []byte) error
	// Receive yields inbound packets.
	Receive() <-chan Packet
	// LocalAddr is this endpoint's address.
	LocalAddr() string
}
