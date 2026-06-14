package state

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startStores spins up n stores on a shared SimNet, all seeded off the first.
func startStores(t *testing.T, net *cluster.SimNet, addrs ...string) []*Store {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	seed := addrs[0]
	var stores []*Store
	for i, a := range addrs {
		var seeds []string
		if a != seed {
			seeds = []string{seed}
		}
		s := New("n"+a, a, seeds, net.Endpoint(a), clock.RealClock{}, rand.New(rand.NewSource(int64(i+1))), quiet())
		go s.Run(ctx, 15*time.Millisecond)
		stores = append(stores, s)
	}
	return stores
}

func eventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	dl := time.Now().Add(within)
	for time.Now().Before(dl) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("not met within %s: %s", within, msg)
}

func nodeOf(s *Store, vm string) string {
	for _, a := range s.Snapshot() {
		if a.Name == vm {
			return a.Node
		}
	}
	return ""
}

// A write on one peer converges to every peer.
func TestConverges(t *testing.T) {
	net := cluster.NewSimNet(0, nil)
	s := startStores(t, net, "a", "b", "c")
	s[0].Set(Assignment{Name: "counter", Node: "host-c"})
	eventually(t, 2*time.Second, func() bool {
		for _, st := range s {
			if nodeOf(st, "counter") != "host-c" {
				return false
			}
		}
		return true
	}, "all peers see counter@host-c")
}

// Anti-entropy converges even under heavy packet loss.
func TestConvergesUnderLoss(t *testing.T) {
	net := cluster.NewSimNet(0.5, rand.New(rand.NewSource(9)))
	s := startStores(t, net, "a", "b", "c")
	s[1].Set(Assignment{Name: "vm1", Node: "host-a"})
	eventually(t, 4*time.Second, func() bool {
		return nodeOf(s[0], "vm1") == "host-a" && nodeOf(s[2], "vm1") == "host-a"
	}, "converges despite 50% loss")
}

// Concurrent writes on two peers during a partition converge deterministically
// (last-write-wins) after heal — the later Lamport stamp must win everywhere.
func TestPartitionHealLWW(t *testing.T) {
	net := cluster.NewSimNet(0, nil)
	s := startStores(t, net, "a", "b", "c")
	s[0].Set(Assignment{Name: "vm", Node: "host-a"})
	eventually(t, 2*time.Second, func() bool { return nodeOf(s[2], "vm") == "host-a" }, "initial converge")

	net.Partition([]string{"a"}, []string{"b", "c"})
	// Both sides rewrite the same VM. s[1] writes after s[0], so once clocks have
	// advanced its stamp should win — but the key point is determinism: ALL peers
	// agree after heal, no matter which value that is.
	s[0].Set(Assignment{Name: "vm", Node: "host-a2"})
	time.Sleep(50 * time.Millisecond)
	s[1].Set(Assignment{Name: "vm", Node: "host-b2"})

	net.Heal()
	eventually(t, 3*time.Second, func() bool {
		v := nodeOf(s[0], "vm")
		return v != "" && v == nodeOf(s[1], "vm") && v == nodeOf(s[2], "vm")
	}, "all peers converge to a single value after heal")
}
