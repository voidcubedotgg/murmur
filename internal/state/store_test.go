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

func hasDesired(s *Store, name string) bool {
	for _, d := range s.Desired() {
		if d.Name == name {
			return true
		}
	}
	return false
}

// Desired writes on one peer converge to every peer.
func TestDesiredConverges(t *testing.T) {
	net := cluster.NewSimNet(0, nil)
	s := startStores(t, net, "a", "b", "c")
	s[0].SetDesired(Spec{Name: "web1"})
	eventually(t, 2*time.Second, func() bool {
		return hasDesired(s[1], "web1") && hasDesired(s[2], "web1")
	}, "web1 desired everywhere")
}

// Claims converge too, and the higher-stamp owner wins everywhere.
func TestClaimsConverge(t *testing.T) {
	net := cluster.NewSimNet(0, nil)
	s := startStores(t, net, "a", "b", "c")
	s[0].SetDesired(Spec{Name: "web1"})
	s[0].SetClaim("web1", Claim{Owner: "na"})
	eventually(t, 2*time.Second, func() bool {
		c, ok := s[2].Claim("web1")
		return ok && c.Owner == "na"
	}, "claim converges")
}

// Both maps survive heavy loss via anti-entropy.
func TestConvergesUnderLoss(t *testing.T) {
	net := cluster.NewSimNet(0.5, rand.New(rand.NewSource(9)))
	s := startStores(t, net, "a", "b", "c")
	s[1].SetDesired(Spec{Name: "vm1"})
	s[1].SetClaim("vm1", Claim{Owner: "nb", SnapshotRef: "/tmp/x"})
	eventually(t, 4*time.Second, func() bool {
		c, ok := s[0].Claim("vm1")
		return hasDesired(s[2], "vm1") && ok && c.Owner == "nb" && c.SnapshotRef == "/tmp/x"
	}, "converges despite 50% loss")
}

// Concurrent claims during a partition converge to one owner after heal.
func TestPartitionHealLWW(t *testing.T) {
	net := cluster.NewSimNet(0, nil)
	s := startStores(t, net, "a", "b", "c")
	s[0].SetDesired(Spec{Name: "vm"})
	eventually(t, 2*time.Second, func() bool { return hasDesired(s[2], "vm") }, "desired converge")

	net.Partition([]string{"a"}, []string{"b", "c"})
	s[0].SetClaim("vm", Claim{Owner: "na"})
	time.Sleep(50 * time.Millisecond)
	s[1].SetClaim("vm", Claim{Owner: "nb"})

	net.Heal()
	eventually(t, 3*time.Second, func() bool {
		c0, _ := s[0].Claim("vm")
		c1, _ := s[1].Claim("vm")
		c2, _ := s[2].Claim("vm")
		return c0.Owner != "" && c0.Owner == c1.Owner && c1.Owner == c2.Owner
	}, "all peers converge to one owner after heal")
}
