package cluster

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/clock"
)

// These tests run real time with short durations against the SimNet. That is
// not yet fully deterministic (fake-clock SWIM is a Stage 7 goal) but the
// SimNet gives us deterministic *topology* control: drops and partitions.

func testConfig() Config {
	return Config{
		Period:           20 * time.Millisecond,
		AckTimeout:       6 * time.Millisecond,
		SuspicionTimeout: 80 * time.Millisecond,
		IndirectK:        2,
		GossipFanout:     6,
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type node struct {
	id     string
	swim   *SWIM
	cancel context.CancelFunc
}

// startCluster spins up members with the given ids on a shared SimNet; each
// joins via the first id as seed.
func startCluster(t *testing.T, net *SimNet, ids ...string) map[string]*node {
	t.Helper()
	nodes := map[string]*node{}
	seed := ids[0]
	for _, id := range ids {
		s := NewSWIM(id, id, testConfig(), net.Endpoint(id), clock.RealClock{}, rand.New(rand.NewSource(int64(len(id)+len(nodes)))), quietLogger())
		ctx, cancel := context.WithCancel(context.Background())
		go s.swimRunForTest(ctx)
		nodes[id] = &node{id: id, swim: s, cancel: cancel}
	}
	for _, id := range ids {
		if id != seed {
			nodes[id].swim.Join(context.Background(), []string{seed})
		}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.cancel()
		}
	})
	return nodes
}

// small wrapper so we can reuse Run with a context.
func (s *SWIM) swimRunForTest(ctx context.Context) { s.Run(ctx) }

func eventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", within, msg)
}

func belief(n *node, id string) State {
	for _, m := range n.swim.Members() {
		if m.ID == id {
			return m.State
		}
	}
	return ""
}

// All members converge to believing every other member Alive.
func TestConverge(t *testing.T) {
	net := NewSimNet(0, nil)
	nodes := startCluster(t, net, "a", "b", "c")
	eventually(t, 2*time.Second, func() bool {
		for _, n := range nodes {
			for _, id := range []string{"a", "b", "c"} {
				if belief(n, id) != Alive {
					return false
				}
			}
		}
		return true
	}, "all members alive everywhere")
}

// Killing a member leads survivors to suspect then declare it Dead within a
// bounded time. This is the stage's headline: the cluster *agrees* it's gone.
func TestDetectsDeath(t *testing.T) {
	net := NewSimNet(0, nil)
	nodes := startCluster(t, net, "a", "b", "c")
	eventually(t, 2*time.Second, func() bool { return belief(nodes["a"], "c") == Alive }, "c initially alive")

	// Kill c: stop its goroutine so it never answers a probe again.
	nodes["c"].cancel()

	eventually(t, 2*time.Second, func() bool {
		return belief(nodes["a"], "c") == Dead && belief(nodes["b"], "c") == Dead
	}, "survivors agree c is Dead")
}

// A node reachable only indirectly (direct path blocked both ways) must NOT be
// declared dead — indirect probing keeps it alive. This is why SWIM beats naive
// heartbeats: a single broken link is not a death sentence.
func TestIndirectProbeAvoidsFalsePositive(t *testing.T) {
	net := NewSimNet(0, nil)
	nodes := startCluster(t, net, "a", "b", "c")
	eventually(t, 2*time.Second, func() bool {
		return belief(nodes["a"], "c") == Alive && belief(nodes["c"], "a") == Alive
	}, "converged")

	// Sever only the direct a<->c link. b can still relay (ping-req).
	net.Block("a", "c")
	net.Block("c", "a")

	// Hold for well beyond the suspicion timeout; c must stay alive via b.
	time.Sleep(400 * time.Millisecond)
	if got := belief(nodes["a"], "c"); got == Dead {
		t.Fatalf("a wrongly declared c Dead despite working indirect path (state=%s)", got)
	}
	if got := belief(nodes["c"], "a"); got == Dead {
		t.Fatalf("c wrongly declared a Dead despite working indirect path (state=%s)", got)
	}
}

// A partition makes each side declare the other Dead; healing (with a re-join
// nudge, standing in for anti-entropy) brings everyone back Alive via first-hand
// resurrection.
func TestPartitionThenHeal(t *testing.T) {
	net := NewSimNet(0, nil)
	nodes := startCluster(t, net, "a", "b", "c")
	eventually(t, 2*time.Second, func() bool {
		return belief(nodes["a"], "c") == Alive && belief(nodes["c"], "a") == Alive
	}, "converged")

	net.Partition([]string{"a", "b"}, []string{"c"})
	eventually(t, 2*time.Second, func() bool {
		return belief(nodes["a"], "c") == Dead && belief(nodes["c"], "a") == Dead
	}, "each side declares the other Dead")

	net.Heal()
	// Nudge re-contact (pure SWIM has no anti-entropy; a real deployment uses a
	// periodic full-state sync or rejoin here).
	nodes["c"].swim.Join(context.Background(), []string{"a"})

	eventually(t, 2*time.Second, func() bool {
		return belief(nodes["a"], "c") == Alive && belief(nodes["c"], "a") == Alive
	}, "cluster heals back to all-alive")
}

// Bootstrap must survive a lossy network: with heavy packet loss a single join
// packet often vanishes, so the loop has to keep retrying via seeds (and
// anti-entropy must fill in the view). Without those fixes a node could be
// isolated forever.
func TestJoinSurvivesPacketLoss(t *testing.T) {
	net := NewSimNet(0.5, rand.New(rand.NewSource(7))) // drop half of all packets
	nodes := startCluster(t, net, "a", "b", "c")
	eventually(t, 4*time.Second, func() bool {
		for _, n := range nodes {
			for _, id := range []string{"a", "b", "c"} {
				if belief(n, id) != Alive {
					return false
				}
			}
		}
		return true
	}, "cluster still converges despite 50% packet loss")
}

// A restarted member (fresh process, same id, incarnation reset) rejoins and is
// believed Alive again, even though peers had marked it Dead.
func TestRejoinAfterDeath(t *testing.T) {
	net := NewSimNet(0, nil)
	nodes := startCluster(t, net, "a", "b", "c")
	eventually(t, 2*time.Second, func() bool { return belief(nodes["a"], "c") == Alive }, "c alive")

	nodes["c"].cancel()
	eventually(t, 2*time.Second, func() bool { return belief(nodes["a"], "c") == Dead }, "c dead")

	// Restart c as a brand-new member on the same address.
	c2 := NewSWIM("c", "c", testConfig(), net.Endpoint("c"), clock.RealClock{}, rand.New(rand.NewSource(99)), quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c2.Run(ctx)
	c2.Join(context.Background(), []string{"a"})

	eventually(t, 2*time.Second, func() bool {
		return belief(nodes["a"], "c") == Alive && belief(nodes["b"], "c") == Alive
	}, "restarted c rejoins as Alive")
}
