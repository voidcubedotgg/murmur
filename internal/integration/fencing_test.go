package integration

import (
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/state"
)

// allHaveDesired reports whether every peer's converged desired set contains vm.
// Tests wait for this before partitioning: the split-brain (and the fenced
// failover) can only happen if both sides actually know the workload exists.
func allHaveDesired(peers map[string]*peer, vm string) bool {
	for _, p := range peers {
		found := false
		for _, d := range p.store.Desired() {
			if d.Name == vm {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// runningPeers returns the ids of peers currently running vm.
func runningPeers(peers map[string]*peer, vm string) []string {
	var out []string
	for id, p := range peers {
		if running(p.fake, vm) {
			out = append(out, id)
		}
	}
	return out
}

// Without fencing, a partition causes split-brain: each side's SWIM declares the
// other dead, each re-claims, and the SAME stateful VM runs in two places at
// once. This is the disaster the CRDT path leads to — convergence is not a lock.
func TestSplitBrainWithoutFencing(t *testing.T) {
	swimNet := cluster.NewSimNet(0, nil)
	stateNet := cluster.NewSimNet(0, nil)
	snapDir := t.TempDir()

	ids := []string{"host-a", "host-b", "host-c"}
	peers := map[string]*peer{}
	for i, id := range ids {
		seed := ""
		if i != 0 {
			seed = "host-a"
		}
		// fencing OFF
		peers[id] = startPeerFenced(t, swimNet, stateNet, snapDir, id, seed, 2, false, 3)
	}
	t.Cleanup(func() {
		for _, p := range peers {
			p.cancel()
		}
	})

	peers["host-a"].store.SetDesired(state.Spec{Name: "db"})
	eventually(t, 6*time.Second, func() bool { return len(runningPeers(peers, "db")) == 1 }, "db running on one peer")
	eventually(t, 6*time.Second, func() bool { return allHaveDesired(peers, "db") }, "desired converges to all peers")

	// Partition host-a away from {host-b,host-c} on BOTH gossip planes.
	partitionAll(swimNet, stateNet, []string{"host-a"}, []string{"host-b", "host-c"})

	// Disaster: db ends up running on BOTH sides at once.
	eventually(t, 10*time.Second, func() bool {
		r := runningPeers(peers, "db")
		// one on the {a} side, at least one on the {b,c} side
		aSide, bcSide := false, false
		for _, id := range r {
			if id == "host-a" {
				aSide = true
			} else {
				bcSide = true
			}
		}
		return aSide && bcSide
	}, "SPLIT-BRAIN: db should be running on both sides (the bug we're exposing)")
	t.Logf("split-brain confirmed: db running on %v simultaneously", runningPeers(peers, "db"))
}

// With quorum fencing, the same partition is safe: the minority {host-a} loses
// quorum and self-fences (stops db); only the majority {host-b,host-c} runs it.
// The cost is the minority's availability — CAP, made concrete.
func TestQuorumFencingPreventsSplitBrain(t *testing.T) {
	swimNet := cluster.NewSimNet(0, nil)
	stateNet := cluster.NewSimNet(0, nil)
	snapDir := t.TempDir()

	ids := []string{"host-a", "host-b", "host-c"}
	peers := map[string]*peer{}
	for i, id := range ids {
		seed := ""
		if i != 0 {
			seed = "host-a"
		}
		// fencing ON, cluster size 3 -> quorum 2
		peers[id] = startPeerFenced(t, swimNet, stateNet, snapDir, id, seed, 2, true, 3)
	}
	t.Cleanup(func() {
		for _, p := range peers {
			p.cancel()
		}
	})

	peers["host-a"].store.SetDesired(state.Spec{Name: "db"})
	eventually(t, 6*time.Second, func() bool { return len(runningPeers(peers, "db")) == 1 }, "db running on one peer")
	eventually(t, 6*time.Second, func() bool { return allHaveDesired(peers, "db") }, "desired converges to all peers")

	partitionAll(swimNet, stateNet, []string{"host-a"}, []string{"host-b", "host-c"})

	// Safety: minority host-a self-fences; majority runs exactly one copy; never
	// both sides at once.
	eventually(t, 10*time.Second, func() bool {
		r := runningPeers(peers, "db")
		if running(peers["host-a"].fake, "db") {
			return false // minority must NOT be running it
		}
		return len(r) == 1 && (r[0] == "host-b" || r[0] == "host-c")
	}, "minority self-fenced; majority runs exactly one copy")

	// And host-a must never run it again while partitioned.
	for i := 0; i < 10; i++ {
		if running(peers["host-a"].fake, "db") {
			t.Fatal("minority resumed running db — fencing failed")
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Logf("safe: db runs only on majority %v; host-a fenced", runningPeers(peers, "db"))
}

func addr(plane, id string) string { return plane + "-" + id }

// partitionAll severs groupA from groupB on both the swim and state planes
// (each plane uses "<plane>-<id>" endpoint addresses).
func partitionAll(swimNet, stateNet *cluster.SimNet, groupA, groupB []string) {
	for _, plane := range []struct {
		net  *cluster.SimNet
		name string
	}{{swimNet, "swim"}, {stateNet, "state"}} {
		var a, b []string
		for _, id := range groupA {
			a = append(a, addr(plane.name, id))
		}
		for _, id := range groupB {
			b = append(b, addr(plane.name, id))
		}
		plane.net.Partition(a, b)
	}
}
