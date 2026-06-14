// Package integration wires the whole stack in-process and asserts the
// north-star: a stateful workload survives the death of its host, restored on a
// survivor, with no leader anywhere. It is the regression guard for the demo.
package integration

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/market"
	"github.com/voidcubedotgg/murmur/internal/state"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// peer bundles one node's full stack, exactly as cmd/murmurd wires it.
type peer struct {
	id     string
	store  *state.Store
	swim   *cluster.SWIM
	fake   *vmm.Fake
	cancel context.CancelFunc
}

func eventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	dl := time.Now().Add(within)
	for time.Now().Before(dl) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not met within %s: %s", within, msg)
}

func startPeer(t *testing.T, swimNet, stateNet *cluster.SimNet, snapDir, id string, seedID string, capacity int) *peer {
	return startPeerFenced(t, swimNet, stateNet, snapDir, id, seedID, capacity, false, 0)
}

// startPeerFenced wires a peer with optional quorum fencing, mirroring cmd/murmurd.
func startPeerFenced(t *testing.T, swimNet, stateNet *cluster.SimNet, snapDir, id, seedID string, capacity int, fencing bool, clusterSize int) *peer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	swimAddr, stateAddr := "swim-"+id, "state-"+id
	var swimSeeds, stateSeeds []string
	if seedID != "" {
		swimSeeds = []string{"swim-" + seedID}
		stateSeeds = []string{"state-" + seedID}
	}

	store := state.New(id, stateAddr, stateSeeds, stateNet.Endpoint(stateAddr), clock.RealClock{},
		rand.New(rand.NewSource(int64(len(id)))), quiet())
	go store.Run(ctx, 30*time.Millisecond)

	sw := cluster.NewSWIM(id, swimAddr, fastSWIM(), swimNet.Endpoint(swimAddr), clock.RealClock{},
		rand.New(rand.NewSource(int64(len(id)+1))), quiet())
	sw.Join(ctx, swimSeeds)
	go sw.Run(ctx)

	quorum := clusterSize/2 + 1
	hasQuorum := func() bool {
		if !fencing {
			return true
		}
		return sw.AliveCount() >= quorum
	}

	fake := vmm.NewFakeWithSnapDir(snapDir)
	r := agent.NewReconciler(id, fake, clock.RealClock{}, 50*time.Millisecond, quiet())
	r.SetSource(func() []agent.DesiredVM {
		if !hasQuorum() {
			return nil // self-fence: minority sheds its workloads
		}
		desired := map[string]state.Spec{}
		for _, sp := range store.Desired() {
			desired[sp.Name] = sp
		}
		var out []agent.DesiredVM
		for name, c := range store.Claims() {
			if c.Owner != id {
				continue
			}
			if sp, ok := desired[name]; ok {
				out = append(out, agent.DesiredVM{
					Spec:        vmm.Spec{Name: name, Image: sp.Image},
					SnapshotRef: vmm.SnapshotRef(c.SnapshotRef),
				})
			}
		}
		return out
	})
	go r.Run(ctx)

	sched := market.New(id, capacity, store, sw, hasQuorum, clock.RealClock{}, quiet())
	go sched.Run(ctx, 50*time.Millisecond)

	return &peer{id: id, store: store, swim: sw, fake: fake, cancel: cancel}
}

func fastSWIM() cluster.Config {
	return cluster.Config{
		Period:           40 * time.Millisecond,
		AckTimeout:       12 * time.Millisecond,
		SuspicionTimeout: 200 * time.Millisecond,
		IndirectK:        2,
		GossipFanout:     6,
	}
}

func ownerOf(p *peer, vm string) string {
	c, _ := p.store.Claim(vm)
	return c.Owner
}

// The north-star: place a stateful VM, let it accrue state, snapshot it, kill its
// owner, and assert a survivor re-claims and restores it with the state intact.
func TestNorthStarFailoverPreservesState(t *testing.T) {
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
		peers[id] = startPeer(t, swimNet, stateNet, snapDir, id, seed, 2)
	}
	t.Cleanup(func() {
		for _, p := range peers {
			p.cancel()
		}
	})

	// Everyone sees everyone alive.
	eventually(t, 3*time.Second, func() bool {
		for _, p := range peers {
			alive := 0
			for _, m := range p.swim.Members() {
				if m.State == cluster.Alive {
					alive++
				}
			}
			if alive < 3 {
				return false
			}
		}
		return true
	}, "membership converges")

	// Submit a stateful VM via host-a; wait until some peer owns + runs it.
	peers["host-a"].store.SetDesired(state.Spec{Name: "counter"})
	eventually(t, 3*time.Second, func() bool {
		o := ownerOf(peers["host-a"], "counter")
		return o != "" && peers[o] != nil && peers[o].fake.Counter("counter") >= 0 &&
			running(peers[o].fake, "counter")
	}, "counter claimed and running")

	owner := ownerOf(peers["host-a"], "counter")
	t.Logf("counter owned by %s", owner)

	// Let the workload accrue state, then snapshot it (owner-side), recording the
	// ref in the claim — exactly what murmurd's snapshot loop does.
	for i := 0; i < 5; i++ {
		peers[owner].fake.WorkloadTick()
	}
	want := peers[owner].fake.Counter("counter")
	if want < 1 {
		t.Fatalf("setup: counter should have advanced, got %d", want)
	}
	ref, err := peers[owner].fake.Snapshot(context.Background(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	c, _ := peers[owner].store.Claim("counter")
	c.SnapshotRef = string(ref)
	peers[owner].store.SetClaim("counter", c)

	// The snapshot ref must reach a survivor BEFORE the owner dies — otherwise the
	// re-claim has no snapshot to restore from (this propagation gap is itself the
	// RPO lesson: anything not yet replicated is at risk).
	eventually(t, 3*time.Second, func() bool {
		for id, p := range peers {
			if id == owner {
				continue
			}
			if rc, ok := p.store.Claim("counter"); ok && rc.SnapshotRef == string(ref) {
				return true
			}
		}
		return false
	}, "snapshot ref replicated to a survivor")

	// KILL the owner (physical death): stop its whole stack.
	t.Logf("killing owner %s (snapshotted counter=%d)", owner, want)
	peers[owner].cancel()

	// A survivor must detect the death, re-claim, restore from snapshot, and the
	// counter must come back intact (== snapshot value), non-zero.
	eventually(t, 6*time.Second, func() bool {
		for id, p := range peers {
			if id == owner {
				continue
			}
			if ownerOf(p, "counter") == id && running(p.fake, "counter") && p.fake.Counter("counter") == want {
				t.Logf("survivor %s restored counter=%d", id, p.fake.Counter("counter"))
				return true
			}
		}
		return false
	}, "a survivor re-claims and restores counter with state intact")
}

func running(f *vmm.Fake, name string) bool {
	obs, _ := f.List(context.Background())
	for _, o := range obs {
		if o.Name == name && o.State == vmm.Running {
			return true
		}
	}
	return false
}
