package control

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// fakeNode is a real reconciler + fake VMM standing in for one agent. Apply
// feeds desired and reconciles immediately so List tells the truth.
type fakeNode struct {
	r *agent.Reconciler
	f *vmm.Fake
}

func newFakeNode() *fakeNode {
	f := vmm.NewFake()
	r := agent.NewReconciler("n", f, stoppedClock{}, time.Second, nil)
	return &fakeNode{r: r, f: f}
}

type stoppedClock struct{}

func (stoppedClock) Now() time.Time                       { return time.Unix(0, 0) }
func (stoppedClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

// fakeClient is the fault-injecting transport — the whole point of Stage 1's
// tests. It lets us reproduce timeouts, lost effects, and duplicate deliveries
// deterministically.
type fakeClient struct {
	nodes map[string]*fakeNode // addr -> node

	failApplies  int // fail (error) this many Apply calls before any succeed
	swallowFirst int // first N Applies return OK but DON'T take effect (lost-effect)

	applyCount int
}

func (c *fakeClient) Apply(ctx context.Context, addr string, req api.ApplyRequest) (api.ApplyResponse, error) {
	c.applyCount++
	if c.applyCount <= c.failApplies {
		// Indistinguishable from a timeout / refused connection to the caller.
		return api.ApplyResponse{}, fmt.Errorf("simulated rpc failure #%d", c.applyCount)
	}
	if c.applyCount <= c.failApplies+c.swallowFirst {
		// The reply says success, but the work never happened (e.g. agent crashed
		// right after acking). This is why the controller must not trust acks.
		return api.ApplyResponse{Applied: true}, nil
	}
	n := c.nodes[addr]
	n.r.SetDesired(req.Spec)
	n.r.ReconcileOnce(ctx)
	return api.ApplyResponse{Applied: true}, nil
}

func (c *fakeClient) Remove(ctx context.Context, addr, name string) error {
	n := c.nodes[addr]
	n.r.RemoveDesired(name)
	n.r.ReconcileOnce(ctx)
	return nil
}

func (c *fakeClient) List(ctx context.Context, addr string) ([]vmm.Observed, error) {
	return c.nodes[addr].f.List(ctx)
}

func newHarness(client *fakeClient, registry map[string]string) *Controller {
	return NewController(registry, client, stoppedClock{}, time.Second, nil)
}

func observed(t *testing.T, n *fakeNode, name string) vmm.State {
	t.Helper()
	obs, err := n.f.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range obs {
		if o.Name == name {
			return o.State
		}
	}
	return vmm.Missing
}

// Placement: a VM named for host-b lands on host-b, not host-a.
func TestPlacementHitsNamedNode(t *testing.T) {
	ctx := context.Background()
	a, b := newFakeNode(), newFakeNode()
	client := &fakeClient{nodes: map[string]*fakeNode{"addr-a": a, "addr-b": b}}
	ctrl := newHarness(client, map[string]string{"host-a": "addr-a", "host-b": "addr-b"})

	if err := ctrl.Place(api.Assignment{Name: "counter", Node: "host-b"}); err != nil {
		t.Fatal(err)
	}
	ctrl.ReconcileOnce(ctx)

	if got := observed(t, b, "counter"); got != vmm.Running {
		t.Fatalf("want counter Running on host-b, got %s", got)
	}
	if got := observed(t, a, "counter"); got != vmm.Missing {
		t.Fatalf("counter should not exist on host-a, got %s", got)
	}
}

// At-least-once: the first two pushes fail; the controller keeps retrying each
// tick and eventually converges.
func TestAtLeastOnceRetryConverges(t *testing.T) {
	ctx := context.Background()
	b := newFakeNode()
	client := &fakeClient{nodes: map[string]*fakeNode{"addr-b": b}, failApplies: 2}
	ctrl := newHarness(client, map[string]string{"host-b": "addr-b"})

	ctrl.Place(api.Assignment{Name: "counter", Node: "host-b"})

	ctrl.ReconcileOnce(ctx) // fail #1
	if observed(t, b, "counter") == vmm.Running {
		t.Fatal("should not be running after first failed push")
	}
	ctrl.ReconcileOnce(ctx) // fail #2
	ctrl.ReconcileOnce(ctx) // success
	if got := observed(t, b, "counter"); got != vmm.Running {
		t.Fatalf("want Running after retries, got %s", got)
	}
}

// Don't trust the ack: Apply reports success but the effect was lost. The next
// reconcile re-observes the gap and re-pushes, converging anyway.
func TestDoesNotTrustAck(t *testing.T) {
	ctx := context.Background()
	b := newFakeNode()
	client := &fakeClient{nodes: map[string]*fakeNode{"addr-b": b}, swallowFirst: 1}
	ctrl := newHarness(client, map[string]string{"host-b": "addr-b"})

	ctrl.Place(api.Assignment{Name: "counter", Node: "host-b"})

	ctrl.ReconcileOnce(ctx) // ack says OK, but nothing actually ran
	if got := observed(t, b, "counter"); got == vmm.Running {
		t.Fatal("setup: lost-effect apply should not have run the VM")
	}
	ctrl.ReconcileOnce(ctx) // observes gap, re-pushes for real
	if got := observed(t, b, "counter"); got != vmm.Running {
		t.Fatalf("want Running after re-push, got %s", got)
	}
}

// Placing on an unknown node is rejected up front.
func TestPlaceUnknownNodeRejected(t *testing.T) {
	client := &fakeClient{nodes: map[string]*fakeNode{}}
	ctrl := newHarness(client, map[string]string{"host-a": "addr-a"})
	if err := ctrl.Place(api.Assignment{Name: "x", Node: "ghost"}); err == nil {
		t.Fatal("expected error placing on unknown node")
	}
}
