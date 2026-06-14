package agent

import (
	"context"
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// testClock is a hand-driven clock. After() returns a channel we never fire,
// because these tests call ReconcileOnce directly — they don't depend on time
// passing, only on convergence per pass.
type testClock struct{}

func (testClock) Now() time.Time                       { return time.Unix(0, 0) }
func (testClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func newTestReconciler(t *testing.T) (*Reconciler, *vmm.Fake) {
	t.Helper()
	f := vmm.NewFake()
	r := NewReconciler("test", f, testClock{}, time.Second, nil)
	return r, f
}

// The Stage 0 lesson, as a test: declare a VM, reconcile, it boots.
func TestReconcileBootsDesired(t *testing.T) {
	ctx := context.Background()
	r, f := newTestReconciler(t)

	r.SetDesired(vmm.Spec{Name: "counter"})
	r.ReconcileOnce(ctx)

	if got := observe(t, f, "counter"); got != vmm.Running {
		t.Fatalf("want counter Running after reconcile, got %s", got)
	}
}

// Fault injection: something kills the VM out of band. The next reconcile must
// notice the gap between desired and observed and converge it back. This is the
// demo, expressed as a test.
func TestReconcileRevivesAfterOutOfBandKill(t *testing.T) {
	ctx := context.Background()
	r, f := newTestReconciler(t)

	r.SetDesired(vmm.Spec{Name: "counter"})
	r.ReconcileOnce(ctx)

	// The world reaches in and stops the VM behind murmur's back.
	f.ForceState("counter", vmm.Missing)
	if got := observe(t, f, "counter"); got != vmm.Missing {
		t.Fatalf("setup: want Missing, got %s", got)
	}

	// One convergence pass should bring it back.
	r.ReconcileOnce(ctx)
	if got := observe(t, f, "counter"); got != vmm.Running {
		t.Fatalf("reconciler did not revive VM: want Running, got %s", got)
	}
}

// Reconcile is idempotent: an already-converged VM is left alone, and repeated
// passes don't thrash it.
func TestReconcileIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r, f := newTestReconciler(t)

	r.SetDesired(vmm.Spec{Name: "counter"})
	for i := 0; i < 5; i++ {
		r.ReconcileOnce(ctx)
	}
	if got := observe(t, f, "counter"); got != vmm.Running {
		t.Fatalf("want Running, got %s", got)
	}
}

// Removing desire converges the VM away.
func TestReconcileKillsUndesired(t *testing.T) {
	ctx := context.Background()
	r, f := newTestReconciler(t)

	r.SetDesired(vmm.Spec{Name: "counter"})
	r.ReconcileOnce(ctx)
	r.RemoveDesired("counter")
	r.ReconcileOnce(ctx)

	if got := observe(t, f, "counter"); got == vmm.Running {
		t.Fatalf("want counter not Running after rm, got %s", got)
	}
}

func observe(t *testing.T, f *vmm.Fake, name string) vmm.State {
	t.Helper()
	obs, err := f.List(context.Background())
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
