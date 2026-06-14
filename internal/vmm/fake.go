package vmm

import (
	"context"
	"fmt"
	"sync"
)

// Fake is an in-memory VMM. It lets the agent and its tests run with zero real
// substrate, and — crucially for the Stage 0 lesson — it exposes ForceState so
// a test (or the demo) can kill a VM "out of band", exactly as if something
// outside murmur reached in and stopped it. The reconciler should then notice
// and converge reality back to desired.
type Fake struct {
	mu    sync.Mutex
	state map[string]State

	// snaps records the last snapshot taken per VM, so Restore is meaningful
	// in later stages without involving any real substrate.
	snaps map[string]State
}

// NewFake returns an empty Fake VMM.
func NewFake() *Fake {
	return &Fake{
		state: make(map[string]State),
		snaps: make(map[string]State),
	}
}

var _ VMM = (*Fake)(nil)

func (f *Fake) Boot(_ context.Context, spec Spec) error {
	if spec.Name == "" {
		return fmt.Errorf("vmm: boot requires a name")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Idempotent: booting a running VM is a no-op.
	f.state[spec.Name] = Running
	return nil
}

func (f *Fake) Kill(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Idempotent: killing a missing VM is fine. We model kill as "stopped"
	// rather than removed, mirroring smolvm machine stop.
	if _, ok := f.state[name]; ok {
		f.state[name] = Stopped
	}
	return nil
}

func (f *Fake) Snapshot(_ context.Context, name string) (SnapshotRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.state[name]
	if !ok {
		return "", fmt.Errorf("vmm: snapshot of missing VM %q", name)
	}
	f.snaps[name] = st
	return SnapshotRef("fake://" + name), nil
}

func (f *Fake) Restore(_ context.Context, name string, ref SnapshotRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.snaps[name]; !ok {
		return fmt.Errorf("vmm: no snapshot for VM %q (ref %s)", name, ref)
	}
	f.state[name] = Running
	return nil
}

func (f *Fake) List(_ context.Context) ([]Observed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Observed, 0, len(f.state))
	for name, st := range f.state {
		out = append(out, Observed{Name: name, State: st})
	}
	return out, nil
}

// ForceState mutates a VM's observed state directly, bypassing the verbs. This
// is the "something killed it behind our back" hook: tests and the demo use it
// to simulate out-of-band failure so the reconciler's convergence is testable.
// A real substrate has no such method — only the Fake.
func (f *Fake) ForceState(name string, st State) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if st == Missing {
		delete(f.state, name)
		return
	}
	f.state[name] = st
}
