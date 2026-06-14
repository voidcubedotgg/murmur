package vmm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Fake is an in-memory VMM. It lets the agent and its tests run with zero real
// substrate, and — crucially for the Stage 0 lesson — it exposes ForceState so
// a test (or the demo) can kill a VM "out of band", exactly as if something
// outside murmur reached in and stopped it. The reconciler should then notice
// and converge reality back to desired.
type Fake struct {
	mu    sync.Mutex
	state map[string]State

	// counters is the workload's accrued state per VM (Stage 5's "in-memory
	// counter"). A running VM accumulates it; Snapshot persists it and Restore
	// brings it back, which is how we prove state survives a failover.
	counters map[string]int

	// snapDir is a SHARED directory where snapshots are written as files, so a
	// Fake in a *different process* (a survivor peer) can Restore from a snapshot
	// taken here. Real state would live in the smolvm artifact; this file is the
	// fake's stand-in to make cross-peer state transfer real in the demo.
	snapDir string
}

// NewFake returns a Fake VMM using the default shared snapshot dir.
func NewFake() *Fake { return NewFakeWithSnapDir(filepath.Join(os.TempDir(), "murmur-snap")) }

// NewFakeWithSnapDir lets callers (and tests) pick the shared snapshot dir.
func NewFakeWithSnapDir(dir string) *Fake {
	return &Fake{state: make(map[string]State), counters: make(map[string]int), snapDir: dir}
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
	if _, ok := f.state[name]; !ok {
		return "", fmt.Errorf("vmm: snapshot of missing VM %q", name)
	}
	if err := os.MkdirAll(f.snapDir, 0o755); err != nil {
		return "", fmt.Errorf("vmm: snapshot dir: %w", err)
	}
	path := filepath.Join(f.snapDir, name+".snap")
	// The file captures the workload state (counter). A survivor restoring it
	// resumes that value — proof the snapshot carried real state.
	content := fmt.Sprintf("%s %d", name, f.counters[name])
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("vmm: write snapshot: %w", err)
	}
	return SnapshotRef(path), nil
}

func (f *Fake) Restore(_ context.Context, name string, ref SnapshotRef) error {
	// Read the shared snapshot file — works even though this Fake never took the
	// snapshot itself (a different peer/process did). That's the cross-peer state
	// transfer the failover demo needs.
	b, err := os.ReadFile(string(ref))
	if err != nil {
		return fmt.Errorf("vmm: restore %q from %s: %w", name, ref, err)
	}
	var snapName string
	var counter int
	// Tolerate a malformed file by restoring as a fresh VM (counter 0).
	_, _ = fmt.Sscanf(string(b), "%s %d", &snapName, &counter)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state[name] = Running
	f.counters[name] = counter
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

// Counter reads a VM's accrued workload state.
func (f *Fake) Counter(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counters[name]
}

// WorkloadTick advances the state of every Running VM by one. Tests call this
// deterministically; the live demo drives it from StartWorkload. It models a
// real workload accumulating state over time.
func (f *Fake) WorkloadTick() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, st := range f.state {
		if st == Running {
			f.counters[name]++
		}
	}
}

// StartWorkload runs WorkloadTick on a timer until ctx is cancelled — the live
// demo's stand-in for a workload accruing state inside the VM.
func (f *Fake) StartWorkload(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				f.WorkloadTick()
			}
		}
	}()
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
