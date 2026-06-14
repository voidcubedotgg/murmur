package vmm

import (
	"context"
	"testing"
)

func TestFakeVerbsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	spec := Spec{Name: "counter"}

	// Boot twice -> still exactly one running VM.
	mustBoot(t, f, spec)
	mustBoot(t, f, spec)
	if got := stateOf(t, f, "counter"); got != Running {
		t.Fatalf("after double boot, want Running, got %s", got)
	}

	// Kill twice -> stays stopped, no error.
	if err := f.Kill(ctx, "counter"); err != nil {
		t.Fatal(err)
	}
	if err := f.Kill(ctx, "counter"); err != nil {
		t.Fatal(err)
	}
	if got := stateOf(t, f, "counter"); got != Stopped {
		t.Fatalf("after double kill, want Stopped, got %s", got)
	}

	// Kill of a never-seen VM is a harmless no-op.
	if err := f.Kill(ctx, "ghost"); err != nil {
		t.Fatalf("kill of missing VM should be no-op, got %v", err)
	}
}

func TestFakeSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	mustBoot(t, f, Spec{Name: "counter"})

	ref, err := f.Snapshot(ctx, "counter")
	if err != nil {
		t.Fatal(err)
	}
	f.ForceState("counter", Missing)
	if err := f.Restore(ctx, "counter", ref); err != nil {
		t.Fatal(err)
	}
	if got := stateOf(t, f, "counter"); got != Running {
		t.Fatalf("after restore, want Running, got %s", got)
	}
}

func mustBoot(t *testing.T, v VMM, s Spec) {
	t.Helper()
	if err := v.Boot(context.Background(), s); err != nil {
		t.Fatalf("boot %s: %v", s.Name, err)
	}
}

func stateOf(t *testing.T, v VMM, name string) State {
	t.Helper()
	obs, err := v.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range obs {
		if o.Name == name {
			return o.State
		}
	}
	return Missing
}

// A snapshot taken by one Fake can be Restored by a DIFFERENT Fake instance
// (sharing the snapshot dir) — the cross-peer state transfer the failover demo
// needs, since the survivor never took the snapshot itself.
func TestFakeSnapshotCrossInstance(t *testing.T) {
	dir := t.TempDir()
	owner := NewFakeWithSnapDir(dir)
	mustBoot(t, owner, Spec{Name: "counter"})
	ref, err := owner.Snapshot(context.Background(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	survivor := NewFakeWithSnapDir(dir) // fresh instance, never saw the VM
	if err := survivor.Restore(context.Background(), "counter", ref); err != nil {
		t.Fatalf("survivor restore from shared snapshot: %v", err)
	}
	if stateOf(t, survivor, "counter") != Running {
		t.Fatal("restored VM should be Running on the survivor")
	}
}
