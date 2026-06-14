// Package vmm is the ONLY package that knows what substrate runs our VMs.
//
// Everything above this package speaks exactly four verbs — boot, kill,
// snapshot, restore — plus an observe (List). That four-verb interface is
// sacred (see CLAUDE.md): the rest of the swarm must never learn whether
// smolvm, a fake, or anything else is underneath.
package vmm

import "context"

// State is what we can observe about a VM. Note there is no "down" here —
// only what the substrate reports. The distinction between "stopped on
// purpose" and "crashed" lives in the reconciler, not here.
type State string

const (
	// Running: the substrate reports the VM as up.
	Running State = "running"
	// Stopped: the VM exists but is not running.
	Stopped State = "stopped"
	// Missing: the substrate has no record of this VM at all.
	Missing State = "missing"
)

// Spec is the desired shape of a VM. Deliberately tiny for Stage 0; it will
// grow (resources, labels) as later stages need them.
type Spec struct {
	Name  string
	Image string
}

// Observed is a point-in-time reading of one VM from the substrate.
type Observed struct {
	Name  string
	State State
}

// SnapshotRef is an opaque handle to a snapshot. Its meaning is substrate-
// specific (for smolvm it is a path to a .smolmachine artifact); callers
// above internal/vmm must treat it as opaque and never interpret it.
type SnapshotRef string

// VMM is the four-verb interface plus observation.
//
// CRITICAL: every verb must be idempotent. The reconciler calls these
// repeatedly against the same VM on every tick — a single ack can't tell a
// slow substrate from a finished one, so the only safe design is to make
// "do it again" harmless. Boot of a running VM is a no-op; Kill of a missing
// VM is a no-op.
type VMM interface {
	// Boot makes the named VM running. No-op if already running.
	Boot(ctx context.Context, spec Spec) error
	// Kill makes the named VM not-running. No-op if already gone.
	Kill(ctx context.Context, name string) error
	// Snapshot captures the named VM's state and returns a handle. (Stage 4.)
	Snapshot(ctx context.Context, name string) (SnapshotRef, error)
	// Restore recreates the named VM from a snapshot handle. (Stage 4.)
	Restore(ctx context.Context, name string, ref SnapshotRef) error
	// List reports what the substrate currently observes.
	List(ctx context.Context) ([]Observed, error)
}
