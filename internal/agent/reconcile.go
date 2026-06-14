package agent

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// Reconciler is the heart of Stage 0's lesson: an orchestrator is a loop that
// *converges* observed state toward desired state, repeatedly and idempotently
// — not a script that runs once. Each tick it reads reality, compares it to
// what we want, and takes the smallest action to close the gap. If something
// kills a VM behind our back, the next tick notices and brings it back.
//
// Concurrency shape (CLAUDE.md): the desired map is guarded by a mutex because
// it's written by the API server goroutine and read by the loop. The loop
// itself is a single goroutine that owns all VMM interaction.
type Reconciler struct {
	node     string
	vmm      vmm.VMM
	clock    Clock
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	desired map[string]DesiredVM
	// source, if set, is the authoritative desired set each reconcile pass — used
	// in the CRDT path where desired state lives in the replicated store, not in
	// this struct. When nil, the locally-set desired map is used (tests).
	source func() []DesiredVM
}

// DesiredVM is one VM this node should run, plus the snapshot to restore from if
// it's being recovered (re-claimed) rather than started fresh.
type DesiredVM struct {
	Spec        vmm.Spec
	SnapshotRef vmm.SnapshotRef
}

// NewReconciler builds a reconciler. node labels every log line so that, once
// there are many nodes, the logs are still legible.
func NewReconciler(node string, v vmm.VMM, clock Clock, interval time.Duration, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		node:     node,
		vmm:      v,
		clock:    clock,
		interval: interval,
		log:      log.With("node", node),
		desired:  make(map[string]DesiredVM),
	}
}

// SetSource installs the authoritative desired-set provider (the replicated
// CRDT store, filtered to this node's claims). When set, it overrides the local
// desired map on every reconcile pass.
func (r *Reconciler) SetSource(src func() []DesiredVM) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.source = src
}

// SetDesired declares that we want this VM running. Idempotent. (Tests use this;
// the CRDT path uses SetSource instead.)
func (r *Reconciler) SetDesired(spec vmm.Spec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.desired[spec.Name] = DesiredVM{Spec: spec}
	r.log.Info("desired set", "vm", spec.Name, "image", spec.Image)
}

// RemoveDesired declares we no longer want this VM. The next reconcile will
// kill it if it's still running.
func (r *Reconciler) RemoveDesired(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.desired, name)
	r.log.Info("desired removed", "vm", name)
}

// snapshotDesired returns a copy of the desired set so the loop never holds the
// lock while talking to the (possibly slow) substrate.
func (r *Reconciler) snapshotDesired() map[string]DesiredVM {
	r.mu.Lock()
	src := r.source
	r.mu.Unlock()
	if src != nil {
		out := make(map[string]DesiredVM)
		for _, d := range src() {
			out[d.Spec.Name] = d
		}
		return out
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]DesiredVM, len(r.desired))
	for k, v := range r.desired {
		out[k] = v
	}
	return out
}

// Run drives the reconcile loop until ctx is cancelled. On shutdown the loop
// simply stops — it does not tear down VMs, because "murmurd exited" must not
// mean "kill the workloads"; that decision belongs to desired state, not to
// process lifetime.
func (r *Reconciler) Run(ctx context.Context) {
	r.log.Info("reconciler started", "interval", r.interval)
	for {
		// Reconcile immediately, then wait a tick. Reconciling first means a
		// freshly-set desired state converges without waiting a full interval.
		r.reconcileOnce(ctx)
		select {
		case <-ctx.Done():
			r.log.Info("reconciler stopped")
			return
		case <-r.clock.After(r.interval):
		}
	}
}

// reconcileOnce performs exactly one convergence pass. Exported-ish for tests
// via ReconcileOnce below; kept lowercase to signal it's loop-internal.
func (r *Reconciler) reconcileOnce(ctx context.Context) {
	desired := r.snapshotDesired()

	observed, err := r.vmm.List(ctx)
	if err != nil {
		// We can't see reality this tick. Do nothing rather than guess — acting
		// on a stale/empty view could needlessly churn VMs. Try again next tick.
		r.log.Error("list failed; skipping reconcile pass", "err", err)
		return
	}
	obsState := make(map[string]vmm.State, len(observed))
	for _, o := range observed {
		obsState[o.Name] = o.State
	}

	// Converge desired VMs toward Running. If a desired VM carries a SnapshotRef
	// (it was re-claimed from a dead owner), Restore it so it resumes with state
	// rather than starting fresh; fall back to Boot if the snapshot is
	// unreachable.
	for name, d := range desired {
		if obsState[name] == vmm.Running {
			continue // already where we want it
		}
		if d.SnapshotRef != "" {
			r.log.Info("reconcile: restoring from snapshot", "vm", name, "ref", string(d.SnapshotRef))
			if err := r.vmm.Restore(ctx, name, d.SnapshotRef); err == nil {
				continue
			} else {
				r.log.Warn("restore failed; booting fresh", "vm", name, "err", err)
			}
		}
		r.log.Info("reconcile: booting", "vm", name, "observed", string(obsStateOr(obsState, name)))
		if err := r.vmm.Boot(ctx, d.Spec); err != nil {
			r.log.Error("boot failed", "vm", name, "err", err)
		}
	}

	// Converge away VMs that exist/run but are no longer desired.
	for name, st := range obsState {
		if _, want := desired[name]; want {
			continue
		}
		if st != vmm.Running {
			continue // nothing to do
		}
		r.log.Info("reconcile: killing undesired", "vm", name)
		if err := r.vmm.Kill(ctx, name); err != nil {
			r.log.Error("kill failed", "vm", name, "err", err)
		}
	}
}

func obsStateOr(m map[string]vmm.State, name string) vmm.State {
	if s, ok := m[name]; ok {
		return s
	}
	return vmm.Missing
}

// ReconcileOnce runs a single pass synchronously. Tests use it to drive the
// loop deterministically without real time.
func (r *Reconciler) ReconcileOnce(ctx context.Context) { r.reconcileOnce(ctx) }
