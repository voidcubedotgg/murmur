package vmm

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

// Smolvm drives real VMs by shelling out to the smolvm binary. smolvm is a
// black box (CLAUDE.md prime directive 1): we NEVER reach into its internals,
// we only speak its CLI. This adapter is the one place allowed to know that the
// substrate is smolvm at all.
//
// Verb mapping:
//
//	boot     -> machine create (if missing) + machine start
//	kill     -> machine stop
//	snapshot -> pack create --from-vm
//	restore  -> machine create --from <artifact> + start
//	observe  -> machine ls --json
type Smolvm struct {
	// Bin is the smolvm executable; defaults to "smolvm" on PATH.
	Bin string
}

// NewSmolvm returns a Smolvm adapter using the given binary path. Empty bin
// falls back to "smolvm" on PATH.
func NewSmolvm(bin string) *Smolvm {
	if bin == "" {
		bin = "smolvm"
	}
	return &Smolvm{Bin: bin}
}

var _ VMM = (*Smolvm)(nil)

// run executes a smolvm subcommand, wrapping failures with the command and its
// combined output so a reconcile decision is debuggable from the logs.
func (s *Smolvm) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, s.Bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("smolvm %v: %w: %s", args, err, out)
	}
	return out, nil
}

// observe returns the observed state of one VM (Missing if smolvm has no record).
func (s *Smolvm) observe(ctx context.Context, name string) (State, error) {
	all, err := s.List(ctx)
	if err != nil {
		return Missing, err
	}
	for _, o := range all {
		if o.Name == name {
			return o.State, nil
		}
	}
	return Missing, nil
}

func (s *Smolvm) Boot(ctx context.Context, spec Spec) error {
	if spec.Name == "" {
		return fmt.Errorf("vmm: boot requires a name")
	}
	st, err := s.observe(ctx, spec.Name)
	if err != nil {
		return err
	}
	// Idempotent: if smolvm has never heard of this VM, create it first.
	if st == Missing {
		args := []string{"machine", "create", "--name", spec.Name, "--net"}
		if spec.Image != "" {
			args = append(args, "--image", spec.Image)
		}
		if _, err := s.run(ctx, args...); err != nil {
			return err
		}
		st = Stopped
	}
	// Idempotent: only start if not already running.
	if st != Running {
		if _, err := s.run(ctx, "machine", "start", "--name", spec.Name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Smolvm) Kill(ctx context.Context, name string) error {
	st, err := s.observe(ctx, name)
	if err != nil {
		return err
	}
	// Idempotent: nothing to stop if it's not running.
	if st != Running {
		return nil
	}
	_, err = s.run(ctx, "machine", "stop", "--name", name)
	return err
}

func (s *Smolvm) Snapshot(ctx context.Context, name string) (SnapshotRef, error) {
	// Stage 4 exercises this. pack create --from-vm captures a VM snapshot into
	// a portable .smolmachine artifact, which Restore consumes.
	path := name + ".smolmachine"
	if _, err := s.run(ctx, "pack", "create", "--from-vm", name, "-o", path); err != nil {
		return "", err
	}
	return SnapshotRef(path), nil
}

func (s *Smolvm) Restore(ctx context.Context, name string, ref SnapshotRef) error {
	// Stage 4. Recreate the named VM from the artifact, then start it.
	if _, err := s.run(ctx, "machine", "create", "--name", name, "--from", string(ref)); err != nil {
		return err
	}
	_, err := s.run(ctx, "machine", "start", "--name", name)
	return err
}

func (s *Smolvm) List(ctx context.Context) ([]Observed, error) {
	out, err := s.run(ctx, "machine", "ls", "--json")
	if err != nil {
		return nil, err
	}
	var raw []struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("smolvm: parse machine ls: %w", err)
	}
	obs := make([]Observed, 0, len(raw))
	for _, r := range raw {
		obs = append(obs, Observed{Name: r.Name, State: mapState(r.State)})
	}
	return obs, nil
}

// mapState translates smolvm's state strings into our State. Anything that
// isn't clearly running but does exist is treated as Stopped — from the
// reconciler's view, "exists but not running" all needs the same action.
func mapState(s string) State {
	switch s {
	case "running":
		return Running
	default:
		return Stopped
	}
}
