package sim

import (
	"testing"
	"time"
)

// North-star, deterministically: place a stateful VM, let it accrue state, kill
// its owner mid-run; a survivor must restore it (counter intact, non-zero).
func TestSim_NorthStarFailover(t *testing.T) {
	s := New(DefaultConfig(1, t.TempDir()))
	s.SetDesired("host-a", "counter")
	// Let it converge + accrue + snapshot for a while.
	s.Run(2 * time.Second)

	owners := s.RunningOwners("counter")
	if len(owners) != 1 {
		t.Fatalf("expected exactly one owner before kill, got %v", owners)
	}
	owner := owners[0]
	beforeCounter := s.Counter(owner, "counter")
	if beforeCounter < 1 {
		t.Fatalf("counter should have accrued, got %d", beforeCounter)
	}

	// Kill the owner; survivors must take over and restore.
	s.At(0, s.Kill(owner)) // applied on the next step
	s.Run(3 * time.Second)

	after := s.RunningOwners("counter")
	if len(after) != 1 || after[0] == owner {
		t.Fatalf("expected a survivor to run counter, got %v (killed %s)", after, owner)
	}
	if c := s.Counter(after[0], "counter"); c < 1 {
		t.Fatalf("restored counter should be non-zero (state survived), got %d", c)
	}
	t.Logf("failover %s -> %s, counter restored to %d", owner, after[0], s.Counter(after[0], "counter"))
}

// Without fencing, a partition produces split-brain: at some step two live
// owners run the same VM. We assert the disaster actually occurs.
func TestSim_SplitBrainWithoutFencing(t *testing.T) {
	cfg := DefaultConfig(7, t.TempDir())
	cfg.Fencing = false
	s := New(cfg)
	s.SetDesired("host-a", "db")
	s.Run(2 * time.Second) // converge + place

	s.At(0, s.Partition([]string{"host-a"}, []string{"host-b", "host-c"}))

	sawSplit := false
	s.RunWithInvariant(3*time.Second, func(sm *Sim) string {
		if len(sm.RunningOwners("db")) >= 2 {
			sawSplit = true
		}
		return "" // never abort; we WANT to observe the bug
	})
	if !sawSplit {
		t.Fatal("expected split-brain (two live owners) without fencing, never observed")
	}
}

// With quorum fencing, the SAME partition is safe: two live owners must NEVER be
// observed at any step (a true DST invariant, checked every step).
func TestSim_QuorumFencingNeverSplitBrains(t *testing.T) {
	cfg := DefaultConfig(7, t.TempDir())
	cfg.Fencing = true
	s := New(cfg)
	s.SetDesired("host-a", "db")
	s.Run(2 * time.Second)

	s.At(0, s.Partition([]string{"host-a"}, []string{"host-b", "host-c"}))

	// Grace: fencing is eventually-safe, not instant. For a detection window the
	// minority hasn't yet realised it lost quorum, so both may briefly run — the
	// residual hole we documented (true instant fencing needs a substrate token).
	// After detection completes, the invariant must hold for the rest of the run.
	s.Run(600 * time.Millisecond)
	msg := s.RunWithInvariant(3*time.Second, func(sm *Sim) string {
		if o := sm.RunningOwners("db"); len(o) >= 2 {
			return "SPLIT-BRAIN with fencing on (post-detection): db on " + o[0] + " and " + o[1]
		}
		return ""
	})
	if msg != "" {
		t.Fatal(msg)
	}
	// And the minority must have actually fenced itself (db not running on host-a).
	for _, o := range s.RunningOwners("db") {
		if o == "host-a" {
			t.Fatal("minority host-a still running db — not fenced")
		}
	}
}

// The whole point of DST: a seed fully determines the run. Two independent runs
// of the same seed + schedule end in byte-identical observable state.
func TestSim_Replayable(t *testing.T) {
	run := func() string {
		s := New(DefaultConfig(42, t.TempDir()))
		s.SetDesired("host-a", "x")
		s.SetDesired("host-a", "y")
		s.At(800*time.Millisecond, s.Kill("host-b"))
		s.At(1500*time.Millisecond, s.Partition([]string{"host-a"}, []string{"host-c"}))
		s.Run(3 * time.Second)
		return s.World()
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("same seed diverged — not deterministic:\n--- run A ---\n%s\n--- run B ---\n%s", a, b)
	}
	t.Logf("replay identical:\n%s", a)
}

// Run the fencing scenario across many seeds; the no-two-owners invariant must
// hold for every one. A violation reports the exact seed to replay.
func TestSim_ManySeedsFencingSafe(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		cfg := DefaultConfig(seed, t.TempDir())
		s := New(cfg)
		s.SetDesired("host-a", "db")
		s.Run(2 * time.Second)
		s.At(0, s.Partition([]string{"host-a"}, []string{"host-b", "host-c"}))
		s.Run(600 * time.Millisecond) // detection grace (residual window)
		msg := s.RunWithInvariant(2*time.Second, func(sm *Sim) string {
			if len(sm.RunningOwners("db")) >= 2 {
				return "two live owners"
			}
			return ""
		})
		if msg != "" {
			t.Fatalf("seed %d violated fencing invariant: %s", seed, msg)
		}
	}
}
