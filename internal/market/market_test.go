package market

import (
	"io"
	"log/slog"
	"testing"

	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/state"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// localStore makes a store with no transport (Set/Claims work without gossip).
func localStore(node string) *state.Store {
	return state.New(node, "", nil, nil, clock.RealClock{}, nil, quiet())
}

// stub membership: a set of dead nodes; everyone else alive.
type deadSet map[string]bool

func (d deadSet) Alive(n string) bool { return !d[n] }

func ownerOf(st *state.Store, vm string) string {
	c, _ := st.Claim(vm)
	return c.Owner
}

// An unclaimed desired VM gets claimed by the scheduler.
func TestClaimsUnowned(t *testing.T) {
	st := localStore("na")
	st.SetDesired(state.Spec{Name: "web1"})
	s := New("na", 5, st, deadSet{}, nil, clock.RealClock{}, quiet())
	s.ScheduleOnce()
	if ownerOf(st, "web1") != "na" {
		t.Fatalf("want web1 owned by na, got %q", ownerOf(st, "web1"))
	}
}

// Capacity is never exceeded.
func TestRespectsCapacity(t *testing.T) {
	st := localStore("na")
	for _, n := range []string{"a", "b", "c"} {
		st.SetDesired(state.Spec{Name: n})
	}
	s := New("na", 2, st, deadSet{}, nil, clock.RealClock{}, quiet())
	s.ScheduleOnce()
	mine := 0
	for _, c := range st.Claims() {
		if c.Owner == "na" {
			mine++
		}
	}
	if mine != 2 {
		t.Fatalf("capacity 2 should yield exactly 2 claims, got %d", mine)
	}
}

// A claim held by a node SWIM reports dead is re-claimed by a survivor, keeping
// the snapshot ref so the new owner can restore.
func TestReclaimsFromDeadOwner(t *testing.T) {
	st := localStore("nb")
	st.SetDesired(state.Spec{Name: "web1"})
	st.SetClaim("web1", state.Claim{Owner: "na", SnapshotRef: "/snap/web1"})

	s := New("nb", 5, st, deadSet{"na": true}, nil, clock.RealClock{}, quiet())
	s.ScheduleOnce()

	c, _ := st.Claim("web1")
	if c.Owner != "nb" {
		t.Fatalf("survivor should re-claim from dead na, got owner %q", c.Owner)
	}
	if c.SnapshotRef != "/snap/web1" {
		t.Fatalf("re-claim must preserve snapshot ref, got %q", c.SnapshotRef)
	}
}

// A live owner's claim is left alone.
func TestLeavesLiveOwnerAlone(t *testing.T) {
	st := localStore("nb")
	st.SetDesired(state.Spec{Name: "web1"})
	st.SetClaim("web1", state.Claim{Owner: "na"})
	s := New("nb", 5, st, deadSet{}, nil, clock.RealClock{}, quiet())
	s.ScheduleOnce()
	if ownerOf(st, "web1") != "na" {
		t.Fatalf("live owner na should keep web1, got %q", ownerOf(st, "web1"))
	}
}

// A claim on an undesired VM is released.
func TestReleasesUndesired(t *testing.T) {
	st := localStore("na")
	st.SetClaim("ghost", state.Claim{Owner: "na"})
	s := New("na", 5, st, deadSet{}, nil, clock.RealClock{}, quiet())
	s.ScheduleOnce()
	if ownerOf(st, "ghost") == "na" {
		t.Fatal("claim on undesired VM should be released")
	}
}
