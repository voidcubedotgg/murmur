package crdt

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// raw is a tiny helper to build a stamped set-entry.
func setE(v string, l uint64, n string) Entry { return Entry{Value: []byte(v), Stamp: Stamp{l, n}} }

func liveView(m *LWWMap) map[string]string {
	out := map[string]string{}
	for k, v := range m.Entries() {
		out[k] = string(v)
	}
	return out
}

// Merge is commutative: a∪b == b∪a regardless of fold order.
func TestMergeCommutative(t *testing.T) {
	a := NewLWWMap()
	a.put("x", setE("a-x", 2, "a"))
	a.put("y", setE("a-y", 1, "a"))
	b := NewLWWMap()
	b.put("x", setE("b-x", 3, "b")) // newer x wins
	b.put("z", setE("b-z", 1, "b"))

	ab := NewLWWMap()
	ab.Merge(a.Raw())
	ab.Merge(b.Raw())
	ba := NewLWWMap()
	ba.Merge(b.Raw())
	ba.Merge(a.Raw())

	if !reflect.DeepEqual(liveView(ab), liveView(ba)) {
		t.Fatalf("not commutative: ab=%v ba=%v", liveView(ab), liveView(ba))
	}
	if liveView(ab)["x"] != "b-x" {
		t.Fatalf("expected newer stamp to win for x, got %q", liveView(ab)["x"])
	}
}

// Merge is idempotent: merging the same state twice changes nothing.
func TestMergeIdempotent(t *testing.T) {
	a := NewLWWMap()
	a.put("x", setE("x1", 1, "a"))
	a.put("y", setE("y1", 2, "b"))
	before := liveView(a)
	a.Merge(a.Raw())
	a.Merge(a.Raw())
	if !reflect.DeepEqual(before, liveView(a)) {
		t.Fatalf("not idempotent: before=%v after=%v", before, liveView(a))
	}
}

// Merge is associative: grouping doesn't matter.
func TestMergeAssociative(t *testing.T) {
	mk := func(k, v string, l uint64, n string) *LWWMap {
		m := NewLWWMap()
		m.put(k, setE(v, l, n))
		return m
	}
	a, b, c := mk("k", "a", 1, "a"), mk("k", "b", 2, "b"), mk("k", "c", 3, "c")

	left := NewLWWMap()
	left.Merge(a.Raw())
	left.Merge(b.Raw())
	left.Merge(c.Raw())

	right := NewLWWMap()
	tmp := NewLWWMap()
	tmp.Merge(b.Raw())
	tmp.Merge(c.Raw())
	right.Merge(a.Raw())
	right.Merge(tmp.Raw())

	if !reflect.DeepEqual(liveView(left), liveView(right)) {
		t.Fatalf("not associative: %v vs %v", liveView(left), liveView(right))
	}
	if liveView(left)["k"] != "c" {
		t.Fatalf("expected highest stamp 'c' to win, got %q", liveView(left)["k"])
	}
}

// Replicas that apply the same set of writes in shuffled, duplicated order all
// converge to the same live view. This is the property the gossip layer relies on.
func TestConvergesUnderReorderAndDuplication(t *testing.T) {
	writes := []struct {
		key, val string
		stamp    Stamp
	}{
		{"a", "a1", Stamp{1, "n1"}},
		{"a", "a2", Stamp{4, "n2"}}, // wins for a
		{"b", "b1", Stamp{2, "n1"}},
		{"c", "c1", Stamp{3, "n3"}},
		{"b", "b0", Stamp{1, "n2"}}, // loses for b
	}
	var golden map[string]string
	for seed := int64(0); seed < 20; seed++ {
		r := rand.New(rand.NewSource(seed))
		m := NewLWWMap()
		// apply each write 1-3 times in random order
		order := r.Perm(len(writes))
		for _, i := range order {
			reps := 1 + r.Intn(3)
			for j := 0; j < reps; j++ {
				m.Set(writes[i].key, []byte(writes[i].val), writes[i].stamp)
			}
		}
		view := liveView(m)
		if golden == nil {
			golden = view
			continue
		}
		if !reflect.DeepEqual(golden, view) {
			t.Fatalf("seed %d diverged: %v != %v", seed, view, golden)
		}
	}
	if golden["a"] != "a2" || golden["b"] != "b1" {
		t.Fatalf("unexpected converged view: %v", golden)
	}
}

// A concurrent Set vs Delete resolves by stamp, deterministically, and a stale
// Set arriving after a newer Delete must NOT resurrect the key.
func TestSetDeleteResolveByStamp(t *testing.T) {
	m := NewLWWMap()
	m.Set("x", []byte("v"), Stamp{1, "a"})
	m.Delete("x", Stamp{2, "a"}) // newer delete wins
	if _, ok := m.Get("x"); ok {
		t.Fatal("delete with higher stamp should remove x")
	}
	// A late, older Set must not bring x back.
	m.Set("x", []byte("late"), Stamp{1, "b"})
	if _, ok := m.Get("x"); ok {
		t.Fatal("stale set must not resurrect a tombstoned key")
	}
	// But a Set newer than the tombstone does revive it.
	m.Set("x", []byte("revived"), Stamp{3, "a"})
	if v, ok := m.Get("x"); !ok || string(v) != "revived" {
		t.Fatalf("newer set should revive key, got %q ok=%v", v, ok)
	}
}

func TestLamportWitnessOrders(t *testing.T) {
	c := NewLamport("a")
	s1 := c.Tick() // 1
	c.Witness(Stamp{Lamport: 10, Node: "b"})
	s2 := c.Tick() // must be > 10
	if !s2.After(s1) || s2.Lamport <= 10 {
		t.Fatalf("witness should push clock past observed: s2=%v", s2)
	}
}

func ExampleLWWRegister() {
	var r LWWRegister[string]
	r.Set("host-a", Stamp{1, "a"})
	r.Set("host-b", Stamp{2, "b"}) // newer wins
	r.Set("host-c", Stamp{1, "c"}) // older loses
	fmt.Println(r.Value)
	// Output: host-b
}
