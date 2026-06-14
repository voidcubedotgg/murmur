package crdt

// LWWMap is a last-write-wins map with tombstones (an LWW-element-set keyed by
// string). Each key carries a value, the Stamp of the write that set it, and a
// deleted flag — so deletions converge too: a delete is just a write of a
// tombstone, and the higher Stamp wins whether it's a set or a delete.
//
// Values are opaque bytes. The CRDT doesn't care what they mean — internal/state
// stores JSON-encoded assignments in them. Keeping the value opaque is what lets
// this type stay a pure CRDT with no knowledge of the domain.
type LWWMap struct {
	entries map[string]Entry
}

// Entry is one key's current cell. Exported so it can be (de)serialised for
// gossip; Deleted entries (tombstones) are retained because we need their Stamp
// to correctly reject a stale resurrection of the key.
type Entry struct {
	Value   []byte `json:"v,omitempty"`
	Stamp   Stamp  `json:"s"`
	Deleted bool   `json:"d,omitempty"`
}

// NewLWWMap returns an empty map.
func NewLWWMap() *LWWMap { return &LWWMap{entries: make(map[string]Entry)} }

// Set writes value at key with the given stamp, if it's newer than what's there.
// The stamp must come from the writer's Lamport clock (Tick).
func (m *LWWMap) Set(key string, value []byte, s Stamp) {
	m.put(key, Entry{Value: value, Stamp: s})
}

// Delete tombstones key with the given stamp, if newer. Kept as a tombstone (not
// removed) so a later-arriving but older Set can't bring the key back to life.
func (m *LWWMap) Delete(key string, s Stamp) {
	m.put(key, Entry{Stamp: s, Deleted: true})
}

func (m *LWWMap) put(key string, e Entry) {
	cur, ok := m.entries[key]
	// Only accept the write if it strictly wins the stamp comparison. Equal
	// stamps can't happen for distinct writes (Node tiebreaker), and re-applying
	// an identical entry is a no-op — that's the idempotence law in action.
	if !ok || e.Stamp.After(cur.Stamp) {
		m.entries[key] = e
	}
}

// Get returns the live value at key (nil, false if absent or tombstoned).
func (m *LWWMap) Get(key string) ([]byte, bool) {
	e, ok := m.entries[key]
	if !ok || e.Deleted {
		return nil, false
	}
	return e.Value, true
}

// Entries returns all live (non-tombstoned) key/value pairs.
func (m *LWWMap) Entries() map[string][]byte {
	out := make(map[string][]byte, len(m.entries))
	for k, e := range m.entries {
		if !e.Deleted {
			out[k] = e.Value
		}
	}
	return out
}

// Raw returns every cell including tombstones — for serialising the full state
// to a peer during anti-entropy gossip.
func (m *LWWMap) Raw() map[string]Entry {
	out := make(map[string]Entry, len(m.entries))
	for k, e := range m.entries {
		out[k] = e
	}
	return out
}

// Merge folds another replica's state into this one, key by key, keeping the
// winning Stamp per key. Because put() only accepts strictly-newer stamps, Merge
// is commutative, associative, and idempotent — the three laws that make gossip
// order and duplication irrelevant.
func (m *LWWMap) Merge(other map[string]Entry) {
	for k, e := range other {
		m.put(k, e)
	}
}
