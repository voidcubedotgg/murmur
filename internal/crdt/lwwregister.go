package crdt

// LWWRegister is a single last-write-wins cell: a value plus the Stamp of the
// write that set it. Merge keeps the value with the winning stamp. It's the
// smallest possible CRDT and the building block the Stage-4 ownership claim will
// use (vm -> owning node, newest stamp wins).
//
// Note what LWW silently does: when two writes are concurrent, one is simply
// dropped — there is no "merge of the values," just a winner. That's fine for an
// owner field; it would be lossy for, say, a set of tags. Choosing the right
// CRDT per field is the real skill.
type LWWRegister[T any] struct {
	Value T     `json:"value"`
	Stamp Stamp `json:"stamp"`
}

// Set overwrites the register if s wins the stamp comparison.
func (r *LWWRegister[T]) Set(v T, s Stamp) {
	if s.After(r.Stamp) {
		r.Value = v
		r.Stamp = s
	}
}

// Merge folds another replica's register in, keeping the winning stamp.
func (r *LWWRegister[T]) Merge(other LWWRegister[T]) {
	if other.Stamp.After(r.Stamp) {
		r.Value = other.Value
		r.Stamp = other.Stamp
	}
}
