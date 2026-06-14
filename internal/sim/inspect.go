package sim

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// RunningOwners returns the ids of live nodes whose VMM actually reports vm
// Running. More than one at a time = split-brain (two live copies).
func (s *Sim) RunningOwners(vm string) []string {
	var out []string
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.dead {
			continue
		}
		obs, _ := n.fake.List(context.Background())
		for _, o := range obs {
			if o.Name == vm && o.State == vmm.Running {
				out = append(out, id)
			}
		}
	}
	sort.Strings(out)
	return out
}

// Counter reads the workload state for vm on node id.
func (s *Sim) Counter(id, vm string) int { return s.nodes[id].fake.Counter(vm) }

// Alive reports whether node id is still being stepped (not killed).
func (s *Sim) Alive(id string) bool { return !s.nodes[id].dead }

// IDs returns the node ids.
func (s *Sim) IDs() []string { return append([]string(nil), s.ids...) }

// World returns a deterministic, order-independent snapshot of observable state
// across all live nodes — used as the equality check for the replay test.
func (s *Sim) World() string {
	var b strings.Builder
	for _, id := range s.ids { // s.ids is in fixed order
		n := s.nodes[id]
		if n.dead {
			fmt.Fprintf(&b, "%s:DEAD\n", id)
			continue
		}
		obs, _ := n.fake.List(context.Background())
		states := map[string]string{}
		for _, o := range obs {
			states[o.Name] = fmt.Sprintf("%s/%d", o.State, n.fake.Counter(o.Name))
		}
		names := make([]string, 0, len(states))
		for k := range states {
			names = append(names, k)
		}
		sort.Strings(names)
		fmt.Fprintf(&b, "%s:", id)
		for _, k := range names {
			fmt.Fprintf(&b, " %s=%s", k, states[k])
		}
		b.WriteByte('\n')
	}
	return b.String()
}
