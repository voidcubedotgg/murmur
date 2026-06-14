package cluster

import (
	"sync"
	"time"
)

// State is a member's liveness as believed by the local node. It is always a
// belief, never ground truth.
type State string

const (
	Alive   State = "alive"
	Suspect State = "suspect"
	Dead    State = "dead"
)

// rank orders states for conflict resolution at equal incarnation: a worse
// belief wins, because bad news ("might be dead") must not be silently
// overwritten by a stale "alive". Only a higher incarnation — which only the
// node itself can mint about itself — can clear suspicion.
func rank(s State) int {
	switch s {
	case Alive:
		return 0
	case Suspect:
		return 1
	case Dead:
		return 2
	default:
		return -1
	}
}

// Member is one node as seen locally.
type Member struct {
	ID          string    `json:"id"`
	Addr        string    `json:"addr"`
	State       State     `json:"state"`
	Incarnation uint64    `json:"incarnation"`
	StateChange time.Time `json:"state_change"`
}

// memberList holds everything the local node believes about the cluster,
// including itself. It is guarded by a mutex because the SWIM goroutine writes
// it while control/HTTP goroutines read snapshots.
type memberList struct {
	mu          sync.Mutex
	selfID      string
	selfAddr    string
	incarnation uint64 // our own incarnation; bumped to refute suspicion
	members     map[string]*Member
	now         func() time.Time
}

func newMemberList(selfID, selfAddr string, now func() time.Time) *memberList {
	ml := &memberList{
		selfID:   selfID,
		selfAddr: selfAddr,
		members:  make(map[string]*Member),
		now:      now,
	}
	ml.members[selfID] = &Member{ID: selfID, Addr: selfAddr, State: Alive, Incarnation: 0, StateChange: now()}
	return ml
}

// selfUpdate returns our own current alive-assertion, always piggybacked so
// peers track our latest incarnation.
func (ml *memberList) selfUpdate() Update {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	return Update{Node: ml.selfID, Addr: ml.selfAddr, State: Alive, Incarnation: ml.incarnation}
}

// apply merges a gossiped Update. It returns:
//   - changed: our view of some member changed (so re-gossip it);
//   - refute:  the update accuses *us* of being Suspect/Dead and we must fight
//     back by bumping our incarnation and asserting Alive.
//
// This merge function is the correctness core of SWIM. Get it wrong and you get
// flapping, lost deaths, or zombies that never die.
func (ml *memberList) apply(u Update) (changed, refute bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	// Gossip about ourselves: the only node allowed to declare us Alive is us.
	if u.Node == ml.selfID {
		if u.State != Alive && u.Incarnation >= ml.incarnation {
			// Someone thinks we're suspect/dead with an incarnation at least as
			// fresh as ours. Out-incarnate them: a strictly higher incarnation
			// is the only thing that beats their claim everywhere it has spread.
			ml.incarnation = u.Incarnation + 1
			self := ml.members[ml.selfID]
			self.State = Alive
			self.Incarnation = ml.incarnation
			self.StateChange = ml.now()
			return true, true
		}
		return false, false
	}

	cur, ok := ml.members[u.Node]
	if !ok {
		ml.members[u.Node] = &Member{
			ID: u.Node, Addr: u.Addr, State: u.State,
			Incarnation: u.Incarnation, StateChange: ml.now(),
		}
		return true, false
	}

	supersedes := u.Incarnation > cur.Incarnation ||
		(u.Incarnation == cur.Incarnation && rank(u.State) > rank(cur.State))
	if !supersedes {
		return false, false
	}
	cur.State = u.State
	cur.Incarnation = u.Incarnation
	if u.Addr != "" {
		cur.Addr = u.Addr
	}
	cur.StateChange = ml.now()
	return true, false
}

// setState forces a local belief about another member (used when our own probe
// fails: we mark the target Suspect, or age Suspect→Dead). Returns the Update to
// gossip, and whether anything changed.
func (ml *memberList) setState(id string, st State) (Update, bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	m, ok := ml.members[id]
	if !ok || m.State == st {
		return Update{}, false
	}
	// Suspicion/death is asserted at the member's *current* incarnation; if the
	// member later refutes with a higher incarnation, that refutation wins.
	m.State = st
	m.StateChange = ml.now()
	return Update{Node: m.ID, Addr: m.Addr, State: st, Incarnation: m.Incarnation}, true
}

// resurrect forces a member back to Alive on first-hand evidence (we just
// received a live message from it), minting a higher incarnation so the
// resurrection out-ranks the stale Suspect/Dead belief everywhere it spread.
//
// Tradeoff worth noting: strictly, only a node should bump its own incarnation.
// Letting a peer do it on first-hand contact is a pragmatic "I literally just
// talked to you, the gossip was wrong/stale" rule. It's what makes a rejoining
// or transiently-partitioned node recoverable; the cost is that an over-eager
// version could cause flapping. We only resurrect on direct, in-protocol
// messages, which keeps it honest.
func (ml *memberList) resurrect(id, addr string) (Update, bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	m, ok := ml.members[id]
	if !ok {
		m = &Member{ID: id, Addr: addr}
		ml.members[id] = m
	}
	if m.State == Alive {
		return Update{}, false
	}
	m.State = Alive
	m.Incarnation++
	if addr != "" {
		m.Addr = addr
	}
	m.StateChange = ml.now()
	return Update{Node: id, Addr: m.Addr, State: Alive, Incarnation: m.Incarnation}, true
}

// agingSuspects returns suspects older than timeout that should become Dead.
func (ml *memberList) agingSuspects(timeout time.Duration) []string {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	var out []string
	for id, m := range ml.members {
		if m.State == Suspect && ml.now().Sub(m.StateChange) >= timeout {
			out = append(out, id)
		}
	}
	return out
}

// snapshot returns a stable copy for external readers.
func (ml *memberList) snapshot() []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	out := make([]Member, 0, len(ml.members))
	for _, m := range ml.members {
		out = append(out, *m)
	}
	return out
}

func (ml *memberList) state(id string) (State, bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	m, ok := ml.members[id]
	if !ok {
		return "", false
	}
	return m.State, true
}

// aliveOthers returns IDs+addrs of members we currently believe Alive, excluding
// ourselves and any excluded id.
func (ml *memberList) aliveOthers(exclude string) []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	var out []Member
	for id, m := range ml.members {
		if id == ml.selfID || id == exclude || m.State != Alive {
			continue
		}
		out = append(out, *m)
	}
	return out
}
