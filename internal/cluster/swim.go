package cluster

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/voidcubedotgg/murmur/internal/clock"
)

// Config tunes the detector. The relationships matter more than the values:
// AckTimeout < Period (so an indirect probe fits in the same period), and
// SuspicionTimeout spans several periods (so a brief blip doesn't kill a node).
type Config struct {
	Period           time.Duration // one probe per period
	AckTimeout       time.Duration // wait for a direct ack before going indirect
	SuspicionTimeout time.Duration // how long Suspect persists before Dead
	IndirectK        int           // number of indirect probers (ping-req fanout)
	GossipFanout     int           // updates piggybacked per message
}

// DefaultConfig is a reasonable real-time setting.
func DefaultConfig() Config {
	return Config{
		Period:           1 * time.Second,
		AckTimeout:       300 * time.Millisecond,
		SuspicionTimeout: 4 * time.Second,
		IndirectK:        2,
		GossipFanout:     4,
	}
}

// gossipItem is a membership Update awaiting dissemination, with a remaining
// retransmit budget so each piece of news is repeated ~log(N) times then drops.
type gossipItem struct {
	u    Update
	left int
}

// SWIM is one cluster member running the protocol. It owns a single goroutine
// (the actor) that does all probing and state mutation; external callers only
// read snapshots.
type SWIM struct {
	cfg  Config
	tr   Transport
	clk  clock.Clock
	rnd  *rand.Rand
	log  *slog.Logger
	self string
	ml   *memberList

	seq    uint64
	gossip []gossipItem

	// curProbe is the in-flight probe this period (SWIM probes one node/period).
	curProbe *probe
	directCh <-chan time.Time
	finalCh  <-chan time.Time
	periodCh <-chan time.Time

	// relays maps a target we're indirectly probing on someone's behalf to the
	// address we must forward the ack to.
	relays map[string]string
}

type probe struct {
	seq        uint64
	target     string
	targetAddr string
	indirect   bool
}

// NewSWIM builds a member. selfAddr is this node's gossip address.
func NewSWIM(selfID, selfAddr string, cfg Config, tr Transport, clk clock.Clock, rnd *rand.Rand, log *slog.Logger) *SWIM {
	if log == nil {
		log = slog.Default()
	}
	if rnd == nil {
		rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &SWIM{
		cfg:    cfg,
		tr:     tr,
		clk:    clk,
		rnd:    rnd,
		log:    log.With("member", selfID),
		self:   selfID,
		ml:     newMemberList(selfID, selfAddr, clk.Now),
		relays: make(map[string]string),
	}
}

// Members returns a snapshot of the local view.
func (s *SWIM) Members() []Member { return s.ml.snapshot() }

// Alive reports whether we currently believe id is Alive. Unknown nodes are not
// alive. Callers (e.g. the control plane) use this to stop hammering the dead —
// but must remember it's a belief, and a wrong one costs a needless reschedule.
func (s *SWIM) Alive(id string) bool {
	st, ok := s.ml.state(id)
	return ok && st == Alive
}

// Join bootstraps membership by pinging seed addresses. We may not know the
// seeds' IDs yet; their acks (which piggyback their self-update) teach us.
func (s *SWIM) Join(ctx context.Context, seeds []string) {
	for _, addr := range seeds {
		if addr == "" || addr == s.ml.selfAddr {
			continue
		}
		s.send(ctx, addr, Message{Type: msgPing, About: ""})
	}
}

// Run drives the protocol until ctx is cancelled. Everything happens in this one
// goroutine: timers and inbound packets are serialized through the select, so no
// locks are needed for protocol state (only the member list, which outsiders
// read, is mutex-guarded).
func (s *SWIM) Run(ctx context.Context) {
	s.log.Info("swim started", "period", s.cfg.Period, "addr", s.ml.selfAddr)
	s.periodCh = s.clk.After(s.cfg.Period)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("swim stopped")
			return
		case <-s.periodCh:
			s.onPeriod(ctx)
			s.periodCh = s.clk.After(s.cfg.Period)
		case <-s.directCh:
			s.onDirectTimeout(ctx)
		case <-s.finalCh:
			s.onFinalTimeout()
		case pkt := <-s.tr.Receive():
			s.onPacket(ctx, pkt)
		}
	}
}

// onPeriod ages suspicions into deaths, then starts a fresh probe.
func (s *SWIM) onPeriod(ctx context.Context) {
	for _, id := range s.ml.agingSuspects(s.cfg.SuspicionTimeout) {
		if u, ok := s.ml.setState(id, Dead); ok {
			s.log.Warn("member declared DEAD (suspicion timed out)", "node", id, "incarnation", u.Incarnation)
			s.enqueue(u)
		}
	}

	targets := s.ml.aliveOthers("")
	if len(targets) == 0 {
		s.clearProbe()
		return
	}
	t := targets[s.rnd.Intn(len(targets))]
	s.seq++
	s.curProbe = &probe{seq: s.seq, target: t.ID, targetAddr: t.Addr}
	s.send(ctx, t.Addr, Message{Type: msgPing, Seq: s.seq, About: s.self, Target: t.ID})
	s.directCh = s.clk.After(s.cfg.AckTimeout)
	s.finalCh = nil
}

// onDirectTimeout fires when a direct ping went unanswered. Rather than suspect
// immediately (the target might just be slow, or our direct path lossy), we ask
// k other members to probe it indirectly. This is SWIM's key false-positive
// reducer: it takes several independent failures to condemn a node.
func (s *SWIM) onDirectTimeout(ctx context.Context) {
	if s.curProbe == nil {
		return
	}
	s.curProbe.indirect = true
	helpers := s.ml.aliveOthers(s.curProbe.target)
	s.shuffle(helpers)
	k := s.cfg.IndirectK
	if k > len(helpers) {
		k = len(helpers)
	}
	for i := 0; i < k; i++ {
		s.send(ctx, helpers[i].Addr, Message{
			Type:       msgPingReq,
			Seq:        s.curProbe.seq,
			About:      s.curProbe.target,
			Target:     s.curProbe.target,
			TargetAddr: s.curProbe.targetAddr,
		})
	}
	s.directCh = nil
	// Give the indirect path one more ack window.
	s.finalCh = s.clk.After(s.cfg.AckTimeout)
}

// onFinalTimeout fires when neither the direct nor any indirect probe was
// answered. We move the target to Suspect (not Dead — still only a suspicion)
// and gossip it; the suspicion timer (in onPeriod) will harden it later unless
// the node refutes.
func (s *SWIM) onFinalTimeout() {
	if s.curProbe == nil {
		return
	}
	if u, ok := s.ml.setState(s.curProbe.target, Suspect); ok {
		s.log.Warn("member SUSPECTED (no ack, direct or indirect)", "node", s.curProbe.target)
		s.enqueue(u)
	}
	s.clearProbe()
}

func (s *SWIM) clearProbe() {
	s.curProbe = nil
	s.directCh = nil
	s.finalCh = nil
}

// onPacket merges piggybacked gossip, then handles the message by type.
func (s *SWIM) onPacket(ctx context.Context, pkt Packet) {
	m, err := decode(pkt.Payload)
	if err != nil {
		return
	}
	// Always merge gossip first — news travels on every message.
	for _, u := range m.Updates {
		s.merge(ctx, u)
	}
	// The sender is, by definition, alive right now — we just received its
	// message. Treat that as first-hand evidence: learn unknown senders, and
	// resurrect any sender we wrongly believed Suspect/Dead. This is what lets a
	// rejoining or transiently-partitioned node come back.
	if m.FromID != "" && m.FromID != s.self {
		s.observeAlive(m.FromID, m.FromAddr)
	}

	switch m.Type {
	case msgPing:
		// Answer "are you alive?" with "yes" — About=self so the prober can
		// correlate. Reply to FromAddr (or the packet source for bare joins).
		replyTo := m.FromAddr
		if replyTo == "" {
			replyTo = pkt.From
		}
		s.send(ctx, replyTo, Message{Type: msgAck, Seq: m.Seq, About: s.self})

	case msgAck:
		s.onAck(ctx, m)

	case msgPingReq:
		// Someone asked us to probe Target on their behalf. Remember who to
		// forward the ack to, then ping the target.
		s.relays[m.Target] = m.FromAddr
		s.seq++
		s.send(ctx, m.TargetAddr, Message{Type: msgPing, Seq: s.seq, About: m.Target, Target: m.Target})
	}
}

func (s *SWIM) onAck(ctx context.Context, m Message) {
	// If this ack confirms our current probe target, the target is alive — clear
	// the probe. We correlate by About (target identity), which is what lets a
	// relayed ack clear an indirect probe.
	if s.curProbe != nil && m.About == s.curProbe.target {
		s.clearProbe()
	}
	// If we're an indirect prober for this target, relay the good news back.
	if reqAddr, ok := s.relays[m.About]; ok {
		delete(s.relays, m.About)
		if reqAddr != "" {
			s.send(ctx, reqAddr, Message{Type: msgAck, About: m.About})
		}
	}
}

// observeAlive records first-hand evidence that id is alive. Unknown nodes are
// learned as Alive(0); nodes we wrongly believed Suspect/Dead are resurrected
// with a bumped incarnation and re-gossiped.
func (s *SWIM) observeAlive(id, addr string) {
	st, known := s.ml.state(id)
	if !known {
		if changed, _ := s.ml.apply(Update{Node: id, Addr: addr, State: Alive, Incarnation: 0}); changed {
			s.enqueue(Update{Node: id, Addr: addr, State: Alive, Incarnation: 0})
		}
		return
	}
	if st != Alive {
		if u, ok := s.ml.resurrect(id, addr); ok {
			s.log.Info("resurrecting on first-hand contact", "node", id, "incarnation", u.Incarnation)
			s.enqueue(u)
		}
	}
}

// merge applies an update and, if it changed our view, re-gossips it. If the
// update accused us, we refute by gossiping our bumped self-incarnation.
func (s *SWIM) merge(ctx context.Context, u Update) {
	changed, refute := s.ml.apply(u)
	if refute {
		s.log.Info("refuting suspicion about self", "incarnation", s.ml.selfUpdate().Incarnation)
		s.enqueue(s.ml.selfUpdate())
		return
	}
	if changed {
		s.enqueue(u)
	}
}

// --- gossip dissemination -------------------------------------------------

// enqueue schedules an update for piggybacked dissemination, replacing any
// older entry for the same node and refreshing its retransmit budget.
func (s *SWIM) enqueue(u Update) {
	budget := s.retransmits()
	for i := range s.gossip {
		if s.gossip[i].u.Node == u.Node {
			s.gossip[i] = gossipItem{u: u, left: budget}
			return
		}
	}
	s.gossip = append(s.gossip, gossipItem{u: u, left: budget})
}

// retransmits = ceil(C * log(N+1)); each update is repeated roughly this many
// times so it reaches the whole cluster with high probability, then is dropped.
func (s *SWIM) retransmits() int {
	n := len(s.ml.snapshot())
	r := int(math.Ceil(3 * math.Log(float64(n+1))))
	if r < 1 {
		r = 1
	}
	return r
}

// drainGossip returns up to GossipFanout updates to piggyback, always including
// our own self-update, decrementing budgets and dropping exhausted items.
func (s *SWIM) drainGossip() []Update {
	out := []Update{s.ml.selfUpdate()}
	kept := s.gossip[:0]
	for _, it := range s.gossip {
		if len(out) < s.cfg.GossipFanout && it.left > 0 {
			out = append(out, it.u)
			it.left--
		}
		if it.left > 0 {
			kept = append(kept, it)
		}
	}
	s.gossip = kept
	return out
}

func (s *SWIM) send(ctx context.Context, addr string, m Message) {
	m.FromID = s.self
	m.FromAddr = s.ml.selfAddr
	if m.Updates == nil {
		m.Updates = s.drainGossip()
	}
	_ = s.tr.Send(ctx, addr, encode(m))
}

func (s *SWIM) shuffle(ms []Member) {
	s.rnd.Shuffle(len(ms), func(i, j int) { ms[i], ms[j] = ms[j], ms[i] })
}
