package cluster

import "encoding/json"

// msgType enumerates the three SWIM message kinds.
type msgType string

const (
	msgPing    msgType = "ping"     // "are you alive?" (direct probe)
	msgAck     msgType = "ack"      // "yes, alive" (carries About = confirmed node)
	msgPingReq msgType = "ping-req" // "probe Target for me" (indirect probe)
)

// Update is one piece of membership gossip. Updates ride piggybacked on every
// message (that's SWIM's dissemination: no separate gossip traffic), so news of
// a death or a refutation spreads as a side effect of normal probing.
type Update struct {
	Node        string `json:"node"`
	Addr        string `json:"addr"`
	State       State  `json:"state"`
	Incarnation uint64 `json:"inc"`
}

// Message is the wire format. JSON for legibility while learning; a production
// SWIM would use a compact binary encoding to fit more in each datagram.
type Message struct {
	Type     msgType `json:"type"`
	FromID   string  `json:"from_id"`
	FromAddr string  `json:"from_addr"`
	Seq      uint64  `json:"seq"`

	// About is the node a ping/ack concerns — the probe target. The prober
	// correlates replies by About (target identity), not by Seq, so an ack
	// relayed through an indirect prober still clears the right probe.
	About string `json:"about,omitempty"`

	// Target/TargetAddr are set on ping-req: "go probe this node for me".
	Target     string `json:"target,omitempty"`
	TargetAddr string `json:"target_addr,omitempty"`

	Updates []Update `json:"updates,omitempty"`
}

func encode(m Message) []byte {
	b, _ := json.Marshal(m)
	return b
}

func decode(b []byte) (Message, error) {
	var m Message
	err := json.Unmarshal(b, &m)
	return m, err
}
