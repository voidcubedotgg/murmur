package cluster

import (
	"context"
	"net"
)

// UDPTransport is the real network: SWIM messages are UDP datagrams. UDP is the
// authentic substrate for SWIM — small, connectionless, lossy — so the protocol
// is forced to treat every send as something that might vanish, which is the
// whole point.
type UDPTransport struct {
	conn net.PacketConn
	recv chan Packet
}

// NewUDPTransport binds a UDP socket on addr (host:port) and starts reading.
func NewUDPTransport(addr string) (*UDPTransport, error) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	t := &UDPTransport{conn: conn, recv: make(chan Packet, 256)}
	go t.readLoop()
	return t, nil
}

func (t *UDPTransport) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, addr, err := t.conn.ReadFrom(buf)
		if err != nil {
			// Conn closed (shutdown) or fatal read error: just stop reading. We do
			// NOT close(t.recv) — a closed channel makes consumers' select spin on
			// zero-value packets. Leaving it open lets them block until ctx cancel.
			return
		}
		b := make([]byte, n)
		copy(b, buf[:n])
		// Drop on a full buffer rather than block the reader — losing a gossip
		// packet is survivable; stalling the receive loop is not.
		select {
		case t.recv <- Packet{From: addr.String(), Payload: b}:
		default:
		}
	}
}

func (t *UDPTransport) Send(_ context.Context, addr string, b []byte) error {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	_, err = t.conn.WriteTo(b, ua)
	return err
}

func (t *UDPTransport) Receive() <-chan Packet { return t.recv }
func (t *UDPTransport) LocalAddr() string      { return t.conn.LocalAddr().String() }
func (t *UDPTransport) Close() error           { return t.conn.Close() }
