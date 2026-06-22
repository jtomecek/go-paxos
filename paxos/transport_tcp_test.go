package paxos

import (
	"context"
	"net"
	"testing"
	"time"
)

// freePort returns a localhost address with a currently-free port. There's a
// small race between closing the probe listener and the caller binding it, but
// it's good enough for tests.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func (t *TCPTransport) connCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.conns)
}

// TestTCPTransportSingleConnection verifies that when two nodes dial each other
// they converge on exactly one connection per peer (no duplicates), which is
// what prevents double-delivery of messages.
func TestTCPTransportSingleConnection(t *testing.T) {
	addrA := freePort(t)
	addrB := freePort(t)

	a := NewTCPTransport(1, nil)
	b := NewTCPTransport(2, nil)
	defer a.Close()
	defer b.Close()

	if err := a.Listen(addrA); err != nil {
		t.Fatalf("a.Listen: %v", err)
	}
	if err := b.Listen(addrB); err != nil {
		t.Fatalf("b.Listen: %v", err)
	}

	// Both nodes dial each other, which historically produced two connections
	// per pair. Duplicate resolution should collapse this to one on each side.
	_ = a.Connect(addrB)
	_ = b.Connect(addrA)

	if !waitFor(t, 2*time.Second, func() bool {
		return a.connCount() == 1 && b.connCount() == 1
	}) {
		t.Fatalf("expected exactly 1 connection per node, got a=%d b=%d",
			a.connCount(), b.connCount())
	}
}

// TestTCPTransportNoDuplicateDelivery verifies a broadcast from one node is
// delivered to the peer exactly once, tagged with the sender's NodeID.
func TestTCPTransportNoDuplicateDelivery(t *testing.T) {
	addrA := freePort(t)
	addrB := freePort(t)

	a := NewTCPTransport(1, nil)
	b := NewTCPTransport(2, nil)
	defer a.Close()
	defer b.Close()

	if err := a.Listen(addrA); err != nil {
		t.Fatalf("a.Listen: %v", err)
	}
	if err := b.Listen(addrB); err != nil {
		t.Fatalf("b.Listen: %v", err)
	}
	_ = a.Connect(addrB)
	_ = b.Connect(addrA)

	if !waitFor(t, 2*time.Second, func() bool {
		return a.connCount() == 1 && b.connCount() == 1
	}) {
		t.Fatalf("connections did not establish")
	}

	msg := &Prepare{Ballot: MakeBallot(1, 1), Instance: 7}
	if err := a.Broadcast(context.Background(), msg); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// First delivery should arrive promptly, from node 1.
	select {
	case env := <-b.Receive():
		if env.From != 1 {
			t.Fatalf("expected From=1, got %d", env.From)
		}
		if _, ok := env.Message.(*Prepare); !ok {
			t.Fatalf("expected *Prepare, got %T", env.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered")
	}

	// There must be no second (duplicate) delivery of the same broadcast.
	select {
	case env := <-b.Receive():
		t.Fatalf("unexpected duplicate delivery: %+v", env)
	case <-time.After(300 * time.Millisecond):
		// good: no duplicate
	}
}
