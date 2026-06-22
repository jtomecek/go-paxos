package paxos

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeTransport is a minimal Transport for unit-testing the Proposer in
// isolation. It captures every Broadcast on its `broadcasts` channel; the
// test driver feeds synthetic responses back to the proposer via
// HandleResponse rather than via Receive(), so Receive() is not exercised.
type fakeTransport struct {
	broadcasts chan Message

	mu     sync.Mutex
	closed bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{broadcasts: make(chan Message, 1024)}
}

func (t *fakeTransport) Listen(addr string) error                               { return nil }
func (t *fakeTransport) Connect(addr string) error                              { return nil }
func (t *fakeTransport) Send(ctx context.Context, to NodeID, msg Message) error { return nil }

func (t *fakeTransport) Broadcast(ctx context.Context, msg Message) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()
	t.broadcasts <- msg
	return nil
}

func (t *fakeTransport) Receive() <-chan MessageEnvelope { return nil }

func (t *fakeTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		close(t.broadcasts)
		t.closed = true
	}
	return nil
}

// TestProposer_ConcurrentProposals verifies that many in-flight Propose calls
// can complete in parallel. This exercises the per-proposal response channel
// fix: before that, all proposals shared transport.Receive() and would race,
// dropping each other's responses and stalling.
func TestProposer_ConcurrentProposals(t *testing.T) {
	const peerCount = 2 // 3-node cluster: self + 2 peers
	const quorum = 2

	transport := newFakeTransport()
	defer transport.Close()

	proposer := NewProposer(1, transport, quorum, peerCount, nil)
	proposer.SetTimeouts(2*time.Second, 2*time.Second)

	// Driver: respond to every Prepare/Accept with quorum-size positive
	// responses. Mimics what Node.handleMessage would do for live peers.
	driverDone := make(chan struct{})
	go func() {
		defer close(driverDone)
		for sent := range transport.broadcasts {
			switch m := sent.(type) {
			case *Prepare:
				for nid := uint16(2); nid < uint16(2+quorum); nid++ {
					proposer.HandleResponse(&Promise{
						Ballot:   m.Ballot,
						Instance: m.Instance,
						FromNode: NodeID(nid),
					})
				}
			case *Accept:
				for nid := uint16(2); nid < uint16(2+quorum); nid++ {
					proposer.HandleResponse(&Accepted{
						Ballot:   m.Ballot,
						Instance: m.Instance,
						FromNode: NodeID(nid),
					})
				}
			}
		}
	}()

	const N = 20
	var wg sync.WaitGroup
	errs := make([]error, N)
	insts := make([]Instance, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			insts[i], errs[i] = proposer.Propose(ctx, []byte(fmt.Sprintf("v%d", i)))
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent proposals stalled — message routing is likely broken")
	}

	transport.Close()
	<-driverDone

	for i, err := range errs {
		if err != nil {
			t.Errorf("proposal %d failed: %v", i, err)
		}
	}

	seen := make(map[Instance]bool)
	for i, inst := range insts {
		if errs[i] != nil {
			continue
		}
		if seen[inst] {
			t.Errorf("duplicate instance %d (proposal %d)", inst, i)
		}
		seen[inst] = true
	}
	if len(seen) != N {
		t.Errorf("expected %d distinct committed instances, got %d", N, len(seen))
	}
}

// TestProposer_HandleResponseIgnoresStale verifies HandleResponse silently
// drops responses for unknown instances or stale ballots, rather than
// blocking or panicking.
func TestProposer_HandleResponseIgnoresStale(t *testing.T) {
	transport := newFakeTransport()
	defer transport.Close()
	proposer := NewProposer(1, transport, 2, 2, nil)

	// No pending proposals — should be a no-op.
	proposer.HandleResponse(&Promise{Ballot: MakeBallot(5, 1), Instance: 99})
	proposer.HandleResponse(&Accepted{Ballot: MakeBallot(5, 1), Instance: 99})
	proposer.HandleResponse(&Reject{Ballot: MakeBallot(5, 1), Instance: 99, HigherBallot: MakeBallot(6, 1)})
	proposer.HandleResponse(&Nack{Ballot: MakeBallot(5, 1), Instance: 99, HigherBallot: MakeBallot(6, 1)})
}
