package paxos

import (
	"errors"
	"testing"
)

// failingStorage wraps MemoryStorage and fails SaveAcceptorState when
// `fail` is true. Used to assert acceptor state rolls back on persist error.
type failingStorage struct {
	*MemoryStorage
	fail bool
}

func (s *failingStorage) SaveAcceptorState(state AcceptorState) error {
	if s.fail {
		return errors.New("simulated persist failure")
	}
	return s.MemoryStorage.SaveAcceptorState(state)
}

func TestAcceptor_HandlePrepare(t *testing.T) {
	acceptor, err := NewAcceptor(1, nil, nil)
	if err != nil {
		t.Fatalf("NewAcceptor failed: %v", err)
	}

	// First prepare should succeed
	ballot := MakeBallot(1, 1)
	prepare := &Prepare{Ballot: ballot, Instance: 1}

	response := acceptor.HandlePrepare(prepare)

	promise, ok := response.(*Promise)
	if !ok {
		t.Fatalf("Expected Promise, got %T", response)
	}

	if promise.Ballot != ballot {
		t.Errorf("Expected ballot %v, got %v", ballot, promise.Ballot)
	}
	if promise.AcceptedBallot != 0 {
		t.Errorf("Expected no accepted ballot, got %v", promise.AcceptedBallot)
	}

	// Lower ballot should be rejected
	lowerBallot := MakeBallot(0, 2)
	prepare2 := &Prepare{Ballot: lowerBallot, Instance: 1}

	response2 := acceptor.HandlePrepare(prepare2)

	reject, ok := response2.(*Reject)
	if !ok {
		t.Fatalf("Expected Reject, got %T", response2)
	}

	if reject.HigherBallot != ballot {
		t.Errorf("Expected higher ballot %v, got %v", ballot, reject.HigherBallot)
	}
}

func TestAcceptor_HandleAccept(t *testing.T) {
	acceptor, err := NewAcceptor(1, nil, nil)
	if err != nil {
		t.Fatalf("NewAcceptor failed: %v", err)
	}

	// First, do a prepare
	ballot := MakeBallot(1, 1)
	prepare := &Prepare{Ballot: ballot, Instance: 1}
	acceptor.HandlePrepare(prepare)

	// Then accept
	value := []byte("hello")
	accept := &Accept{Ballot: ballot, Instance: 1, Value: value}

	response := acceptor.HandleAccept(accept)

	accepted, ok := response.(*Accepted)
	if !ok {
		t.Fatalf("Expected Accepted, got %T", response)
	}

	if accepted.Ballot != ballot {
		t.Errorf("Expected ballot %v, got %v", ballot, accepted.Ballot)
	}

	// Check state
	acceptedBallot, acceptedValue, hasAccepted := acceptor.GetAccepted(1)
	if !hasAccepted {
		t.Error("Expected to have accepted value")
	}
	if acceptedBallot != ballot {
		t.Errorf("Expected accepted ballot %v, got %v", ballot, acceptedBallot)
	}
	if string(acceptedValue) != string(value) {
		t.Errorf("Expected accepted value %s, got %s", value, acceptedValue)
	}
}

func TestAcceptor_ReturnsAcceptedValueInPromise(t *testing.T) {
	acceptor, err := NewAcceptor(1, nil, nil)
	if err != nil {
		t.Fatalf("NewAcceptor failed: %v", err)
	}

	// First round: prepare and accept
	ballot1 := MakeBallot(1, 1)
	acceptor.HandlePrepare(&Prepare{Ballot: ballot1, Instance: 1})
	acceptor.HandleAccept(&Accept{Ballot: ballot1, Instance: 1, Value: []byte("first")})

	// Second round: new prepare should return the accepted value
	ballot2 := MakeBallot(2, 2)
	response := acceptor.HandlePrepare(&Prepare{Ballot: ballot2, Instance: 1})

	promise, ok := response.(*Promise)
	if !ok {
		t.Fatalf("Expected Promise, got %T", response)
	}

	if promise.AcceptedBallot != ballot1 {
		t.Errorf("Expected accepted ballot %v, got %v", ballot1, promise.AcceptedBallot)
	}
	if string(promise.AcceptedValue) != "first" {
		t.Errorf("Expected accepted value 'first', got '%s'", promise.AcceptedValue)
	}
}

// TestAcceptor_PersistFailureRollsBackPrepare verifies that when persisting
// a Promise fails, in-memory state reverts so it stays in lockstep with disk.
// Otherwise a subsequent crash would surface a state we never durably promised.
func TestAcceptor_PersistFailureRollsBackPrepare(t *testing.T) {
	storage := &failingStorage{MemoryStorage: NewMemoryStorage()}
	acceptor, err := NewAcceptor(1, storage, nil)
	if err != nil {
		t.Fatalf("NewAcceptor failed: %v", err)
	}

	// Successfully promise ballot1.
	ballot1 := MakeBallot(1, 1)
	if _, ok := acceptor.HandlePrepare(&Prepare{Ballot: ballot1, Instance: 1}).(*Promise); !ok {
		t.Fatalf("first Prepare: expected Promise")
	}
	if got := acceptor.GetPromised(1); got != ballot1 {
		t.Fatalf("expected promise %v, got %v", ballot1, got)
	}

	// Now make persist fail and try to bump to ballot2.
	storage.fail = true
	ballot2 := MakeBallot(2, 1)
	if _, ok := acceptor.HandlePrepare(&Prepare{Ballot: ballot2, Instance: 1}).(*Reject); !ok {
		t.Fatalf("expected Reject under persist failure")
	}

	// In-memory promise must roll back to ballot1.
	if got := acceptor.GetPromised(1); got != ballot1 {
		t.Errorf("expected promise to roll back to %v, got %v", ballot1, got)
	}
}

// TestAcceptor_PersistFailureRollsBackAccept verifies that a failed Accept
// persist reverts both the promise and the accepted (ballot, value).
func TestAcceptor_PersistFailureRollsBackAccept(t *testing.T) {
	storage := &failingStorage{MemoryStorage: NewMemoryStorage()}
	acceptor, err := NewAcceptor(1, storage, nil)
	if err != nil {
		t.Fatalf("NewAcceptor failed: %v", err)
	}

	ballot1 := MakeBallot(1, 1)
	acceptor.HandlePrepare(&Prepare{Ballot: ballot1, Instance: 1})
	if _, ok := acceptor.HandleAccept(&Accept{Ballot: ballot1, Instance: 1, Value: []byte("v1")}).(*Accepted); !ok {
		t.Fatalf("first Accept: expected Accepted")
	}

	// Fail the next Accept.
	storage.fail = true
	ballot2 := MakeBallot(2, 1)
	if _, ok := acceptor.HandleAccept(&Accept{Ballot: ballot2, Instance: 1, Value: []byte("v2")}).(*Nack); !ok {
		t.Fatalf("expected Nack under persist failure")
	}

	ab, av, has := acceptor.GetAccepted(1)
	if !has {
		t.Fatal("expected accepted state to be retained after rollback")
	}
	if ab != ballot1 {
		t.Errorf("expected accepted ballot to roll back to %v, got %v", ballot1, ab)
	}
	if string(av) != "v1" {
		t.Errorf("expected accepted value to roll back to 'v1', got %q", av)
	}
	if got := acceptor.GetPromised(1); got != ballot1 {
		t.Errorf("expected promise to roll back to %v, got %v", ballot1, got)
	}
}

func TestBallot(t *testing.T) {
	ballot := MakeBallot(100, 5)

	if ballot.Round() != 100 {
		t.Errorf("Expected round 100, got %d", ballot.Round())
	}
	if ballot.NodeID() != 5 {
		t.Errorf("Expected nodeID 5, got %d", ballot.NodeID())
	}

	// Ballots from different nodes should not collide
	ballot1 := MakeBallot(1, 1)
	ballot2 := MakeBallot(1, 2)

	if ballot1 == ballot2 {
		t.Error("Ballots from different nodes should not be equal")
	}

	// Higher round should be higher ballot
	ballotHighRound := MakeBallot(2, 1)
	if ballotHighRound <= ballot1 {
		t.Error("Higher round should produce higher ballot")
	}
}
