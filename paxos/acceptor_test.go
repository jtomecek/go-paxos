package paxos

import (
	"testing"
)

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
