package paxos

import (
	"bytes"
	"testing"
)

func TestMessageEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
	}{
		{
			name: "Prepare",
			msg:  &Prepare{Ballot: MakeBallot(1, 2), Instance: 10},
		},
		{
			name: "Promise without accepted value",
			msg:  &Promise{Ballot: MakeBallot(1, 2), Instance: 10, FromNode: 3},
		},
		{
			name: "Promise with accepted value",
			msg: &Promise{
				Ballot:         MakeBallot(1, 2),
				Instance:       10,
				FromNode:       3,
				AcceptedBallot: MakeBallot(0, 1),
				AcceptedValue:  []byte("hello world"),
			},
		},
		{
			name: "Reject",
			msg:  &Reject{Ballot: MakeBallot(1, 2), Instance: 10, FromNode: 3, HigherBallot: MakeBallot(2, 4)},
		},
		{
			name: "Accept",
			msg:  &Accept{Ballot: MakeBallot(1, 2), Instance: 10, Value: []byte("test value")},
		},
		{
			name: "Accepted",
			msg:  &Accepted{Ballot: MakeBallot(1, 2), Instance: 10, FromNode: 5},
		},
		{
			name: "Nack",
			msg:  &Nack{Ballot: MakeBallot(1, 2), Instance: 10, FromNode: 5, HigherBallot: MakeBallot(3, 6)},
		},
		{
			name: "Commit",
			msg:  &Commit{Ballot: MakeBallot(1, 2), Instance: 10, Value: []byte("committed value")},
		},
		{
			name: "Heartbeat",
			msg:  &Heartbeat{Ballot: MakeBallot(5, 1), LeaderID: 1, LastInstance: 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			var buf bytes.Buffer
			if err := tt.msg.Encode(&buf); err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Decode
			decoded, err := decodeMessage(&buf)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Compare based on type
			switch expected := tt.msg.(type) {
			case *Prepare:
				actual := decoded.(*Prepare)
				if expected.Ballot != actual.Ballot || expected.Instance != actual.Instance {
					t.Errorf("Mismatch: expected %+v, got %+v", expected, actual)
				}

			case *Promise:
				actual := decoded.(*Promise)
				if expected.Ballot != actual.Ballot ||
					expected.Instance != actual.Instance ||
					expected.FromNode != actual.FromNode ||
					expected.AcceptedBallot != actual.AcceptedBallot ||
					!bytes.Equal(expected.AcceptedValue, actual.AcceptedValue) {
					t.Errorf("Mismatch: expected %+v, got %+v", expected, actual)
				}

			case *Accept:
				actual := decoded.(*Accept)
				if expected.Ballot != actual.Ballot ||
					expected.Instance != actual.Instance ||
					!bytes.Equal(expected.Value, actual.Value) {
					t.Errorf("Mismatch: expected %+v, got %+v", expected, actual)
				}

			case *Commit:
				actual := decoded.(*Commit)
				if expected.Ballot != actual.Ballot ||
					expected.Instance != actual.Instance ||
					!bytes.Equal(expected.Value, actual.Value) {
					t.Errorf("Mismatch: expected %+v, got %+v", expected, actual)
				}

			case *Heartbeat:
				actual := decoded.(*Heartbeat)
				if expected.Ballot != actual.Ballot ||
					expected.LeaderID != actual.LeaderID ||
					expected.LastInstance != actual.LastInstance {
					t.Errorf("Mismatch: expected %+v, got %+v", expected, actual)
				}
			}
		})
	}
}
