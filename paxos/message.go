package paxos

import (
	"encoding/binary"
	"errors"
	"io"
)

// MessageType identifies the type of Paxos message.
type MessageType uint8

const (
	// Phase 1 messages
	MsgPrepare    MessageType = 1 // Proposer -> Acceptors: "May I propose with this ballot?"
	MsgPromise    MessageType = 2 // Acceptor -> Proposer: "Yes, and here's what I've seen"
	MsgReject     MessageType = 3 // Acceptor -> Proposer: "No, I've promised a higher ballot"

	// Phase 2 messages
	MsgAccept     MessageType = 4 // Proposer -> Acceptors: "Please accept this value"
	MsgAccepted   MessageType = 5 // Acceptor -> Proposer: "I accepted it"
	MsgNack       MessageType = 6 // Acceptor -> Proposer: "Rejected, higher ballot seen"

	// Commit notification
	MsgCommit     MessageType = 7 // Leader -> All: "This value is committed"

	// Leader election / heartbeat
	MsgHeartbeat  MessageType = 8 // Leader -> Followers: "I'm still alive"

	// Catch-up
	MsgCatchupReq MessageType = 9  // Follower -> Leader: "I'm behind, send me entries"
	MsgCatchupRes MessageType = 10 // Leader -> Follower: "Here are the entries you missed"
)

// Message represents any Paxos protocol message.
type Message interface {
	Type() MessageType
	Encode(w io.Writer) error
}

// decodeMessage reads a message from a reader.
func decodeMessage(r io.Reader) (Message, error) {
	var msgType MessageType
	if err := binary.Read(r, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}

	switch msgType {
	case MsgPrepare:
		return decodePrepare(r)
	case MsgPromise:
		return decodePromise(r)
	case MsgReject:
		return decodeReject(r)
	case MsgAccept:
		return decodeAccept(r)
	case MsgAccepted:
		return decodeAccepted(r)
	case MsgNack:
		return decodeNack(r)
	case MsgCommit:
		return decodeCommit(r)
	case MsgHeartbeat:
		return decodeHeartbeat(r)
	default:
		return nil, errors.New("unknown message type")
	}
}

// --- Prepare (Phase 1a) ---

// Prepare is sent by a proposer to begin Phase 1.
// "I want to propose with ballot B for instance I. May I?"
type Prepare struct {
	Ballot   Ballot
	Instance Instance
}

func (m *Prepare) Type() MessageType { return MsgPrepare }

func (m *Prepare) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, m.Instance)
}

func decodePrepare(r io.Reader) (*Prepare, error) {
	m := &Prepare{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	return m, nil
}

// --- Promise (Phase 1b) ---

// Promise is sent by an acceptor in response to Prepare.
// "I promise not to accept proposals with ballot < B.
//  Here's the highest ballot and value I've accepted (if any)."
type Promise struct {
	Ballot         Ballot   // The ballot we're promising
	Instance       Instance
	FromNode       NodeID
	AcceptedBallot Ballot   // Highest ballot we've accepted (0 if none)
	AcceptedValue  Value    // Value of the accepted proposal (nil if none)
}

func (m *Promise) Type() MessageType { return MsgPromise }

func (m *Promise) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Instance); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.FromNode); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.AcceptedBallot); err != nil {
		return err
	}
	// Write value with length prefix
	if err := binary.Write(w, binary.BigEndian, uint32(len(m.AcceptedValue))); err != nil {
		return err
	}
	if len(m.AcceptedValue) > 0 {
		if _, err := w.Write(m.AcceptedValue); err != nil {
			return err
		}
	}
	return nil
}

func decodePromise(r io.Reader) (*Promise, error) {
	m := &Promise{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.FromNode); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.AcceptedBallot); err != nil {
		return nil, err
	}
	var valueLen uint32
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}
	if valueLen > 0 {
		m.AcceptedValue = make([]byte, valueLen)
		if _, err := io.ReadFull(r, m.AcceptedValue); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// --- Reject (Phase 1b negative) ---

// Reject is sent when an acceptor has promised a higher ballot.
type Reject struct {
	Ballot        Ballot   // The ballot that was rejected
	Instance      Instance
	FromNode      NodeID
	HigherBallot  Ballot   // The higher ballot we've promised
}

func (m *Reject) Type() MessageType { return MsgReject }

func (m *Reject) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Instance); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.FromNode); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, m.HigherBallot)
}

func decodeReject(r io.Reader) (*Reject, error) {
	m := &Reject{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.FromNode); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.HigherBallot); err != nil {
		return nil, err
	}
	return m, nil
}

// --- Accept (Phase 2a) ---

// Accept is sent by a proposer to request acceptance of a value.
// "Please accept value V with ballot B for instance I."
type Accept struct {
	Ballot   Ballot
	Instance Instance
	Value    Value
}

func (m *Accept) Type() MessageType { return MsgAccept }

func (m *Accept) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Instance); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(m.Value))); err != nil {
		return err
	}
	if len(m.Value) > 0 {
		if _, err := w.Write(m.Value); err != nil {
			return err
		}
	}
	return nil
}

func decodeAccept(r io.Reader) (*Accept, error) {
	m := &Accept{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	var valueLen uint32
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}
	if valueLen > 0 {
		m.Value = make([]byte, valueLen)
		if _, err := io.ReadFull(r, m.Value); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// --- Accepted (Phase 2b) ---

// Accepted is sent by an acceptor confirming acceptance.
type Accepted struct {
	Ballot   Ballot
	Instance Instance
	FromNode NodeID
}

func (m *Accepted) Type() MessageType { return MsgAccepted }

func (m *Accepted) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Instance); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, m.FromNode)
}

func decodeAccepted(r io.Reader) (*Accepted, error) {
	m := &Accepted{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.FromNode); err != nil {
		return nil, err
	}
	return m, nil
}

// --- Nack (Phase 2b negative) ---

// Nack is sent when an acceptor rejects an Accept due to a higher promise.
type Nack struct {
	Ballot       Ballot
	Instance     Instance
	FromNode     NodeID
	HigherBallot Ballot
}

func (m *Nack) Type() MessageType { return MsgNack }

func (m *Nack) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Instance); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.FromNode); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, m.HigherBallot)
}

func decodeNack(r io.Reader) (*Nack, error) {
	m := &Nack{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.FromNode); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.HigherBallot); err != nil {
		return nil, err
	}
	return m, nil
}

// --- Commit ---

// Commit announces that a value has been chosen for an instance.
type Commit struct {
	Ballot   Ballot
	Instance Instance
	Value    Value
}

func (m *Commit) Type() MessageType { return MsgCommit }

func (m *Commit) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Instance); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(m.Value))); err != nil {
		return err
	}
	if len(m.Value) > 0 {
		if _, err := w.Write(m.Value); err != nil {
			return err
		}
	}
	return nil
}

func decodeCommit(r io.Reader) (*Commit, error) {
	m := &Commit{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Instance); err != nil {
		return nil, err
	}
	var valueLen uint32
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}
	if valueLen > 0 {
		m.Value = make([]byte, valueLen)
		if _, err := io.ReadFull(r, m.Value); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// --- Heartbeat ---

// Heartbeat is sent by the leader to maintain leadership.
type Heartbeat struct {
	Ballot       Ballot
	LeaderID     NodeID
	LastInstance Instance // Highest committed instance
}

func (m *Heartbeat) Type() MessageType { return MsgHeartbeat }

func (m *Heartbeat) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.Ballot); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.LeaderID); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, m.LastInstance)
}

func decodeHeartbeat(r io.Reader) (*Heartbeat, error) {
	m := &Heartbeat{}
	if err := binary.Read(r, binary.BigEndian, &m.Ballot); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.LeaderID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.LastInstance); err != nil {
		return nil, err
	}
	return m, nil
}
