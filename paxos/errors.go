package paxos

import "errors"

// Configuration errors
var (
	ErrInvalidNodeID           = errors.New("paxos: node ID must be non-zero")
	ErrInvalidAddress          = errors.New("paxos: address is required")
	ErrNoPeers                 = errors.New("paxos: at least one peer is required")
	ErrElectionTimeoutTooShort = errors.New("paxos: election timeout must be > 3x heartbeat interval")
)

// Runtime errors
var (
	// ErrNotLeader is returned when a non-leader tries to propose.
	ErrNotLeader = errors.New("paxos: not the leader")

	// ErrNoQuorum is returned when quorum cannot be reached.
	ErrNoQuorum = errors.New("paxos: could not reach quorum")

	// ErrPreempted is returned when a higher ballot preempts our proposal.
	ErrPreempted = errors.New("paxos: preempted by higher ballot")

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("paxos: operation timed out")

	// ErrShutdown is returned when the node is shutting down.
	ErrShutdown = errors.New("paxos: node is shutting down")

	// ErrInstanceGap is returned when there's a gap in the log.
	ErrInstanceGap = errors.New("paxos: gap in instance sequence")
)

// Storage errors
var (
	ErrNotFound      = errors.New("paxos: entry not found")
	ErrStorageClosed = errors.New("paxos: storage is closed")
)
