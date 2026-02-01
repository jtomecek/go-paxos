// Package paxos implements the Multi-Paxos consensus algorithm.
//
// Multi-Paxos allows a cluster of nodes to agree on a sequence of values,
// tolerating failures of up to (N-1)/2 nodes in a cluster of N nodes.
package paxos

import (
	"context"
	"time"
)

// Ballot represents a unique proposal number.
// Higher ballots take precedence over lower ones.
// Format: (round << 16) | nodeID - ensures global uniqueness
type Ballot uint64

// MakeBallot creates a ballot number from a round and node ID.
// This ensures ballots from different nodes never collide.
func MakeBallot(round uint32, nodeID uint16) Ballot {
	return Ballot(uint64(round)<<16 | uint64(nodeID))
}

// Round extracts the round number from a ballot.
func (b Ballot) Round() uint32 {
	return uint32(b >> 16)
}

// NodeID extracts the proposing node's ID from a ballot.
func (b Ballot) NodeID() uint16 {
	return uint16(b & 0xFFFF)
}

// Instance identifies a position in the replicated log.
// Each instance goes through independent Paxos consensus.
type Instance uint64

// NodeID uniquely identifies a node in the cluster.
type NodeID uint16

// Value represents the data being agreed upon.
type Value []byte

// CommittedValue represents a value that has been chosen by consensus.
type CommittedValue struct {
	Instance Instance  // Log position
	Value    Value     // The agreed-upon value
	Ballot   Ballot    // Ballot that achieved consensus
}

// Config holds the configuration for a Paxos node.
type Config struct {
	// NodeID is the unique identifier for this node (required).
	// Must be unique across the cluster and fit in uint16.
	NodeID NodeID

	// Address is the listen address for this node (required).
	// Format: "host:port"
	Address string

	// Peers contains addresses of other nodes in the cluster (required).
	// Must include all other nodes for proper operation.
	Peers []string

	// Transport is the network transport implementation.
	// Default: TCP transport
	Transport Transport

	// Storage is the persistence backend.
	// Default: in-memory storage (not suitable for production)
	Storage Storage

	// Logger receives log messages.
	// Default: no logging
	Logger Logger

	// PrepareTimeout is how long to wait for Phase 1 responses.
	// Default: 1 second
	PrepareTimeout time.Duration

	// AcceptTimeout is how long to wait for Phase 2 responses.
	// Default: 1 second
	AcceptTimeout time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats.
	// Default: 100 milliseconds
	HeartbeatInterval time.Duration

	// ElectionTimeout is how long to wait before starting an election.
	// Should be > HeartbeatInterval * 3 to avoid spurious elections.
	// Default: 1 second
	ElectionTimeout time.Duration

	// MaxInFlightInstances limits concurrent proposals.
	// Default: 100
	MaxInFlightInstances int
}

// DefaultConfig returns a Config with sensible defaults filled in.
func (c Config) WithDefaults() Config {
	if c.PrepareTimeout == 0 {
		c.PrepareTimeout = 1 * time.Second
	}
	if c.AcceptTimeout == 0 {
		c.AcceptTimeout = 1 * time.Second
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 1 * time.Second
	}
	if c.MaxInFlightInstances == 0 {
		c.MaxInFlightInstances = 100
	}
	return c
}

// Validate checks if the configuration is valid.
func (c Config) Validate() error {
	if c.NodeID == 0 {
		return ErrInvalidNodeID
	}
	if c.Address == "" {
		return ErrInvalidAddress
	}
	if len(c.Peers) == 0 {
		return ErrNoPeers
	}
	if c.ElectionTimeout <= c.HeartbeatInterval*3 {
		return ErrElectionTimeoutTooShort
	}
	return nil
}

// Transport defines the interface for network communication.
type Transport interface {
	// Listen starts accepting connections on the configured address.
	Listen(addr string) error

	// Connect establishes a connection to a peer.
	Connect(addr string) error

	// Send transmits a message to a specific peer.
	Send(ctx context.Context, to string, msg Message) error

	// Broadcast sends a message to all connected peers.
	Broadcast(ctx context.Context, msg Message) error

	// Receive returns a channel of incoming messages.
	Receive() <-chan MessageEnvelope

	// Close shuts down the transport.
	Close() error
}

// MessageEnvelope wraps a message with sender information.
type MessageEnvelope struct {
	From    string  // Sender address
	Message Message // The message itself
}

// Storage defines the interface for persistent state.
type Storage interface {
	// SaveAcceptorState persists the acceptor's promised and accepted state.
	SaveAcceptorState(state AcceptorState) error

	// LoadAcceptorState retrieves the acceptor's persisted state.
	LoadAcceptorState() (AcceptorState, error)

	// AppendLog adds a committed entry to the write-ahead log.
	AppendLog(entry LogEntry) error

	// GetLog retrieves a log entry by instance number.
	GetLog(instance Instance) (LogEntry, error)

	// GetLogRange retrieves log entries in a range [from, to).
	GetLogRange(from, to Instance) ([]LogEntry, error)

	// GetLastInstance returns the highest instance number in the log.
	GetLastInstance() (Instance, error)

	// SaveSnapshot persists a snapshot at a given instance.
	SaveSnapshot(snap Snapshot) error

	// LoadSnapshot retrieves the latest snapshot.
	LoadSnapshot() (Snapshot, error)

	// Close releases resources.
	Close() error
}

// AcceptorState holds the persistent state of an acceptor.
// This MUST be persisted before responding to any message.
type AcceptorState struct {
	// PromisedBallot is the highest ballot we promised not to accept below.
	PromisedBallot map[Instance]Ballot

	// AcceptedBallot is the ballot of the last accepted proposal.
	AcceptedBallot map[Instance]Ballot

	// AcceptedValue is the value of the last accepted proposal.
	AcceptedValue map[Instance]Value
}

// LogEntry represents a committed value in the log.
type LogEntry struct {
	Instance Instance
	Ballot   Ballot
	Value    Value
}

// Snapshot represents a point-in-time snapshot of committed state.
type Snapshot struct {
	LastInstance Instance
	Data         []byte
	Checksum     []byte
}

// Logger defines a simple logging interface.
type Logger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// NoopLogger is a logger that discards all messages.
type NoopLogger struct{}

func (NoopLogger) Debug(msg string, keysAndValues ...interface{}) {}
func (NoopLogger) Info(msg string, keysAndValues ...interface{})  {}
func (NoopLogger) Warn(msg string, keysAndValues ...interface{})  {}
func (NoopLogger) Error(msg string, keysAndValues ...interface{}) {}
