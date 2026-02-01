package paxos

import (
	"context"
	"sync"
	"time"
)

// Proposer implements the proposer role in Paxos.
//
// The proposer drives consensus by:
// 1. Phase 1 (Prepare): Establishing a ballot and learning any previously accepted values
// 2. Phase 2 (Accept): Proposing a value and getting it accepted by a quorum
//
// In Multi-Paxos, a stable leader can skip Phase 1 for subsequent proposals
// using the same ballot, dramatically improving performance.
type Proposer struct {
	nodeID     NodeID
	transport  Transport
	logger     Logger
	quorumSize int

	prepareTimeout time.Duration
	acceptTimeout  time.Duration

	mu sync.Mutex

	// currentBallot is our current ballot number
	currentRound uint32

	// isLeader indicates if we believe we are the leader
	isLeader bool

	// leaderBallot is the ballot we're using as leader (valid if isLeader)
	leaderBallot Ballot

	// nextInstance is the next instance number to propose for
	nextInstance Instance

	// pending tracks in-flight proposals
	pending map[Instance]*proposalState
}

// proposalState tracks the state of a single proposal.
type proposalState struct {
	instance   Instance
	ballot     Ballot
	value      Value
	promises   []*Promise
	accepteds  []*Accepted
	done       chan error
	committed  bool
}

// NewProposer creates a new proposer.
func NewProposer(nodeID NodeID, transport Transport, quorumSize int, logger Logger) *Proposer {
	if logger == nil {
		logger = NoopLogger{}
	}

	return &Proposer{
		nodeID:         nodeID,
		transport:      transport,
		logger:         logger,
		quorumSize:     quorumSize,
		prepareTimeout: 1 * time.Second,
		acceptTimeout:  1 * time.Second,
		currentRound:   0,
		nextInstance:   1,
		pending:        make(map[Instance]*proposalState),
	}
}

// SetTimeouts configures the proposal timeouts.
func (p *Proposer) SetTimeouts(prepare, accept time.Duration) {
	p.prepareTimeout = prepare
	p.acceptTimeout = accept
}

// Propose attempts to get a value agreed upon for the next instance.
//
// This is the main entry point for clients. It:
// 1. Acquires an instance number
// 2. Runs Paxos Phase 1 (unless we're already the leader)
// 3. Runs Paxos Phase 2 with the value
// 4. Returns when the value is committed or an error occurs
func (p *Proposer) Propose(ctx context.Context, value Value) (Instance, error) {
	p.mu.Lock()
	instance := p.nextInstance
	p.nextInstance++

	// Create a new ballot
	p.currentRound++
	ballot := MakeBallot(p.currentRound, uint16(p.nodeID))

	state := &proposalState{
		instance:  instance,
		ballot:    ballot,
		value:     value,
		promises:  make([]*Promise, 0),
		accepteds: make([]*Accepted, 0),
		done:      make(chan error, 1),
	}
	p.pending[instance] = state
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.pending, instance)
		p.mu.Unlock()
	}()

	p.logger.Info("Proposer: starting proposal",
		"instance", instance,
		"ballot", ballot,
		"valueLen", len(value),
	)

	// Phase 1: Prepare
	acceptedValue, err := p.runPhase1(ctx, state)
	if err != nil {
		return 0, err
	}

	// If someone already accepted a value, we must propose that instead
	// (This is the key safety property of Paxos)
	if acceptedValue != nil {
		p.logger.Info("Proposer: using previously accepted value",
			"instance", instance,
		)
		state.value = acceptedValue
	}

	// Phase 2: Accept
	err = p.runPhase2(ctx, state)
	if err != nil {
		return 0, err
	}

	p.logger.Info("Proposer: proposal committed",
		"instance", instance,
		"ballot", ballot,
	)

	return instance, nil
}

// runPhase1 executes the Prepare phase.
// Returns any previously accepted value that we must use.
func (p *Proposer) runPhase1(ctx context.Context, state *proposalState) (Value, error) {
	p.logger.Debug("Proposer: starting Phase 1",
		"instance", state.instance,
		"ballot", state.ballot,
	)

	// Send Prepare to all acceptors
	prepareMsg := &Prepare{
		Ballot:   state.ballot,
		Instance: state.instance,
	}

	if err := p.transport.Broadcast(ctx, prepareMsg); err != nil {
		return nil, err
	}

	// Wait for quorum of promises
	ctx, cancel := context.WithTimeout(ctx, p.prepareTimeout)
	defer cancel()

	promises := make([]*Promise, 0, p.quorumSize)
	responses := p.transport.Receive()

	for {
		select {
		case <-ctx.Done():
			if len(promises) >= p.quorumSize {
				break
			}
			return nil, ErrTimeout

		case env := <-responses:
			switch msg := env.Message.(type) {
			case *Promise:
				if msg.Ballot == state.ballot && msg.Instance == state.instance {
					promises = append(promises, msg)
					p.logger.Debug("Proposer: received Promise",
						"instance", state.instance,
						"from", msg.FromNode,
						"count", len(promises),
						"need", p.quorumSize,
					)

					if len(promises) >= p.quorumSize {
						// Find highest accepted value (if any)
						return p.findHighestAccepted(promises), nil
					}
				}

			case *Reject:
				if msg.Ballot == state.ballot && msg.Instance == state.instance {
					p.logger.Debug("Proposer: received Reject",
						"instance", state.instance,
						"from", msg.FromNode,
						"higherBallot", msg.HigherBallot,
					)
					// A higher ballot exists - we're preempted
					return nil, ErrPreempted
				}
			}
		}
	}
}

// runPhase2 executes the Accept phase.
func (p *Proposer) runPhase2(ctx context.Context, state *proposalState) error {
	p.logger.Debug("Proposer: starting Phase 2",
		"instance", state.instance,
		"ballot", state.ballot,
		"valueLen", len(state.value),
	)

	// Send Accept to all acceptors
	acceptMsg := &Accept{
		Ballot:   state.ballot,
		Instance: state.instance,
		Value:    state.value,
	}

	if err := p.transport.Broadcast(ctx, acceptMsg); err != nil {
		return err
	}

	// Wait for quorum of accepteds
	ctx, cancel := context.WithTimeout(ctx, p.acceptTimeout)
	defer cancel()

	accepteds := make([]*Accepted, 0, p.quorumSize)
	responses := p.transport.Receive()

	for {
		select {
		case <-ctx.Done():
			if len(accepteds) >= p.quorumSize {
				break
			}
			return ErrTimeout

		case env := <-responses:
			switch msg := env.Message.(type) {
			case *Accepted:
				if msg.Ballot == state.ballot && msg.Instance == state.instance {
					accepteds = append(accepteds, msg)
					p.logger.Debug("Proposer: received Accepted",
						"instance", state.instance,
						"from", msg.FromNode,
						"count", len(accepteds),
						"need", p.quorumSize,
					)

					if len(accepteds) >= p.quorumSize {
						// Value is chosen! Broadcast commit.
						return p.broadcastCommit(ctx, state)
					}
				}

			case *Nack:
				if msg.Ballot == state.ballot && msg.Instance == state.instance {
					p.logger.Debug("Proposer: received Nack",
						"instance", state.instance,
						"from", msg.FromNode,
						"higherBallot", msg.HigherBallot,
					)
					return ErrPreempted
				}
			}
		}
	}
}

// broadcastCommit sends a Commit message to all nodes.
func (p *Proposer) broadcastCommit(ctx context.Context, state *proposalState) error {
	commitMsg := &Commit{
		Ballot:   state.ballot,
		Instance: state.instance,
		Value:    state.value,
	}

	p.logger.Debug("Proposer: broadcasting Commit",
		"instance", state.instance,
		"ballot", state.ballot,
	)

	return p.transport.Broadcast(ctx, commitMsg)
}

// findHighestAccepted finds the value with the highest accepted ballot.
// This is used in Phase 1 to determine what value to propose.
//
// From Paxos Made Simple:
// "If any acceptors had accepted any proposals, then the proposer
// chooses the value of the proposal with the highest ballot number."
func (p *Proposer) findHighestAccepted(promises []*Promise) Value {
	var highestBallot Ballot
	var highestValue Value

	for _, promise := range promises {
		if promise.AcceptedBallot > highestBallot {
			highestBallot = promise.AcceptedBallot
			highestValue = promise.AcceptedValue
		}
	}

	return highestValue
}

// BecomeLeader attempts to establish this node as the leader.
// This runs Phase 1 for a range of instances, allowing
// subsequent proposals to skip directly to Phase 2.
func (p *Proposer) BecomeLeader(ctx context.Context, fromInstance Instance) error {
	p.mu.Lock()
	p.currentRound++
	ballot := MakeBallot(p.currentRound, uint16(p.nodeID))
	p.mu.Unlock()

	p.logger.Info("Proposer: attempting to become leader",
		"ballot", ballot,
		"fromInstance", fromInstance,
	)

	// Run Phase 1 for the leadership instance (instance 0 is special)
	state := &proposalState{
		instance: 0, // Special "leadership" instance
		ballot:   ballot,
	}

	_, err := p.runPhase1(ctx, state)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.isLeader = true
	p.leaderBallot = ballot
	p.mu.Unlock()

	p.logger.Info("Proposer: became leader",
		"ballot", ballot,
	)

	return nil
}

// IsLeader returns true if this proposer believes it is the leader.
func (p *Proposer) IsLeader() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isLeader
}

// StepDown marks this proposer as no longer being the leader.
func (p *Proposer) StepDown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isLeader = false
	p.logger.Info("Proposer: stepped down from leadership")
}
