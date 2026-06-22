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
	peerCount  int

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

	// pending tracks in-flight proposals, keyed by instance.
	// Node.handleMessage looks responses up here via HandleResponse.
	pending map[Instance]*proposalState

	// onCommit, if set, is invoked with the Commit message when a proposal
	// reaches a quorum. Transport.Broadcast only reaches remote peers, so this
	// hook is how the proposing node delivers the commit to its own learner.
	onCommit func(*Commit)
}

// proposalState tracks the state of a single proposal.
//
// `responses` receives Promise / Reject / Accepted / Nack messages routed by
// Node.handleMessage via Proposer.HandleResponse. The buffer is sized so that
// every peer can deliver one Phase-1 and one Phase-2 response without blocking
// the message loop.
type proposalState struct {
	instance  Instance
	ballot    Ballot
	value     Value
	responses chan Message
}

// NewProposer creates a new proposer.
//
// peerCount is the number of remote peers (excluding self); it sizes the
// per-proposal response buffer so the message loop never blocks delivering
// matching Promise/Accepted/Reject/Nack messages.
func NewProposer(nodeID NodeID, transport Transport, quorumSize, peerCount int, logger Logger) *Proposer {
	if logger == nil {
		logger = NoopLogger{}
	}

	return &Proposer{
		nodeID:         nodeID,
		transport:      transport,
		logger:         logger,
		quorumSize:     quorumSize,
		peerCount:      peerCount,
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
		responses: make(chan Message, p.responseBufferSize()),
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
//
// Responses are delivered to state.responses by Node.handleMessage via
// HandleResponse; HandleResponse already filters by (instance, ballot) so any
// message that arrives here belongs to this proposal.
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

	for {
		select {
		case <-ctx.Done():
			return nil, ErrTimeout

		case msg := <-state.responses:
			switch m := msg.(type) {
			case *Promise:
				promises = append(promises, m)
				p.logger.Debug("Proposer: received Promise",
					"instance", state.instance,
					"from", m.FromNode,
					"count", len(promises),
					"need", p.quorumSize,
				)

				if len(promises) >= p.quorumSize {
					return p.findHighestAccepted(promises), nil
				}

			case *Reject:
				p.logger.Debug("Proposer: received Reject",
					"instance", state.instance,
					"from", m.FromNode,
					"higherBallot", m.HigherBallot,
				)
				return nil, ErrPreempted
			}
		}
	}
}

// runPhase2 executes the Accept phase.
//
// Late Phase-1 responses (Promise / Reject) may still arrive on
// state.responses if they were in flight when Phase 1 completed; the type
// switch below silently discards them.
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

	for {
		select {
		case <-ctx.Done():
			return ErrTimeout

		case msg := <-state.responses:
			switch m := msg.(type) {
			case *Accepted:
				accepteds = append(accepteds, m)
				p.logger.Debug("Proposer: received Accepted",
					"instance", state.instance,
					"from", m.FromNode,
					"count", len(accepteds),
					"need", p.quorumSize,
				)

				if len(accepteds) >= p.quorumSize {
					return p.broadcastCommit(ctx, state)
				}

			case *Nack:
				p.logger.Debug("Proposer: received Nack",
					"instance", state.instance,
					"from", m.FromNode,
					"higherBallot", m.HigherBallot,
				)
				return ErrPreempted
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

	// Deliver the commit to our own learner first: the value is already chosen
	// (a quorum accepted it), and Broadcast below only reaches remote peers, so
	// without this the proposing node would never learn its own committed value.
	if p.onCommit != nil {
		p.onCommit(commitMsg)
	}

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
//
// NOTE: Phase 2 of the broader fix plan replaces this with a prefix-Prepare
// that covers all instances >= fromInstance in one round. Until that lands,
// this still runs Phase 1 against the magic instance 0 and the fast path is
// not actually applied in Propose.
func (p *Proposer) BecomeLeader(ctx context.Context, fromInstance Instance) error {
	p.mu.Lock()
	p.currentRound++
	ballot := MakeBallot(p.currentRound, uint16(p.nodeID))

	state := &proposalState{
		instance:  0, // Special "leadership" instance
		ballot:    ballot,
		responses: make(chan Message, p.responseBufferSize()),
	}
	p.pending[0] = state
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.pending, 0)
		p.mu.Unlock()
	}()

	p.logger.Info("Proposer: attempting to become leader",
		"ballot", ballot,
		"fromInstance", fromInstance,
	)

	if _, err := p.runPhase1(ctx, state); err != nil {
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

// LeaderBallot returns the ballot this proposer is using as leader. The
// returned value is meaningful only when IsLeader() is true; concurrent
// callers must accept that the ballot can change after the call returns.
func (p *Proposer) LeaderBallot() Ballot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.leaderBallot
}

// StepDown marks this proposer as no longer being the leader.
func (p *Proposer) StepDown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isLeader = false
	p.logger.Info("Proposer: stepped down from leadership")
}

// HandleResponse routes a Phase-1/Phase-2 response message to the pending
// proposal it belongs to. Called by Node.handleMessage. It is non-blocking:
// if the proposal's response buffer is full (which should not happen given
// responseBufferSize accounts for one Promise + one Accepted per peer), the
// message is dropped and a warning is logged.
func (p *Proposer) HandleResponse(msg Message) {
	var instance Instance
	var ballot Ballot

	switch m := msg.(type) {
	case *Promise:
		instance, ballot = m.Instance, m.Ballot
	case *Reject:
		instance, ballot = m.Instance, m.Ballot
	case *Accepted:
		instance, ballot = m.Instance, m.Ballot
	case *Nack:
		instance, ballot = m.Instance, m.Ballot
	default:
		return
	}

	p.mu.Lock()
	state, ok := p.pending[instance]
	p.mu.Unlock()

	if !ok || state.ballot != ballot {
		// No matching proposal — stale response from a finished or preempted
		// attempt. Safe to drop.
		return
	}

	select {
	case state.responses <- msg:
	default:
		p.logger.Warn("Proposer: response buffer full, dropping",
			"instance", instance,
			"ballot", ballot,
		)
	}
}

// responseBufferSize returns the per-proposal response buffer size: at least
// one Promise plus one Accepted from each peer, so the message loop never
// blocks delivering matching responses.
func (p *Proposer) responseBufferSize() int {
	n := 2*p.peerCount + 1
	if n < 4 {
		n = 4
	}
	return n
}
