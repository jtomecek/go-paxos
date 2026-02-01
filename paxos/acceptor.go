package paxos

import (
	"sync"
)

// Acceptor implements the acceptor role in Paxos.
//
// The acceptor's job is simple but critical:
// 1. Remember the highest ballot it has promised
// 2. Remember the highest ballot and value it has accepted
// 3. Only accept proposals with ballots >= its promised ballot
//
// CRITICAL: All state changes MUST be persisted before responding.
// If an acceptor crashes after responding but before persisting,
// it could violate the protocol's safety guarantees.
type Acceptor struct {
	nodeID  NodeID
	storage Storage
	logger  Logger

	mu sync.RWMutex

	// promised maps instance -> highest ballot we've promised not to accept below.
	// If we receive a Prepare with ballot B, we promise not to accept any ballot < B.
	promised map[Instance]Ballot

	// accepted maps instance -> the ballot of the last proposal we accepted.
	acceptedBallot map[Instance]Ballot

	// acceptedValue maps instance -> the value of the last proposal we accepted.
	acceptedValue map[Instance]Value
}

// NewAcceptor creates a new acceptor.
func NewAcceptor(nodeID NodeID, storage Storage, logger Logger) (*Acceptor, error) {
	if logger == nil {
		logger = NoopLogger{}
	}

	a := &Acceptor{
		nodeID:         nodeID,
		storage:        storage,
		logger:         logger,
		promised:       make(map[Instance]Ballot),
		acceptedBallot: make(map[Instance]Ballot),
		acceptedValue:  make(map[Instance]Value),
	}

	// Recover state from storage
	if storage != nil {
		state, err := storage.LoadAcceptorState()
		if err != nil && err != ErrNotFound {
			return nil, err
		}
		if state.PromisedBallot != nil {
			a.promised = state.PromisedBallot
		}
		if state.AcceptedBallot != nil {
			a.acceptedBallot = state.AcceptedBallot
		}
		if state.AcceptedValue != nil {
			a.acceptedValue = state.AcceptedValue
		}
	}

	return a, nil
}

// HandlePrepare processes a Phase 1 Prepare message.
//
// Algorithm:
//   - If ballot >= our promised ballot for this instance:
//     - Update promised ballot
//     - Persist state
//     - Reply with Promise containing any previously accepted value
//   - Otherwise:
//     - Reply with Reject containing our promised ballot
func (a *Acceptor) HandlePrepare(msg *Prepare) Message {
	a.mu.Lock()
	defer a.mu.Unlock()

	instance := msg.Instance
	ballot := msg.Ballot

	promisedBallot := a.promised[instance]

	a.logger.Debug("Acceptor: handling Prepare",
		"instance", instance,
		"ballot", ballot,
		"promised", promisedBallot,
	)

	// Check if we can promise this ballot
	if ballot < promisedBallot {
		// We've already promised a higher ballot - reject
		a.logger.Debug("Acceptor: rejecting Prepare (ballot too low)",
			"instance", instance,
			"ballot", ballot,
			"promised", promisedBallot,
		)
		return &Reject{
			Ballot:       ballot,
			Instance:     instance,
			FromNode:     a.nodeID,
			HigherBallot: promisedBallot,
		}
	}

	// Update our promise
	a.promised[instance] = ballot

	// Persist before responding (CRITICAL for safety)
	if err := a.persist(); err != nil {
		a.logger.Error("Acceptor: failed to persist state", "error", err)
		// We can't safely respond if we can't persist
		// Return a reject to be safe
		return &Reject{
			Ballot:       ballot,
			Instance:     instance,
			FromNode:     a.nodeID,
			HigherBallot: ballot, // This signals a storage error rather than a higher ballot
		}
	}

	// Send promise with any previously accepted value
	return &Promise{
		Ballot:         ballot,
		Instance:       instance,
		FromNode:       a.nodeID,
		AcceptedBallot: a.acceptedBallot[instance],
		AcceptedValue:  a.acceptedValue[instance],
	}
}

// HandleAccept processes a Phase 2 Accept message.
//
// Algorithm:
//   - If ballot >= our promised ballot for this instance:
//     - Accept the proposal (update accepted ballot and value)
//     - Persist state
//     - Reply with Accepted
//   - Otherwise:
//     - Reply with Nack containing our promised ballot
func (a *Acceptor) HandleAccept(msg *Accept) Message {
	a.mu.Lock()
	defer a.mu.Unlock()

	instance := msg.Instance
	ballot := msg.Ballot

	promisedBallot := a.promised[instance]

	a.logger.Debug("Acceptor: handling Accept",
		"instance", instance,
		"ballot", ballot,
		"promised", promisedBallot,
		"valueLen", len(msg.Value),
	)

	// Check if we can accept this proposal
	if ballot < promisedBallot {
		// We've promised a higher ballot - reject
		a.logger.Debug("Acceptor: rejecting Accept (ballot too low)",
			"instance", instance,
			"ballot", ballot,
			"promised", promisedBallot,
		)
		return &Nack{
			Ballot:       ballot,
			Instance:     instance,
			FromNode:     a.nodeID,
			HigherBallot: promisedBallot,
		}
	}

	// Accept the proposal
	a.promised[instance] = ballot // Also update promise
	a.acceptedBallot[instance] = ballot
	a.acceptedValue[instance] = msg.Value

	// Persist before responding (CRITICAL for safety)
	if err := a.persist(); err != nil {
		a.logger.Error("Acceptor: failed to persist state", "error", err)
		// Roll back the acceptance
		delete(a.acceptedBallot, instance)
		delete(a.acceptedValue, instance)
		return &Nack{
			Ballot:       ballot,
			Instance:     instance,
			FromNode:     a.nodeID,
			HigherBallot: ballot,
		}
	}

	return &Accepted{
		Ballot:   ballot,
		Instance: instance,
		FromNode: a.nodeID,
	}
}

// GetAccepted returns the accepted value for an instance, if any.
func (a *Acceptor) GetAccepted(instance Instance) (Ballot, Value, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	ballot, hasBallot := a.acceptedBallot[instance]
	value := a.acceptedValue[instance]

	return ballot, value, hasBallot
}

// GetPromised returns the promised ballot for an instance.
func (a *Acceptor) GetPromised(instance Instance) Ballot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.promised[instance]
}

// persist saves the acceptor state to storage.
func (a *Acceptor) persist() error {
	if a.storage == nil {
		return nil
	}

	state := AcceptorState{
		PromisedBallot: a.promised,
		AcceptedBallot: a.acceptedBallot,
		AcceptedValue:  a.acceptedValue,
	}

	return a.storage.SaveAcceptorState(state)
}

// ForgetInstance clears state for an instance (after it's been committed).
// This is an optimization to prevent unbounded memory growth.
func (a *Acceptor) ForgetInstance(instance Instance) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.promised, instance)
	delete(a.acceptedBallot, instance)
	delete(a.acceptedValue, instance)
}
