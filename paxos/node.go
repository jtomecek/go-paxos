package paxos

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Node represents a participant in a Paxos cluster.
// It combines all three roles (Proposer, Acceptor, Learner) into
// a single coherent unit.
type Node struct {
	config Config
	nodeID NodeID

	transport Transport
	storage   Storage
	logger    Logger

	acceptor *Acceptor
	proposer *Proposer

	// Learner state
	mu            sync.RWMutex
	committed     map[Instance]CommittedValue
	lastCommitted Instance
	commitCh      chan CommittedValue

	// Leader election state
	currentLeader NodeID
	leaderBallot  Ballot
	// electionDeadline is when a follower will call an election if it has not
	// heard from a leader. It is randomized (see randomElectionTimeout) so that
	// nodes don't all time out together and duel for leadership.
	electionDeadline time.Time
	// electing guards against starting more than one election at a time; an
	// election can take up to PrepareTimeout*2 while the ticker keeps firing.
	electing bool
	// rng backs randomElectionTimeout. Only accessed while holding mu.
	rng *rand.Rand

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	started bool
}

// NewNode creates a new Paxos node with the given configuration.
func NewNode(cfg Config) (*Node, error) {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = NoopLogger{}
	}

	storage := cfg.Storage
	if storage == nil {
		storage = NewMemoryStorage()
	}

	transport := cfg.Transport
	if transport == nil {
		transport = NewTCPTransport(cfg.NodeID, logger)
	}

	// Calculate quorum size: majority of total nodes (including self)
	totalNodes := len(cfg.Peers) + 1
	quorumSize := (totalNodes / 2) + 1

	acceptor, err := NewAcceptor(cfg.NodeID, storage, logger)
	if err != nil {
		return nil, err
	}

	proposer := NewProposer(cfg.NodeID, transport, quorumSize, len(cfg.Peers), logger)
	proposer.SetTimeouts(cfg.PrepareTimeout, cfg.AcceptTimeout)

	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		config:        cfg,
		nodeID:        cfg.NodeID,
		transport:     transport,
		storage:       storage,
		logger:        logger,
		acceptor:      acceptor,
		proposer:      proposer,
		committed:     make(map[Instance]CommittedValue),
		lastCommitted: 0,
		commitCh:      make(chan CommittedValue, 100),
		currentLeader: 0,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.NodeID))),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Let the proposer deliver our own commits to the learner. Broadcast only
	// reaches peers, so this is how a value we propose ends up in our own log.
	proposer.onCommit = n.handleCommit

	// Give the proposer our local acceptor so it counts its own vote toward
	// quorums (a quorum is a majority of all nodes, including self).
	proposer.acceptor = acceptor

	// Recover committed log from storage
	if err := n.recoverLog(); err != nil {
		cancel()
		return nil, err
	}

	return n, nil
}

// Start begins the node's operation.
func (n *Node) Start() error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return nil
	}
	n.started = true
	n.mu.Unlock()

	n.logger.Info("Node: starting",
		"nodeID", n.nodeID,
		"address", n.config.Address,
		"peers", n.config.Peers,
	)

	// Start listening
	if err := n.transport.Listen(n.config.Address); err != nil {
		return err
	}

	// Connect to peers
	for _, peer := range n.config.Peers {
		if err := n.transport.Connect(peer); err != nil {
			n.logger.Warn("Node: failed to connect to peer",
				"peer", peer,
				"error", err,
			)
			// Continue - we'll retry later
		}
	}

	// Arm the election timer so the first election waits a (randomized) timeout
	// instead of firing immediately on the zero-valued deadline.
	n.mu.Lock()
	n.resetElectionDeadline()
	n.mu.Unlock()

	// Start message handler
	n.wg.Add(1)
	go n.messageLoop()

	// Start leader election monitor
	n.wg.Add(1)
	go n.electionLoop()

	return nil
}

// Propose submits a value for consensus.
// Blocks until the value is committed or an error occurs.
func (n *Node) Propose(ctx context.Context, value []byte) (Instance, error) {
	n.mu.RLock()
	if !n.started {
		n.mu.RUnlock()
		return 0, ErrShutdown
	}
	n.mu.RUnlock()

	return n.proposer.Propose(ctx, value)
}

// Subscribe returns a channel that receives committed values.
// Values are delivered in instance order.
func (n *Node) Subscribe() <-chan CommittedValue {
	return n.commitCh
}

// GetCommitted returns the committed value for a specific instance.
func (n *Node) GetCommitted(instance Instance) (CommittedValue, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	val, ok := n.committed[instance]
	return val, ok
}

// LastCommitted returns the highest committed instance number.
func (n *Node) LastCommitted() Instance {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastCommitted
}

// IsLeader returns true if this node believes it is the leader.
func (n *Node) IsLeader() bool {
	return n.proposer.IsLeader()
}

// Close gracefully shuts down the node.
func (n *Node) Close() error {
	n.cancel()
	n.wg.Wait()

	if err := n.transport.Close(); err != nil {
		return err
	}

	if err := n.storage.Close(); err != nil {
		return err
	}

	close(n.commitCh)

	n.logger.Info("Node: shut down", "nodeID", n.nodeID)
	return nil
}

// messageLoop handles incoming messages.
func (n *Node) messageLoop() {
	defer n.wg.Done()

	messages := n.transport.Receive()

	for {
		select {
		case <-n.ctx.Done():
			return

		case env := <-messages:
			n.handleMessage(env)
		}
	}
}

// handleMessage dispatches a message to the appropriate handler.
//
// The Node is the sole reader of transport.Receive(). Phase-1/Phase-2
// responses are forwarded to the Proposer via HandleResponse, which routes
// them to the matching pending proposal's response channel.
func (n *Node) handleMessage(env MessageEnvelope) {
	switch msg := env.Message.(type) {
	case *Prepare:
		response := n.acceptor.HandlePrepare(msg)
		if err := n.transport.Send(n.ctx, env.From, response); err != nil {
			n.logger.Error("Node: failed to send response", "error", err)
		}

	case *Accept:
		response := n.acceptor.HandleAccept(msg)
		if err := n.transport.Send(n.ctx, env.From, response); err != nil {
			n.logger.Error("Node: failed to send response", "error", err)
		}

	case *Commit:
		n.handleCommit(msg)

	case *Heartbeat:
		n.handleHeartbeat(msg)

	case *Promise, *Reject, *Accepted, *Nack:
		n.proposer.HandleResponse(msg)
	}
}

// handleCommit processes a committed value.
func (n *Node) handleCommit(msg *Commit) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Already committed?
	if _, exists := n.committed[msg.Instance]; exists {
		return
	}

	cv := CommittedValue{
		Instance: msg.Instance,
		Value:    msg.Value,
		Ballot:   msg.Ballot,
	}

	n.committed[msg.Instance] = cv
	if msg.Instance > n.lastCommitted {
		n.lastCommitted = msg.Instance
	}

	// Persist to log
	entry := LogEntry{
		Instance: msg.Instance,
		Ballot:   msg.Ballot,
		Value:    msg.Value,
	}
	if err := n.storage.AppendLog(entry); err != nil {
		n.logger.Error("Node: failed to persist log entry", "error", err)
	}

	// Clean up acceptor state for this instance
	n.acceptor.ForgetInstance(msg.Instance)

	// Notify subscribers (non-blocking)
	select {
	case n.commitCh <- cv:
	default:
		n.logger.Warn("Node: commit channel full, dropping notification",
			"instance", msg.Instance,
		)
	}

	n.logger.Debug("Node: committed value",
		"instance", msg.Instance,
		"ballot", msg.Ballot,
		"valueLen", len(msg.Value),
	)
}

// handleHeartbeat processes a leader heartbeat.
func (n *Node) handleHeartbeat(msg *Heartbeat) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Accept heartbeat from higher or equal ballot
	if msg.Ballot >= n.leaderBallot {
		n.currentLeader = msg.LeaderID
		n.leaderBallot = msg.Ballot
		// We've heard from a live leader; defer any election.
		n.resetElectionDeadline()

		// If we thought we were leader, step down
		if n.proposer.IsLeader() && msg.LeaderID != n.nodeID {
			n.proposer.StepDown()
		}
	}
}

// electionLoop monitors for leader failures and triggers elections.
func (n *Node) electionLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-ticker.C:
			n.checkLeaderAndHeartbeat()
		}
	}
}

// checkLeaderAndHeartbeat either sends heartbeats (if leader) or
// checks if an election is needed.
func (n *Node) checkLeaderAndHeartbeat() {
	if n.proposer.IsLeader() {
		n.mu.RLock()
		lastCommitted := n.lastCommitted
		n.mu.RUnlock()
		n.sendHeartbeat(lastCommitted)
		return
	}

	// Not leader: start an election if our deadline has passed and one isn't
	// already running. Pushing the deadline out under the same lock prevents
	// the ticker from spawning a second election while this one is in flight.
	n.mu.Lock()
	needElection := !n.electing && time.Now().After(n.electionDeadline)
	if needElection {
		n.electing = true
		n.resetElectionDeadline()
	}
	n.mu.Unlock()

	if needElection {
		n.logger.Info("Node: leader timeout, starting election")
		go n.startElection()
	}
}

// sendHeartbeat broadcasts a leader heartbeat to the peers.
func (n *Node) sendHeartbeat(lastCommitted Instance) {
	msg := &Heartbeat{
		Ballot:       n.proposer.LeaderBallot(),
		LeaderID:     n.nodeID,
		LastInstance: lastCommitted,
	}
	if err := n.transport.Broadcast(n.ctx, msg); err != nil {
		n.logger.Error("Node: failed to send heartbeat", "error", err)
	}
}

// startElection attempts to become the leader. Only one runs at a time; the
// caller sets n.electing and this clears it on return.
func (n *Node) startElection() {
	defer func() {
		n.mu.Lock()
		n.electing = false
		n.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(n.ctx, n.config.PrepareTimeout*2)
	defer cancel()

	n.mu.RLock()
	fromInstance := n.lastCommitted + 1
	n.mu.RUnlock()

	if err := n.proposer.BecomeLeader(ctx, fromInstance); err != nil {
		n.logger.Debug("Node: failed to become leader", "error", err)
		// Back off (with fresh jitter) before the next attempt.
		n.mu.Lock()
		n.resetElectionDeadline()
		n.mu.Unlock()
		return
	}

	n.mu.Lock()
	n.currentLeader = n.nodeID
	n.leaderBallot = n.proposer.LeaderBallot()
	lastCommitted := n.lastCommitted
	n.resetElectionDeadline()
	n.mu.Unlock()

	// Assert leadership immediately so followers reset their election timers
	// before they time out and start a competing election.
	n.sendHeartbeat(lastCommitted)
}

// resetElectionDeadline pushes the next election timeout out by a randomized
// interval. Must be called with n.mu held.
func (n *Node) resetElectionDeadline() {
	n.electionDeadline = time.Now().Add(n.randomElectionTimeout())
}

// randomElectionTimeout returns a value in [ElectionTimeout, 2*ElectionTimeout).
// The randomization breaks symmetry between nodes so they don't repeatedly
// duel for leadership. Must be called with n.mu held (uses n.rng).
func (n *Node) randomElectionTimeout() time.Duration {
	base := n.config.ElectionTimeout
	return base + time.Duration(n.rng.Int63n(int64(base)))
}

// recoverLog loads committed entries from storage.
func (n *Node) recoverLog() error {
	lastInstance, err := n.storage.GetLastInstance()
	if err != nil && err != ErrNotFound {
		return err
	}

	if lastInstance == 0 {
		return nil // Empty log
	}

	entries, err := n.storage.GetLogRange(1, lastInstance+1)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		n.committed[entry.Instance] = CommittedValue{
			Instance: entry.Instance,
			Value:    entry.Value,
			Ballot:   entry.Ballot,
		}
		if entry.Instance > n.lastCommitted {
			n.lastCommitted = entry.Instance
		}
	}

	n.logger.Info("Node: recovered log",
		"entries", len(entries),
		"lastInstance", n.lastCommitted,
	)

	return nil
}
