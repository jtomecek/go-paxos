package paxos

import "sync"

// MemoryStorage is an in-memory storage implementation.
// NOT suitable for production - data is lost on restart.
// Useful for testing and development.
type MemoryStorage struct {
	mu sync.RWMutex

	acceptorState AcceptorState
	log           map[Instance]LogEntry
	lastInstance  Instance
	snapshot      *Snapshot

	closed bool
}

// NewMemoryStorage creates a new in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		acceptorState: AcceptorState{
			PromisedBallot: make(map[Instance]Ballot),
			AcceptedBallot: make(map[Instance]Ballot),
			AcceptedValue:  make(map[Instance]Value),
		},
		log: make(map[Instance]LogEntry),
	}
}

// SaveAcceptorState persists the acceptor's state.
func (s *MemoryStorage) SaveAcceptorState(state AcceptorState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	// Deep copy the maps
	s.acceptorState.PromisedBallot = make(map[Instance]Ballot)
	for k, v := range state.PromisedBallot {
		s.acceptorState.PromisedBallot[k] = v
	}

	s.acceptorState.AcceptedBallot = make(map[Instance]Ballot)
	for k, v := range state.AcceptedBallot {
		s.acceptorState.AcceptedBallot[k] = v
	}

	s.acceptorState.AcceptedValue = make(map[Instance]Value)
	for k, v := range state.AcceptedValue {
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		s.acceptorState.AcceptedValue[k] = valueCopy
	}

	return nil
}

// LoadAcceptorState retrieves the acceptor's persisted state.
func (s *MemoryStorage) LoadAcceptorState() (AcceptorState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return AcceptorState{}, ErrStorageClosed
	}

	if len(s.acceptorState.PromisedBallot) == 0 &&
		len(s.acceptorState.AcceptedBallot) == 0 {
		return AcceptorState{}, ErrNotFound
	}

	// Deep copy
	state := AcceptorState{
		PromisedBallot: make(map[Instance]Ballot),
		AcceptedBallot: make(map[Instance]Ballot),
		AcceptedValue:  make(map[Instance]Value),
	}

	for k, v := range s.acceptorState.PromisedBallot {
		state.PromisedBallot[k] = v
	}
	for k, v := range s.acceptorState.AcceptedBallot {
		state.AcceptedBallot[k] = v
	}
	for k, v := range s.acceptorState.AcceptedValue {
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		state.AcceptedValue[k] = valueCopy
	}

	return state, nil
}

// AppendLog adds a committed entry to the log.
func (s *MemoryStorage) AppendLog(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	// Copy the value
	entryCopy := LogEntry{
		Instance: entry.Instance,
		Ballot:   entry.Ballot,
		Value:    make([]byte, len(entry.Value)),
	}
	copy(entryCopy.Value, entry.Value)

	s.log[entry.Instance] = entryCopy

	if entry.Instance > s.lastInstance {
		s.lastInstance = entry.Instance
	}

	return nil
}

// GetLog retrieves a log entry by instance number.
func (s *MemoryStorage) GetLog(instance Instance) (LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return LogEntry{}, ErrStorageClosed
	}

	entry, ok := s.log[instance]
	if !ok {
		return LogEntry{}, ErrNotFound
	}

	return entry, nil
}

// GetLogRange retrieves log entries in a range [from, to).
func (s *MemoryStorage) GetLogRange(from, to Instance) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	entries := make([]LogEntry, 0)
	for i := from; i < to; i++ {
		if entry, ok := s.log[i]; ok {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// GetLastInstance returns the highest instance number in the log.
func (s *MemoryStorage) GetLastInstance() (Instance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, ErrStorageClosed
	}

	if s.lastInstance == 0 {
		return 0, ErrNotFound
	}

	return s.lastInstance, nil
}

// SaveSnapshot persists a snapshot.
func (s *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	// Copy snapshot data
	snapCopy := Snapshot{
		LastInstance: snap.LastInstance,
		Data:         make([]byte, len(snap.Data)),
		Checksum:     make([]byte, len(snap.Checksum)),
	}
	copy(snapCopy.Data, snap.Data)
	copy(snapCopy.Checksum, snap.Checksum)

	s.snapshot = &snapCopy

	return nil
}

// LoadSnapshot retrieves the latest snapshot.
func (s *MemoryStorage) LoadSnapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return Snapshot{}, ErrStorageClosed
	}

	if s.snapshot == nil {
		return Snapshot{}, ErrNotFound
	}

	return *s.snapshot, nil
}

// Close releases resources.
func (s *MemoryStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return nil
}

// Verify interface compliance
var _ Storage = (*MemoryStorage)(nil)
