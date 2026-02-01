// Package storage provides storage backends for Paxos persistence.
package storage

import (
	"sync"

	"github.com/jtomecek/go-paxos/paxos"
)

// MemoryStorage is an in-memory storage implementation.
// NOT suitable for production - data is lost on restart.
// Useful for testing and development.
type MemoryStorage struct {
	mu sync.RWMutex

	acceptorState paxos.AcceptorState
	log           map[paxos.Instance]paxos.LogEntry
	lastInstance  paxos.Instance
	snapshot      *paxos.Snapshot

	closed bool
}

// NewMemoryStorage creates a new in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		acceptorState: paxos.AcceptorState{
			PromisedBallot: make(map[paxos.Instance]paxos.Ballot),
			AcceptedBallot: make(map[paxos.Instance]paxos.Ballot),
			AcceptedValue:  make(map[paxos.Instance]paxos.Value),
		},
		log: make(map[paxos.Instance]paxos.LogEntry),
	}
}

// SaveAcceptorState persists the acceptor's state.
func (s *MemoryStorage) SaveAcceptorState(state paxos.AcceptorState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return paxos.ErrStorageClosed
	}

	// Deep copy the maps
	s.acceptorState.PromisedBallot = make(map[paxos.Instance]paxos.Ballot)
	for k, v := range state.PromisedBallot {
		s.acceptorState.PromisedBallot[k] = v
	}

	s.acceptorState.AcceptedBallot = make(map[paxos.Instance]paxos.Ballot)
	for k, v := range state.AcceptedBallot {
		s.acceptorState.AcceptedBallot[k] = v
	}

	s.acceptorState.AcceptedValue = make(map[paxos.Instance]paxos.Value)
	for k, v := range state.AcceptedValue {
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		s.acceptorState.AcceptedValue[k] = valueCopy
	}

	return nil
}

// LoadAcceptorState retrieves the acceptor's persisted state.
func (s *MemoryStorage) LoadAcceptorState() (paxos.AcceptorState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return paxos.AcceptorState{}, paxos.ErrStorageClosed
	}

	if len(s.acceptorState.PromisedBallot) == 0 {
		return paxos.AcceptorState{}, paxos.ErrNotFound
	}

	// Deep copy
	state := paxos.AcceptorState{
		PromisedBallot: make(map[paxos.Instance]paxos.Ballot),
		AcceptedBallot: make(map[paxos.Instance]paxos.Ballot),
		AcceptedValue:  make(map[paxos.Instance]paxos.Value),
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
func (s *MemoryStorage) AppendLog(entry paxos.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return paxos.ErrStorageClosed
	}

	// Copy the value
	entryCopy := paxos.LogEntry{
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
func (s *MemoryStorage) GetLog(instance paxos.Instance) (paxos.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return paxos.LogEntry{}, paxos.ErrStorageClosed
	}

	entry, ok := s.log[instance]
	if !ok {
		return paxos.LogEntry{}, paxos.ErrNotFound
	}

	return entry, nil
}

// GetLogRange retrieves log entries in a range [from, to).
func (s *MemoryStorage) GetLogRange(from, to paxos.Instance) ([]paxos.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, paxos.ErrStorageClosed
	}

	entries := make([]paxos.LogEntry, 0)
	for i := from; i < to; i++ {
		if entry, ok := s.log[i]; ok {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// GetLastInstance returns the highest instance number in the log.
func (s *MemoryStorage) GetLastInstance() (paxos.Instance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, paxos.ErrStorageClosed
	}

	if s.lastInstance == 0 {
		return 0, paxos.ErrNotFound
	}

	return s.lastInstance, nil
}

// SaveSnapshot persists a snapshot.
func (s *MemoryStorage) SaveSnapshot(snap paxos.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return paxos.ErrStorageClosed
	}

	// Copy snapshot data
	snapCopy := paxos.Snapshot{
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
func (s *MemoryStorage) LoadSnapshot() (paxos.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return paxos.Snapshot{}, paxos.ErrStorageClosed
	}

	if s.snapshot == nil {
		return paxos.Snapshot{}, paxos.ErrNotFound
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
var _ paxos.Storage = (*MemoryStorage)(nil)
