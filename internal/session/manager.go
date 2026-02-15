// Package session manages multiplexed sessions over the forward/reverse channels.
package session

import (
	"log"
	"sync"
	"sync/atomic"
)

// Session represents a single proxied connection (e.g., one SSH or VNC session).
type Session struct {
	ID      uint32
	Name    string       // Service name this session is associated with
	DataCh  chan []byte   // Incoming data from the remote side
	Done    chan struct{} // Closed when session ends
	once    sync.Once
}

// Close signals that this session is done.
func (s *Session) Close() {
	s.once.Do(func() {
		close(s.Done)
	})
}

// Manager tracks all active sessions and dispatches incoming data.
type Manager struct {
	mu       sync.RWMutex
	sessions map[uint32]*Session
	nextID   atomic.Uint32
}

// NewManager creates a new session manager.
func NewManager() *Manager {
	return &Manager{
		sessions: make(map[uint32]*Session),
	}
}

// NewSession creates and registers a new session.
func (m *Manager) NewSession(name string) *Session {
	id := m.nextID.Add(1)
	s := &Session{
		ID:     id,
		Name:   name,
		DataCh: make(chan []byte, 256),
		Done:   make(chan struct{}),
	}
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	log.Printf("[session] new session %d for service %q", id, name)
	return s
}

// RegisterSession registers a session with a specific ID (used by the outer proxy
// when it receives an OPEN frame).
func (m *Manager) RegisterSession(id uint32, name string) *Session {
	s := &Session{
		ID:     id,
		Name:   name,
		DataCh: make(chan []byte, 256),
		Done:   make(chan struct{}),
	}
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	log.Printf("[session] registered session %d for service %q", id, name)
	return s
}

// Get returns a session by ID, or nil if not found.
func (m *Manager) Get(id uint32) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[id]
}

// Remove removes a session from the manager.
func (m *Manager) Remove(id uint32) {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	if ok {
		s.Close()
		log.Printf("[session] removed session %d", id)
	}
}

// Dispatch sends data to the specified session's data channel.
// Returns false if the session doesn't exist or is full.
func (m *Manager) Dispatch(id uint32, data []byte) bool {
	m.mu.RLock()
	s, ok := m.sessions[id]
	m.mu.RUnlock()
	if !ok {
		return false
	}

	select {
	case s.DataCh <- data:
		return true
	case <-s.Done:
		return false
	}
}

// Count returns the number of active sessions.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// CloseAll closes all active sessions.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	for id, s := range m.sessions {
		s.Close()
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	log.Printf("[session] all sessions closed")
}
