package session

import (
	"sync"

	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

// Session represents a user session, which may include an active transaction.
// Each session is identified by a unique ID and can hold a reference to a transaction.
type Session struct {
	ID          string
	Transaction *kvstore.Transaction
}

// SessionManager manages multiple user sessions, allowing for the creation,
// retrieval, and deletion of sessions. It ensures thread-safe access to sessions.
type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
	}
}

func (sm *SessionManager) GetOrCreate(id string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if s, ok := sm.sessions[id]; ok {
		return s
	}
	session := &Session{ID: id}
	sm.sessions[id] = session
	return session
}

func (sm *SessionManager) Delete(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, id)
}
