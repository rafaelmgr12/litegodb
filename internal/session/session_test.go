package session_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/session"
)

func TestSessionManager_GetOrCreate(t *testing.T) {
	sm := session.NewSessionManager()

	id := "12345"
	s := sm.GetOrCreate(id)
	if s.ID != id {
		t.Errorf("expected session ID to be %s, got %s", id, s.ID)
	}

	s2 := sm.GetOrCreate(id)
	if s != s2 {
		t.Errorf("expected to retrieve the same session instance")
	}
}

func TestSessionManager_Delete(t *testing.T) {
	sm := session.NewSessionManager()

	id := "12345"
	sm.GetOrCreate(id)

	sm.Delete(id)

	s := sm.GetOrCreate(id)
	if s.ID != id {
		t.Errorf("expected session ID to be %s, got %s", id, s.ID)
	} else if s.Transaction != nil {
		t.Errorf("expected session transaction to be nil for a new session")
	} else if !s.LastActive.IsZero() {
		t.Errorf("expected LastActive to be zero for a new session")
	}
}
