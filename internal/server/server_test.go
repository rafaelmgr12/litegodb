package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerStart(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()

	s.pingHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.StatusCode)
	}
}
