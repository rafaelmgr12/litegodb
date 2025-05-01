package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/rafaelmgr12/litegodb/internal/sqlparser"
)

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expected := s.Cfg.Server.AuthToken
		if expected == "" {
			next(w, r)
			return
		}
		token := r.Header.Get("Authorization")
		if token != "Bearer "+expected {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) pingHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}

type KVRequest struct {
	Table string `json:"table"`
	Key   int    `json:"key"`
	Value string `json:"value,omitempty"`
}

func (s *Server) putHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := s.DB.Put(req.Table, req.Key, req.Value); err != nil {
		http.Error(w, "Put failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	table := r.URL.Query().Get("table")
	keyStr := r.URL.Query().Get("key")

	key, err := strconv.Atoi(keyStr)
	if err != nil {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	val, found, err := s.DB.Get(table, key)
	if err != nil {
		http.Error(w, "DB Get error", http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := s.DB.Delete(req.Table, req.Key); err != nil {
		http.Error(w, "Delete failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) sqlHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		http.Error(w, "Empty query", http.StatusBadRequest)
		return
	}

	result, err := sqlparser.ParseAndExecute(req.Query, s.DB)
	if err != nil {
		http.Error(w, "SQL execution error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if result == nil {
		http.Error(w, "No result", http.StatusNotFound)
		return
	}

	resp := map[string]interface{}{
		"status": "ok",
		"result": result,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)

}
