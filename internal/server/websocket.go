package server

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type WSRequest struct {
	Op    string `json:"op"`
	Table string `json:"table"`
	Key   int    `json:"key"`
	Value string `json:"value,omitempty"`
}

type WSResponse struct {
	Status  string `json:"status"`
	Value   string `json:"value,omitempty"`
	Message string `json:"message,omitempty"`
}

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Failed WebSocket upgrade", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	for {
		var req WSRequest
		if err := conn.ReadJSON(&req); err != nil {
			log.Printf("WebSocket read error: %v", err)
			break
		}

		var resp WSResponse

		switch req.Op {
		case "put":
			err := s.DB.Put(req.Table, req.Key, req.Value)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else {
				resp = WSResponse{Status: "ok"}
			}
		case "get":
			val, found, err := s.DB.Get(req.Table, req.Key)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else if !found {
				resp = WSResponse{Status: "error", Message: "key not found"}
			} else {
				resp = WSResponse{Status: "ok", Value: val}
			}
		case "delete":
			err := s.DB.Delete(req.Table, req.Key)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else {
				resp = WSResponse{Status: "ok"}
			}
		case "ping":
			resp = WSResponse{Status: "ok", Message: "pong"}
		default:
			resp = WSResponse{Status: "error", Message: "unknown operation"}
		}

		if err := conn.WriteJSON(resp); err != nil {
			log.Printf("WebSocket write error: %v", err)
			break
		}
	}
}
