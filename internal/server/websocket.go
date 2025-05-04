package server

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rafaelmgr12/litegodb/internal/session"
	"github.com/rafaelmgr12/litegodb/internal/sqlparser"
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

	// Register connection
	s.connMutex.Lock()
	s.connections[conn] = true
	s.connMutex.Unlock()

	sess := session.NewSessionManager().GetOrCreate(conn.RemoteAddr().String())

	defer func() {
		// Unregister connection
		s.connMutex.Lock()
		delete(s.connections, conn)
		s.connMutex.Unlock()

		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "closing connection"))
		time.Sleep(1 * time.Second)
		conn.Close()
	}()

	for {
		var req WSRequest
		if err := conn.ReadJSON(&req); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("WebSocket closed normally: %v", err)
			} else if websocket.IsUnexpectedCloseError(err, websocket.CloseAbnormalClosure) {
				log.Printf("Unexpected WebSocket close, gracefully shutdonw: %v", err)
			} else {
				log.Printf("Other WebSocket close error: %v", err)
			}
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
		case "update":
			err := s.DB.Update(req.Table, req.Key, req.Value)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else {
				resp = WSResponse{Status: "ok"}
			}
		case "sql":
			res, err := sqlparser.ParseAndExecute(req.Value, s.DB, sess)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else {
				if strRes, ok := res.(string); ok {
					resp = WSResponse{Status: "ok", Value: strRes}
				} else {
					resp = WSResponse{Status: "error", Message: "unexpected result type"}
				}
			}
		default:
			resp = WSResponse{Status: "error", Message: "unknown operation"}
		}

		if err := conn.WriteJSON(resp); err != nil {
			log.Printf("WebSocket write error: %v", err)
			break
		}
	}
}
