package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
	"github.com/rs/cors"
)

var (
	db   litegodb.DB
	conf *litegodb.Config
)

func main() {
	var err error
	db, conf, err = litegodb.Open("config.yaml")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Setup routes
	mux := http.NewServeMux()
	mux.HandleFunc("/put", withAuth(putHandler))
	mux.HandleFunc("/get", withAuth(getHandler))
	mux.HandleFunc("/delete", withAuth(deleteHandler))
	mux.HandleFunc("/ping", pingHandler)
	mux.HandleFunc("/ws", wsHandler)

	handler := http.Handler(mux)
	if conf.Server.EnableCORS {
		handler = cors.Default().Handler(mux)
	}

	port := fmt.Sprintf(":%d", conf.Server.Port)
	log.Printf("LiteGoDB REST server listening at http://localhost%s\n", port)
	log.Fatal(http.ListenAndServe(port, handler))
}

func withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expected := conf.Server.AuthToken
		if expected == "" {
			next(w, r)
			return
		}
		token := r.Header.Get("Authorization")
		if token != fmt.Sprintf("Bearer %s", expected) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

type KVRequest struct {
	Table string `json:"table"`
	Key   int    `json:"key"`
	Value string `json:"value,omitempty"`
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := db.Put(req.Table, req.Key, req.Value); err != nil {
		http.Error(w, "DB Put failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	table := r.URL.Query().Get("table")
	keyStr := r.URL.Query().Get("key")

	key, err := strconv.Atoi(keyStr)
	if err != nil {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	val, found, err := db.Get(table, key)
	if err != nil {
		http.Error(w, "DB Get failed", http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Only DELETE allowed", http.StatusMethodNotAllowed)
		return
	}

	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := db.Delete(req.Table, req.Key); err != nil {
		http.Error(w, "DB Delete failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Failed to upgrade to WebSocket", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	for {
		var req WSRequest
		if err := conn.ReadJSON(&req); err != nil {
			log.Printf("read error: %v", err)
			break
		}

		var resp WSResponse

		switch req.Op {
		case "put":
			err := db.Put(req.Table, req.Key, req.Value)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else {
				resp = WSResponse{Status: "ok"}
			}
		case "get":
			val, found, err := db.Get(req.Table, req.Key)
			if err != nil {
				resp = WSResponse{Status: "error", Message: err.Error()}
			} else if !found {
				resp = WSResponse{Status: "error", Message: "key not found"}
			} else {
				resp = WSResponse{Status: "ok", Value: val}
			}
		case "delete":
			err := db.Delete(req.Table, req.Key)
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
			log.Printf("write error: %v", err)
			break
		}
	}
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
