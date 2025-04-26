package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
	"github.com/rs/cors"
)

type Server struct {
	DB          litegodb.DB
	Cfg         *litegodb.Config
	mux         *http.ServeMux
	connections map[*websocket.Conn]bool
	connMutex   sync.Mutex
}

func NewServer(db litegodb.DB, cfg *litegodb.Config) *Server {
	s := &Server{
		DB:          db,
		Cfg:         cfg,
		mux:         http.NewServeMux(),
		connections: make(map[*websocket.Conn]bool),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/ping", s.pingHandler)
	s.mux.HandleFunc("/put", s.withAuth(s.putHandler))
	s.mux.HandleFunc("/get", s.withAuth(s.getHandler))
	s.mux.HandleFunc("/delete", s.withAuth(s.deleteHandler))
	s.mux.HandleFunc("/ws", s.wsHandler)
}

func (s *Server) setupHandler() http.Handler {
	handler := http.Handler(s.mux)
	if s.Cfg.Server.EnableCORS {
		handler = cors.Default().Handler(handler)
	}
	return handler
}

func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.Cfg.Server.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.setupHandler(),
	}

	go func() {
		log.Printf("LiteGoDB server listening on %s...", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Gracefully shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First close WebSocket connections
	s.shutdownConnections()

	// Then shutdown HTTP server
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("HTTP server shutdown error: %v", err)
	}

	// Finally close the database
	if err := s.DB.Close(); err != nil {
		log.Printf("Database close error: %v", err)
	}

	log.Println("Server shutdown complete.")
	return nil
}

func (s *Server) shutdownConnections() {
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	for conn := range s.connections {
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutdown"))
		conn.Close()
	}
	log.Printf("Closed %d WebSocket connections", len(s.connections))
}
