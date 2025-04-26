package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
	"github.com/rs/cors"
)

type Server struct {
	DB  litegodb.DB
	Cfg *litegodb.Config
	mux *http.ServeMux
}

func NewServer(db litegodb.DB, cfg *litegodb.Config) *Server {
	s := &Server{
		DB:  db,
		Cfg: cfg,
		mux: http.NewServeMux(),
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

func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.Cfg.Server.Port)
	handler := http.Handler(s.mux)

	if s.Cfg.Server.EnableCORS {
		handler = cors.Default().Handler(handler)
	}

	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Gracefully shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	if err := s.DB.Close(); err != nil {
		log.Printf("Error closing DB: %v", err)
	}

	log.Println("Server exited properly")
	return nil
}
