package server

import (
	"fmt"
	"log"
	"net/http"

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

	log.Printf("Starting server on %s...", addr)
	return http.ListenAndServe(addr, handler)
}
