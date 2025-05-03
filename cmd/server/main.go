package main

import (
	"log"

	"github.com/rafaelmgr12/litegodb/internal/interfaces"
	"github.com/rafaelmgr12/litegodb/internal/server"
	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
)

func main() {
	db, cfg, err := litegodb.Open("config.yaml")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	adapted := &interfaces.AdapterDB{Impl: db}

	srv := server.NewServer(adapted, cfg)
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
