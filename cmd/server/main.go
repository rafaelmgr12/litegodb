package main

import (
	"log"

	"github.com/rafaelmgr12/litegodb/internal/server"
	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
)

func main() {

	db, cfg, err := litegodb.Open("config.yaml")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	server := server.NewServer(db, cfg)

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
