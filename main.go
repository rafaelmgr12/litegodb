package main

import (
	"fmt"
	"log"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

func main() {
	// Create a new DiskManager
	diskManager, err := disk.NewFileDiskManager("data.db")
	if err != nil {
		log.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	// Initialize the key-value store with a degree of 2 for the B-Tree (minimalist approach)
	kvStore, err := kvstore.NewBTreeKVStore(2, diskManager, "log.db")
	if err != nil {
		log.Fatalf("Failed to create the key-value store: %v", err)
	}
	defer kvStore.Close()

	// Start periodic flushing to disk every 10 seconds
	kvStore.StartPeriodicFlush(10 * time.Second)

	// Put some key-value pairs
	log.Println("Putting key-value pairs...")
	if err := kvStore.Put(1, "value1"); err != nil {
		log.Fatalf("Failed to put key-value pair: %v", err)
	}
	if err := kvStore.Put(2, "value2"); err != nil {
		log.Fatalf("Failed to put key-value pair: %v", err)
	}

	// Get key-value pairs
	log.Println("Getting key-value pairs...")
	value, found, err := kvStore.Get(1)
	if err != nil {
		log.Fatalf("Failed to get key-value pair: %v", err)
	}
	if found {
		fmt.Printf("Key 1: %s\n", value)
	} else {
		fmt.Println("Key 1 not found")
	}

	value, found, err = kvStore.Get(3)
	if err != nil {
		log.Fatalf("Failed to get key-value pair: %v", err)
	}
	if found {
		fmt.Printf("Key 3: %s\n", value)
	} else {
		fmt.Println("Key 3 not found")
	}

	// Delete a key-value pair
	log.Println("Deleting key 2...")
	if err := kvStore.Delete(2); err != nil {
		log.Fatalf("Failed to delete key-value pair: %v", err)
	}

	// Attempt to get the deleted key
	value, found, err = kvStore.Get(2)
	if err != nil {
		log.Fatalf("Failed to get key-value pair: %v", err)
	}
	if found {
		fmt.Printf("Key 2: %s\n", value)
	} else {
		fmt.Println("Key 2 not found")
	}
}
