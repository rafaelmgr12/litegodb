package kvstore_test

import (
	"os"
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

func TestKVStoreBasic(t *testing.T) {
	diskManager, err := disk.NewFileDiskManager("test_kvstore.db")
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer os.Remove("test_kvstore.db")
	defer diskManager.Close()

	logFile := "test_kvstore.log"
	kvStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}
	defer os.Remove(logFile)
	defer kvStore.Close()

	// Test PUT operation
	if err := kvStore.Put(1, "one"); err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}
	if err := kvStore.Put(2, "two"); err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Test GET operation
	value, found, err := kvStore.Get(1)
	if err != nil {
		t.Fatalf("Error during GET: %v", err)
	}
	if !found || value != "one" {
		t.Fatalf("Expected value 'one', got '%s'", value)
	}

	value, found, err = kvStore.Get(2)
	if err != nil {
		t.Fatalf("Error during GET: %v", err)
	}
	if !found || value != "two" {
		t.Fatalf("Expected value 'two', got '%s'", value)
	}

	// Test DELETE operation
	if err := kvStore.Delete(1); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
	value, found, err = kvStore.Get(1)
	if err != nil {
		t.Fatalf("Error during GET after DELETE: %v", err)
	}
	if found {
		t.Fatalf("Expected key 1 to be deleted, but found value '%s'", value)
	}

	// Test persistence (Flush and Load)
	if err := kvStore.Flush(); err != nil {
		t.Fatalf("Failed to flush KVStore: %v", err)
	}

	loadedKVStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	if err != nil {
		t.Fatalf("Failed to load KVStore: %v", err)
	}
	if err := loadedKVStore.Load(); err != nil {
		t.Fatalf("Failed to load KVStore from log: %v", err)
	}

	value, found, err = loadedKVStore.Get(2)
	if err != nil {
		t.Fatalf("Error during GET on loaded KVStore: %v", err)
	}
	if !found || value != "two" {
		t.Fatalf("Expected value 'two' in loaded KVStore, got '%s'", value)
	}
}
