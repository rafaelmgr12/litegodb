package integrations

import (
	"os"
	"testing"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

func setupKVStore(t *testing.T) (*kvstore.BTreeKVStore, func()) {
	// Setup disk manager
	diskManager, err := disk.NewFileDiskManager("test_data.db")
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	// Initialize key-value store
	kvStore, err := kvstore.NewBTreeKVStore(3, diskManager, "test_log.db")
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	// Define cleanup function
	cleanup := func() {
		kvStore.Close()
		os.Remove("test_data.db")
		os.Remove("test_log.db")
	}

	return kvStore, cleanup
}

func TestKVStoreIntegration(t *testing.T) {
	kvStore, cleanup := setupKVStore(t)
	defer cleanup()

	// Test basic operations
	testBasicOperations(t, kvStore)
	// Test crash recovery
	testCrashRecovery(t)
	// Test periodic flush
	testPeriodicFlush(t)
}

func testBasicOperations(t *testing.T, kvStore *kvstore.BTreeKVStore) {
	t.Run("Basic Operations", func(t *testing.T) {
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
	})
}

func testCrashRecovery(t *testing.T) {
	t.Run("Crash Recovery", func(t *testing.T) {
		kvStore, cleanup := setupKVStore(t)
		defer cleanup()

		// Insert some key-value pairs
		if err := kvStore.Put(1, "one"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
		if err := kvStore.Put(2, "two"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		// Simulate crash by closing the store without flushing
		kvStore.Close()

		// Reinitialize the store and load data from the log
		diskManager, err := disk.NewFileDiskManager("test_data.db")
		if err != nil {
			t.Fatalf("Failed to create DiskManager: %v", err)
		}

		kvStore, err = kvstore.NewBTreeKVStore(3, diskManager, "test_log.db")
		if err != nil {
			t.Fatalf("Failed to create KVStore: %v", err)
		}
		defer kvStore.Close()

		if err := kvStore.Load(); err != nil {
			t.Fatalf("Failed to load KV store: %v", err)
		}

		// Verify data is consistent
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
	})
}

func testPeriodicFlush(t *testing.T) {
	t.Run("Periodic Flush", func(t *testing.T) {
		kvStore, cleanup := setupKVStore(t)
		defer cleanup()

		// Start periodic flushing every second
		kvStore.StartPeriodicFlush(1 * time.Second)

		// Insert some data
		if err := kvStore.Put(1, "one"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
		if err := kvStore.Put(2, "two"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		// Wait for at least one flush to occur
		time.Sleep(2 * time.Second)

		// Simulate restarting the KVStore
		diskManager, err := disk.NewFileDiskManager("test_data.db")
		if err != nil {
			t.Fatalf("Failed to create DiskManager: %v", err)
		}
		defer diskManager.Close()

		recoveredStore, err := kvstore.NewBTreeKVStore(3, diskManager, "test_log.db")
		if err != nil {
			t.Fatalf("Failed to create recovered KVStore: %v", err)
		}
		defer recoveredStore.Close()

		if err := recoveredStore.Load(); err != nil {
			t.Fatalf("Failed to load recovered KVStore: %v", err)
		}

		// Verify data is consistent
		value, found, err := recoveredStore.Get(1)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "one" {
			t.Fatalf("Expected value 'one', got '%s'", value)
		}

		value, found, err = recoveredStore.Get(2)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "two" {
			t.Fatalf("Expected value 'two', got '%s'", value)
		}
	})
}
