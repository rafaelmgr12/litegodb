package integrations

import (
	"os"
	"testing"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

func setupKVStore(t *testing.T) (*kvstore.BTreeKVStore, func()) {
	diskManager, err := disk.NewFileDiskManager("test_data.db")
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	kvStore, err := kvstore.NewBTreeKVStore(3, diskManager, "test_log.log")
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	cleanup := func() {
		kvStore.Close()
		os.Remove("test_data.db")
		os.Remove("test_log.log")
	}

	return kvStore, cleanup
}

func TestKVStoreIntegration(t *testing.T) {
	kvStore, cleanup := setupKVStore(t)
	defer cleanup()

	table := "integration_table"
	if err := kvStore.CreateTableName(table, 3); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testBasicOperations(t, kvStore, table)
	testCrashRecovery(t, table)
	testPeriodicFlush(t, table)
}

func testBasicOperations(t *testing.T, kvStore *kvstore.BTreeKVStore, table string) {
	t.Run("Basic Operations", func(t *testing.T) {
		if err := kvStore.Put(table, 1, "one"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
		if err := kvStore.Put(table, 2, "two"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		value, found, err := kvStore.Get(table, 1)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "one" {
			t.Fatalf("Expected value 'one', got '%s'", value)
		}

		value, found, err = kvStore.Get(table, 2)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "two" {
			t.Fatalf("Expected value 'two', got '%s'", value)
		}

		if err := kvStore.Delete(table, 1); err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}
		value, found, err = kvStore.Get(table, 1)
		if err != nil {
			t.Fatalf("Error during GET after DELETE: %v", err)
		}
		if found {
			t.Fatalf("Expected key 1 to be deleted, but found value '%s'", value)
		}
	})
}

func testCrashRecovery(t *testing.T, table string) {
	t.Run("Crash Recovery", func(t *testing.T) {
		kvStore, cleanup := setupKVStore(t)
		defer cleanup()

		if err := kvStore.CreateTableName(table, 3); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		if err := kvStore.Put(table, 1, "one"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
		if err := kvStore.Put(table, 2, "two"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		kvStore.Close()

		diskManager, err := disk.NewFileDiskManager("test_data.db")
		if err != nil {
			t.Fatalf("Failed to create DiskManager: %v", err)
		}

		kvStore, err = kvstore.NewBTreeKVStore(3, diskManager, "test_log.log")
		if err != nil {
			t.Fatalf("Failed to create KVStore: %v", err)
		}
		defer kvStore.Close()

		if err := kvStore.Load(); err != nil {
			t.Fatalf("Failed to load KV store: %v", err)
		}

		value, found, err := kvStore.Get(table, 1)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "one" {
			t.Fatalf("Expected value 'one', got '%s'", value)
		}

		value, found, err = kvStore.Get(table, 2)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "two" {
			t.Fatalf("Expected value 'two', got '%s'", value)
		}
	})
}

func testPeriodicFlush(t *testing.T, table string) {
	t.Run("Periodic Flush", func(t *testing.T) {
		kvStore, cleanup := setupKVStore(t)
		defer cleanup()

		if err := kvStore.CreateTableName(table, 3); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		kvStore.StartPeriodicFlush(1 * time.Second)

		if err := kvStore.Put(table, 1, "one"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
		if err := kvStore.Put(table, 2, "two"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		time.Sleep(2 * time.Second)

		diskManager, err := disk.NewFileDiskManager("test_data.db")
		if err != nil {
			t.Fatalf("Failed to create DiskManager: %v", err)
		}
		defer diskManager.Close()

		recoveredStore, err := kvstore.NewBTreeKVStore(3, diskManager, "test_log.log")
		if err != nil {
			t.Fatalf("Failed to create recovered KVStore: %v", err)
		}
		defer recoveredStore.Close()

		if err := recoveredStore.Load(); err != nil {
			t.Fatalf("Failed to load recovered KVStore: %v", err)
		}

		value, found, err := recoveredStore.Get(table, 1)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "one" {
			t.Fatalf("Expected value 'one', got '%s'", value)
		}

		value, found, err = recoveredStore.Get(table, 2)
		if err != nil {
			t.Fatalf("Error during GET: %v", err)
		}
		if !found || value != "two" {
			t.Fatalf("Expected value 'two', got '%s'", value)
		}
	})
}
