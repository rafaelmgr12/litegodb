package kvstore_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/btree"
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

func TestKVStore(t *testing.T) {
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
	})

	t.Run("Empty Database", func(t *testing.T) {
		value, found, err := kvStore.Get(100)
		if err != nil {
			t.Fatalf("Error during GET on empty database: %v", err)
		}
		if found {
			t.Fatalf("Expected no value for key 100, found '%s'", value)
		}
	})

	t.Run("Overwrite Value", func(t *testing.T) {
		if err := kvStore.Put(1, "one"); err != nil {
			t.Fatalf("Failed to put key 1: %v", err)
		}
		if err := kvStore.Put(1, "uno"); err != nil {
			t.Fatalf("Failed to overwrite key 1: %v", err)
		}
		value, found, err := kvStore.Get(1)
		if err != nil {
			t.Fatalf("Error during GET for overwritten key: %v", err)
		}
		if !found || value != "uno" {
			t.Fatalf("Expected value 'uno', got '%s'", value)
		}
	})

	t.Run("Delete Non-Existent Key", func(t *testing.T) {
		err := kvStore.Delete(999)
		if err != nil {
			t.Fatalf("Error when deleting non-existent key: %v", err)
		}
	})

	t.Run("Multiple Deletes", func(t *testing.T) {
		keys := []int{10, 20, 30}
		values := []string{"ten", "twenty", "thirty"}
		for i, key := range keys {
			if err := kvStore.Put(key, values[i]); err != nil {
				t.Fatalf("Failed to put key %d: %v", key, err)
			}
		}
		for _, key := range keys {
			if err := kvStore.Delete(key); err != nil {
				t.Fatalf("Failed to delete key %d: %v", key, err)
			}
			value, found, err := kvStore.Get(key)
			if err != nil {
				t.Fatalf("Error during GET after multiple deletes: %v", err)
			}
			if found {
				t.Fatalf("Expected key %d to be deleted, but found value '%s'", key, value)
			}
		}
	})
}

func TestBTreeNodeSerialization(t *testing.T) {
	// Mock data for testing
	mockData := make(map[int32][]byte)

	// Create a mock fetchPageData function
	fetchPageData := func(pageID int32) ([]byte, error) {
		data, exists := mockData[pageID]
		if !exists {
			return nil, fmt.Errorf("page not found: %d", pageID)
		}
		return data, nil
	}

	// Create a sample B-Tree node
	rootNode := btree.NewNodeComplete(
		1,
		[]int{10, 20, 30},
		[]interface{}{"ten", "twenty", "thirty"},
		nil,
		true,
		3,
	)

	// Serialize the node and store it in mockData
	serializedRoot, err := kvstore.SerializeNodeForTest(rootNode)
	if err != nil {
		t.Fatalf("Failed to serialize root node: %v", err)
	}
	mockData[1] = serializedRoot

	// Deserialize the node
	deserializedNode, err := kvstore.DeserializeNodeForTest(serializedRoot, fetchPageData)
	if err != nil {
		t.Fatalf("Failed to deserialize node: %v", err)
	}

	// Compare the original and deserialized nodes
	if !nodesAreEqual(rootNode, deserializedNode) {
		t.Errorf("Original and deserialized nodes do not match")
	}
}

func TestPeriodicFlush(t *testing.T) {
	diskManager, err := disk.NewFileDiskManager("test_periodic_flush.db")
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer os.Remove("test_periodic_flush.db")
	defer diskManager.Close()

	logFile := "test_periodic_flush.log"
	kvStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}
	defer os.Remove(logFile)
	defer kvStore.Close()

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
	recoveredStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
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
}

// Helper function to compare nodes
func nodesAreEqual(a, b *btree.Node) bool {
	if a.ID() != b.ID() || a.IsLeaf() != b.IsLeaf() || len(a.Keys()) != len(b.Keys()) {
		return false
	}
	for i, key := range a.Keys() {
		if key != b.Keys()[i] {
			return false
		}
	}
	for i, value := range a.Values() {
		if value != b.Values()[i] {
			return false
		}
	}
	return true
}
