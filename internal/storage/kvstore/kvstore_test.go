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

const (
	dbFile  = "test_kvstore.db"
	logFile = "test_kvstore.log"
)

func setupTestKVStore(t *testing.T) (*kvstore.BTreeKVStore, func()) {
	diskManager, err := disk.NewFileDiskManager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	store, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	cleanup := func() {
		store.Close()
		diskManager.Close()
		os.Remove(dbFile)
		os.Remove(logFile)
	}

	return store, cleanup
}

func TestKVStoreBasicOperations(t *testing.T) {
	kvStore, cleanup := setupTestKVStore(t)
	defer cleanup()

	// PUT
	if err := kvStore.Put(1, "one"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := kvStore.Put(2, "two"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// GET
	assertGet(t, kvStore, 1, "one")
	assertGet(t, kvStore, 2, "two")

	// DELETE
	if err := kvStore.Delete(1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	assertNotFound(t, kvStore, 1)

	// FLUSH and RELOAD
	if err := kvStore.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	diskManager, _ := disk.NewFileDiskManager(dbFile)
	loadedStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	if err != nil {
		t.Fatalf("Failed to reload store: %v", err)
	}
	defer loadedStore.Close()
	if err := loadedStore.Load(); err != nil {
		t.Fatalf("Failed to load WAL: %v", err)
	}
	assertGet(t, loadedStore, 2, "two")
}

func TestKVStore(t *testing.T) {
	kvStore, cleanup := setupTestKVStore(t)
	defer cleanup()

	t.Run("Empty Database", func(t *testing.T) {
		assertNotFound(t, kvStore, 100)
	})

	t.Run("Overwrite Value", func(t *testing.T) {
		_ = kvStore.Put(1, "one")
		_ = kvStore.Put(1, "uno")
		assertGet(t, kvStore, 1, "uno")
	})

	t.Run("Delete Non-Existent Key", func(t *testing.T) {
		err := kvStore.Delete(999)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("Multiple Deletes", func(t *testing.T) {
		keys := []int{10, 20, 30}
		values := []string{"ten", "twenty", "thirty"}
		for i := range keys {
			_ = kvStore.Put(keys[i], values[i])
		}
		for _, key := range keys {
			_ = kvStore.Delete(key)
			assertNotFound(t, kvStore, key)
		}
	})
}

func TestBTreeNodeSerialization(t *testing.T) {
	mockData := make(map[int32][]byte)
	fetchPageData := func(pageID int32) ([]byte, error) {
		data, ok := mockData[pageID]
		if !ok {
			return nil, fmt.Errorf("page not found: %d", pageID)
		}
		return data, nil
	}

	node := btree.NewNodeComplete(1, []int{10, 20, 30}, []interface{}{"ten", "twenty", "thirty"}, nil, true, 3)
	serialized, _ := kvstore.SerializeNodeForTest(node)
	mockData[1] = serialized

	deserialized, _ := kvstore.DeserializeNodeForTest(serialized, fetchPageData)
	if !nodesAreEqual(node, deserialized) {
		t.Errorf("Node mismatch")
	}
}

func TestPeriodicFlush(t *testing.T) {
	diskManager, _ := disk.NewFileDiskManager("test_periodic_flush.db")
	logFile := "test_periodic_flush.log"
	store, _ := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	defer os.Remove("test_periodic_flush.db")
	defer os.Remove(logFile)
	defer store.Close()

	store.StartPeriodicFlush(1 * time.Second)
	_ = store.Put(1, "one")
	_ = store.Put(2, "two")
	time.Sleep(2 * time.Second)

	recoveredStore, _ := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	defer recoveredStore.Close()
	_ = recoveredStore.Load()

	assertGet(t, recoveredStore, 1, "one")
	assertGet(t, recoveredStore, 2, "two")
}

func assertGet(t *testing.T, store *kvstore.BTreeKVStore, key int, expected string) {
	value, found, err := store.Get(key)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if !found || value != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, value)
	}
}

func assertNotFound(t *testing.T, store *kvstore.BTreeKVStore, key int) {
	value, found, err := store.Get(key)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key %d to be missing, found value '%s'", key, value)
	}
}

func nodesAreEqual(a, b *btree.Node) bool {
	if a.ID() != b.ID() || a.IsLeaf() != b.IsLeaf() || len(a.Keys()) != len(b.Keys()) {
		return false
	}
	for i, key := range a.Keys() {
		if key != b.Keys()[i] {
			return false
		}
	}
	for i, val := range a.Values() {
		if val != b.Values()[i] {
			return false
		}
	}
	return true
}
