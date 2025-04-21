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

	table := "test_table"
	err := kvStore.CreateTableName(table, 3)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := kvStore.Put(table, 1, "one"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := kvStore.Put(table, 2, "two"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	assertGet(t, kvStore, table, 1, "one")
	assertGet(t, kvStore, table, 2, "two")

	if err := kvStore.Delete(table, 1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	assertNotFound(t, kvStore, table, 1)

	if err := kvStore.Flush(table); err != nil {
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
	assertGet(t, loadedStore, table, 2, "two")
}

func TestKVStore(t *testing.T) {
	kvStore, cleanup := setupTestKVStore(t)
	defer cleanup()

	table := "kvtable"
	_ = kvStore.CreateTableName(table, 3)

	t.Run("Empty Database", func(t *testing.T) {
		assertNotFound(t, kvStore, table, 100)
	})

	t.Run("Overwrite Value", func(t *testing.T) {
		_ = kvStore.Put(table, 1, "one")
		_ = kvStore.Put(table, 1, "uno")
		assertGet(t, kvStore, table, 1, "uno")
	})

	t.Run("Delete Non-Existent Key", func(t *testing.T) {
		err := kvStore.Delete(table, 999)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("Multiple Deletes", func(t *testing.T) {
		keys := []int{10, 20, 30}
		values := []string{"ten", "twenty", "thirty"}
		for i := range keys {
			_ = kvStore.Put(table, keys[i], values[i])
		}
		for _, key := range keys {
			_ = kvStore.Delete(table, key)
			assertNotFound(t, kvStore, table, key)
		}
	})
}

func TestBTreeSerializationDeserialization(t *testing.T) {
	original := btree.NewBTree(3)

	testCases := []struct {
		key   int
		value string
	}{
		{10, "ten"},
		{20, "twenty"},
		{5, "five"},
	}

	for _, tc := range testCases {
		original.Insert(tc.key, tc.value)
	}

	mockDisk := make(map[int32][]byte)

	var save func(node *btree.Node)
	save = func(node *btree.Node) {
		data, err := original.Serialize()
		if err != nil {
			t.Fatalf("failed to serialize node: %v", err)
		}
		mockDisk[node.ID()] = data
		for _, child := range node.Children() {
			save(child)
		}
	}
	save(original.Root())

	rootID := original.Root().ID()
	rootData := mockDisk[rootID]

	fetch := func(pageID int32) ([]byte, error) {
		data, ok := mockDisk[pageID]
		if !ok {
			return nil, fmt.Errorf("page %d not found", pageID)
		}
		return data, nil
	}

	recovered, err := btree.Deserialize(rootData, fetch)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	for _, tc := range testCases {
		val, found := recovered.Search(tc.key)
		if !found || val != tc.value {
			t.Errorf("expected %q for key %d, got %v", tc.value, tc.key, val)
		}
	}
}

func TestPeriodicFlush(t *testing.T) {
	diskManager, _ := disk.NewFileDiskManager("test_periodic_flush.db")
	logFile := "test_periodic_flush.log"
	store, _ := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	defer os.Remove("test_periodic_flush.db")
	defer os.Remove(logFile)
	defer store.Close()

	table := "flush_table"
	_ = store.CreateTableName(table, 3)

	store.StartPeriodicFlush(1 * time.Second)
	_ = store.Put(table, 1, "one")
	_ = store.Put(table, 2, "two")
	time.Sleep(2 * time.Second)

	recoveredStore, _ := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	defer recoveredStore.Close()
	_ = recoveredStore.Load()

	assertGet(t, recoveredStore, table, 1, "one")
	assertGet(t, recoveredStore, table, 2, "two")
}

func TestDropTable(t *testing.T) {
	store, cleanup := setupTestKVStore(t)
	defer cleanup()

	table := "drop_table"
	_ = store.CreateTableName(table, 3)

	if err := store.DropTable(table); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	if _, found, _ := store.Get(table, 1); found {
		t.Fatalf("Expected table %s to be dropped", table)
	}
}

func assertGet(t *testing.T, store *kvstore.BTreeKVStore, table string, key int, expected string) {
	value, found, err := store.Get(table, key)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if !found || value != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, value)
	}
}

func assertNotFound(t *testing.T, store *kvstore.BTreeKVStore, table string, key int) {
	value, found, err := store.Get(table, key)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key %d to be missing, found value '%s'", key, value)
	}
}
