package integrations

import (
	"os"
	"testing"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
	"github.com/stretchr/testify/require"
)

func setupKVStore(t *testing.T, dbFile, logFile string) (*kvstore.BTreeKVStore, func()) {
	_ = os.Remove(dbFile)
	_ = os.Remove(logFile)

	diskManager, err := disk.NewFileDiskManager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	kvStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	cleanup := func() {
		kvStore.Close()
		diskManager.Close()
		os.Remove(dbFile)
		os.Remove(logFile)
	}

	return kvStore, cleanup
}

func TestKVStoreIntegration(t *testing.T) {
	dbFile := "test_integration.db"
	logFile := "test_integration.log"
	kvStore, cleanup := setupKVStore(t, dbFile, logFile)
	defer cleanup()

	table := "integration_table"

	if err := kvStore.CreateTableName(table, 3); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testTransactionIntegration(t, kvStore, table+"_tx")
	testBasicOperations(t, kvStore, table)
	testCrashRecovery(t, dbFile, logFile, table+"_crash")
	testPeriodicFlush(t, dbFile, logFile, table+"_flush")
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

		kvStore.Close()
		kvStore.DropTable(table)
	})
}

func testCrashRecovery(t *testing.T, dbFile, logFile, table string) {
	t.Run("Crash Recovery", func(t *testing.T) {
		kvStore, cleanup := setupKVStore(t, dbFile, logFile)
		defer cleanup()

		_ = kvStore.CreateTableName(table, 3)
		_ = kvStore.Put(table, 1, "one")
		_ = kvStore.Put(table, 2, "two")

		kvStore.Close()

		diskManager, err := disk.NewFileDiskManager(dbFile)
		require.NoError(t, err)

		kvStore, err = kvstore.NewBTreeKVStore(3, diskManager, logFile)
		require.NoError(t, err)
		defer kvStore.Close()

		require.NoError(t, kvStore.Load())

		assertGet(t, kvStore, table, 1, "one")
		assertGet(t, kvStore, table, 2, "two")
	})
}

func testPeriodicFlush(t *testing.T, dbFile, logFile, table string) {
	t.Run("Periodic Flush", func(t *testing.T) {
		kvStore, cleanup := setupKVStore(t, dbFile, logFile)
		defer cleanup()

		_ = kvStore.CreateTableName(table, 3)
		kvStore.StartPeriodicFlush(1 * time.Second)

		_ = kvStore.Put(table, 1, "one")
		_ = kvStore.Put(table, 2, "two")
		time.Sleep(2 * time.Second)

		diskManager, err := disk.NewFileDiskManager(dbFile)
		require.NoError(t, err)
		defer diskManager.Close()

		recoveredStore, err := kvstore.NewBTreeKVStore(3, diskManager, logFile)
		require.NoError(t, err)
		defer recoveredStore.Close()

		require.NoError(t, recoveredStore.Load())

		assertGet(t, recoveredStore, table, 1, "one")
		assertGet(t, recoveredStore, table, 2, "two")
	})
}

func testTransactionIntegration(t *testing.T, kvStore *kvstore.BTreeKVStore, table string) {
	t.Run("Transaction Integration", func(t *testing.T) {
		// Ensure the table is created
		err := kvStore.CreateTableName(table, 3)
		require.NoError(t, err)

		// Begin a transaction and commit it
		tx := kvStore.BeginTransaction()
		tx.PutBatch(table, 1, "one")
		tx.PutBatch(table, 2, "two")

		// Commit the transaction
		err = tx.Commit()
		require.NoError(t, err)

		// Verify the committed data
		assertGet(t, kvStore, table, 1, "one")
		assertGet(t, kvStore, table, 2, "two")

		// Begin another transaction and roll it back
		tx = kvStore.BeginTransaction()
		tx.PutBatch(table, 3, "three")
		tx.Rollback()

		// Verify that the rolled-back data is not present
		_, found, err := kvStore.Get(table, 3)
		require.NoError(t, err)
		require.False(t, found)

		// Ensure the kvStore is not closed prematurely
		require.NotNil(t, kvStore)
	})
}

func assertGet(t *testing.T, kv *kvstore.BTreeKVStore, table string, key int, expected string) {
	val, found, err := kv.Get(table, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, expected, val)
}
