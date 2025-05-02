package kvstore_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
	"github.com/stretchr/testify/assert"
)

func setupTestKV(t *testing.T) *kvstore.BTreeKVStore {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := tempDir + "/data.db"
	logPath := tempDir + "/log.db"

	dm, err := disk.NewFileDiskManager(dbPath)
	assert.NoError(t, err)

	kv, err := kvstore.NewBTreeKVStore(2, dm, logPath)
	assert.NoError(t, err)

	// Use defer-safe CreateTable
	_ = kv.DropTable("users") // Avoid conflicts
	_ = kv.CreateTableName("users", 2)

	return kv
}

func TestTransaction_Commit_Success(t *testing.T) {
	kv := setupTestKV(t)

	tx := kv.BeginTransaction()
	tx.PutBatch("users", 1, "rafael")
	tx.PutBatch("users", 2, "joao")

	err := tx.Commit()
	assert.NoError(t, err)

	v1, found, _ := kv.Get("users", 1)
	assert.True(t, found)
	assert.Equal(t, "rafael", v1)

	v2, found, _ := kv.Get("users", 2)
	assert.True(t, found)
	assert.Equal(t, "joao", v2)
}

func TestTransaction_Rollback_BeforeCommit(t *testing.T) {
	kv := setupTestKV(t)

	tx := kv.BeginTransaction()
	tx.PutBatch("users", 1, "temp")
	tx.Rollback()

	_, found, _ := kv.Get("users", 1)
	assert.False(t, found)
}

func TestTransaction_Commit_FailureWithRollback(t *testing.T) {
	kv := setupTestKV(t)

	// Insert an initial value to test rollback
	_ = kv.Put("users", 1, "original")

	tx := kv.BeginTransaction()
	tx.PutBatch("users", 1, "modified")
	tx.PutBatch("nonexistent", 2, "invalid") // This will fail

	err := tx.Commit()
	assert.Error(t, err)

	// Value should be rolled back to original
	v, found, _ := kv.Get("users", 1)
	assert.True(t, found)
	assert.Equal(t, "original", v)

	// The invalid table should not exist
	_, found, _ = kv.Get("nonexistent", 2)
	assert.False(t, found)
}

func TestTransaction_DeleteBatch_Rollback(t *testing.T) {
	kv := setupTestKV(t)
	_ = kv.Put("users", 1, "keepme")

	tx := kv.BeginTransaction()
	tx.DeleteBatch("users", 1)
	tx.Rollback()

	v, found, _ := kv.Get("users", 1)
	assert.True(t, found)
	assert.Equal(t, "keepme", v)
}
