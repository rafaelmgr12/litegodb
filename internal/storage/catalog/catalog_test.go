package catalog_test

import (
	"os"
	"testing"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/catalog"
	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDBFile = "test.db"

func setupCatalog(t *testing.T) (*catalog.Catalog, func()) {
	dm, err := disk.NewFileDiskManager(testDBFile)
	require.NoError(t, err)

	cat := catalog.NewCatalog(dm)
	require.NoError(t, err)

	cleanup := func() {
		err := dm.Close()
		require.NoError(t, err)
		err = os.Remove(testDBFile)
		require.NoError(t, err)
	}

	return cat, cleanup

}

func TestCatalog_CreateAndGetTable(t *testing.T) {

	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	meta, ok := cat.Get("users")
	require.True(t, ok)
	assert.Equal(t, "users", meta.Name)
	assert.Equal(t, int32(3), meta.Degree)
	assert.Equal(t, int32(1), meta.RootID)
}

func TestCatalog_ListTables(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	_ = cat.CreateTable("users", 3, 1)
	_ = cat.CreateTable("products", 4, 10)

	list := cat.List()
	assert.ElementsMatch(t, list, []string{"users", "products"})
}

func TestCatalog_DuplicateTable(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	err = cat.CreateTable("users", 4, 2)
	require.Error(t, err)
	assert.Equal(t, "table users already exists", err.Error())
}

func TestCatalog_DropTable(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	err = cat.DropTable("users")
	require.NoError(t, err)

	_, ok := cat.Get("users")
	assert.False(t, ok)
}

func TestCatalog_DropNonExistentTable(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	err = cat.DropTable("non_existent")
	require.Error(t, err)
	assert.Equal(t, "table non_existent does not exist", err.Error())
}

func TestCatalog_SaveAndLoad(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	_ = cat.CreateTable("users", 3, 1)
	_ = cat.CreateTable("products", 4, 10)
	require.NoError(t, cat.Save())

	dm, err := disk.NewFileDiskManager(testDBFile)
	require.NoError(t, err)

	cat2 := catalog.NewCatalog(dm)
	require.NoError(t, cat2.Load())

	meta, ok := cat2.Get("products")
	require.True(t, ok)
	assert.Equal(t, int32(4), meta.Degree)
	assert.Equal(t, int32(10), meta.RootID)

	require.NoError(t, dm.Close())
}

func TestCatalog_MetadataInitialization(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	meta, ok := cat.Get("users")
	require.True(t, ok)

	assert.Equal(t, "users", meta.Name)
	assert.Equal(t, int32(3), meta.Degree)
	assert.Equal(t, int32(1), meta.RootID)
	assert.Equal(t, int32(0), meta.RowCount)
	assert.WithinDuration(t, time.Now(), meta.CreatedAt, time.Second, "CreatedAt should be close to the current time")
}

func TestCatalog_IncrementRowCount(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	err = cat.IncrementRowCount("users")
	require.NoError(t, err)

	meta, ok := cat.Get("users")
	require.True(t, ok)
	assert.Equal(t, int32(1), meta.RowCount)

	err = cat.IncrementRowCount("users")
	require.NoError(t, err)

	meta, ok = cat.Get("users")
	require.True(t, ok)
	assert.Equal(t, int32(2), meta.RowCount)
}

func TestCatalog_DecrementRowCount(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.CreateTable("users", 3, 1)
	require.NoError(t, err)

	err = cat.IncrementRowCount("users")
	require.NoError(t, err)

	err = cat.DecrementRowCount("users")
	require.NoError(t, err)

	meta, ok := cat.Get("users")
	require.True(t, ok)
	assert.Equal(t, int32(0), meta.RowCount)

	err = cat.DecrementRowCount("users")
	require.NoError(t, err)

	meta, ok = cat.Get("users")
	require.True(t, ok)
	assert.Equal(t, int32(0), meta.RowCount)
}

func TestCatalog_RowCountOnNonExistentTable(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	err := cat.IncrementRowCount("nonexistent")
	require.Error(t, err)
	assert.Equal(t, "table nonexistent does not exist", err.Error())

	err = cat.DecrementRowCount("nonexistent")
	require.Error(t, err)
	assert.Equal(t, "table nonexistent does not exist", err.Error())
}

func TestCatalog_SaveAndLoadWithMetadata(t *testing.T) {
	cat, cleanup := setupCatalog(t)
	defer cleanup()

	_ = cat.CreateTable("users", 3, 1)
	_ = cat.CreateTable("products", 4, 10)

	// Update metadata
	_ = cat.IncrementRowCount("users")
	_ = cat.IncrementRowCount("users")
	_ = cat.IncrementRowCount("products")

	require.NoError(t, cat.Save())

	dm, err := disk.NewFileDiskManager(testDBFile)
	require.NoError(t, err)

	cat2 := catalog.NewCatalog(dm)
	require.NoError(t, cat2.Load())

	meta, ok := cat2.Get("users")
	require.True(t, ok)
	assert.Equal(t, int32(3), meta.Degree)
	assert.Equal(t, int32(1), meta.RootID)
	assert.Equal(t, int32(2), meta.RowCount) // RowCount should persist
	assert.WithinDuration(t, time.Now(), meta.CreatedAt, time.Second)

	meta, ok = cat2.Get("products")
	require.True(t, ok)
	assert.Equal(t, int32(4), meta.Degree)
	assert.Equal(t, int32(10), meta.RootID)
	assert.Equal(t, int32(1), meta.RowCount) // RowCount should persist
	assert.WithinDuration(t, time.Now(), meta.CreatedAt, time.Second)

	require.NoError(t, dm.Close())
}
