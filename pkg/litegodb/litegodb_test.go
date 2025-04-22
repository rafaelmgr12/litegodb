package litegodb_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
)

// setupTestDB creates a temporary config file and initializes the database.
// It returns the DB instance and a teardown function to clean up the test files.
func setupTestDB(t *testing.T) (litegodb.DB, func()) {
	t.Helper()

	configFile := "test-config.yaml"
	dbFile := "testdata.db"
	logFile := "testlog.log"

	err := os.WriteFile(configFile, []byte(`
degree: 2
db_file: "`+dbFile+`"
log_file: "`+logFile+`"
flush_every: 1s
`), 0644)
	assert.NoError(t, err)

	db, err := litegodb.Open(configFile)
	assert.NoError(t, err)

	teardown := func() {
		_ = db.Close()
		_ = os.Remove(configFile)
		_ = os.Remove(dbFile)
		_ = os.Remove(logFile)
	}

	return db, teardown
}

func TestPutAndGet(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	table := "users"
	key := 1
	value := "rafael"

	err := db.Put(table, key, value)
	assert.NoError(t, err)

	result, found, err := db.Get(table, key)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value, result)
}

func TestDelete(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	table := "users"
	key := 42

	_ = db.Put(table, key, "to delete")

	err := db.Delete(table, key)
	assert.NoError(t, err)

	_, found, err := db.Get(table, key)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestFlush(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	table := "products"
	_ = db.Put(table, 10, "macbook")

	err := db.Flush(table)
	assert.NoError(t, err)
}

func TestLoad(t *testing.T) {
	db, teardown := setupTestDB(t)

	table := "session"
	key := 5
	value := "token"

	_ = db.Put(table, key, value)
	_ = db.Flush(table)
	_ = db.Close()

	// Reload database from disk
	db2, err := litegodb.Open("test-config.yaml")
	assert.NoError(t, err)
	defer teardown()

	result, found, err := db2.Get(table, key)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value, result)
}

func TestClose(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	err := db.Close()
	assert.NoError(t, err)
}

func TestCreateTable(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	err := db.CreateTable("clients", 3)
	assert.NoError(t, err)

	err = db.Put("clients", 1, "maria")
	assert.NoError(t, err)

	val, found, err := db.Get("clients", 1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "maria", val)
}

func TestDropTable(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	table := "logs"
	_ = db.CreateTable(table, 2)
	_ = db.Put(table, 99, "entry")

	err := db.DropTable(table)
	assert.NoError(t, err)

	_, found, err := db.Get(table, 99)
	assert.Error(t, err)
	assert.False(t, found)
}

func TestPutAutoCreatesTable(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	err := db.Put("autogen", 1, "value")
	assert.NoError(t, err)

	val, found, err := db.Get("autogen", 1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value", val)
}
