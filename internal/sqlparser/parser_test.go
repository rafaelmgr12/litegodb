package sqlparser_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/sqlparser"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
	"github.com/stretchr/testify/assert"
)

type mockDB struct {
	store map[string]map[int]string
}

func newMockDB() *mockDB {
	return &mockDB{store: make(map[string]map[int]string)}
}

func (m *mockDB) Put(table string, key int, value string) error {
	if m.store[table] == nil {
		m.store[table] = make(map[int]string)
	}
	m.store[table][key] = value
	return nil
}

func (m *mockDB) Get(table string, key int) (string, bool, error) {
	t, ok := m.store[table]
	if !ok {
		return "", false, nil
	}
	val, found := t[key]
	return val, found, nil
}

func (m *mockDB) Delete(table string, key int) error {
	t, ok := m.store[table]
	if !ok {
		return nil
	}
	delete(t, key)
	return nil
}

func (m *mockDB) Flush(table string) error                   { return nil }
func (m *mockDB) CreateTable(table string, degree int) error { return nil }
func (m *mockDB) DropTable(table string) error               { return nil }
func (m *mockDB) Load() error                                { return nil }
func (m *mockDB) Close() error                               { return nil }
func (m *mockDB) BeginTransaction() *kvstore.Transaction     { return nil }

func TestParseAndExecute_InsertSelectDelete(t *testing.T) {
	db := newMockDB()

	// Test INSERT
	insertQuery := "INSERT INTO users (`key`, `value`) VALUES (1, 'rafael')"
	res, err := sqlparser.ParseAndExecute(insertQuery, db)
	assert.NoError(t, err)
	assert.Equal(t, "inserted", res)

	// Test SELECT
	selectQuery := "SELECT `key`, `value` FROM users WHERE `key` = 1"
	res, err = sqlparser.ParseAndExecute(selectQuery, db)
	assert.NoError(t, err)

	resultMap, ok := res.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, resultMap["key"])
	assert.Equal(t, "rafael", resultMap["value"])

	// Test DELETE
	deleteQuery := "DELETE FROM users WHERE `key` = 1"
	res, err = sqlparser.ParseAndExecute(deleteQuery, db)
	assert.NoError(t, err)
	assert.Equal(t, "deleted", res)

	// Test SELECT after DELETE (should fail)
	_, err = sqlparser.ParseAndExecute(selectQuery, db)
	assert.Error(t, err)
}

func TestParseAndExecute_InvalidQueries(t *testing.T) {
	db := newMockDB()

	// Unsupported command
	badQuery := "UPDATE users SET `value` = 'rafael' WHERE `key` = 1"
	_, err := sqlparser.ParseAndExecute(badQuery, db)
	assert.Error(t, err)

	// Bad syntax
	badSyntax := "SELECT FROM WHERE"
	_, err = sqlparser.ParseAndExecute(badSyntax, db)
	assert.Error(t, err)
}
