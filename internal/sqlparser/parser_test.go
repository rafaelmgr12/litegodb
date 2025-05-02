package sqlparser_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/session"
	"github.com/rafaelmgr12/litegodb/internal/sqlparser"
	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
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
func (m *mockDB) BeginTransaction() litegodb.Transaction     { return nil }

func TestParseAndExecute_InsertSelectDelete(t *testing.T) {
	db := newMockDB()
	session := session.NewSessionManager().GetOrCreate("test")

	// INSERT
	insert := "INSERT INTO users (`key`, `value`) VALUES (1, 'rafael')"
	res, err := sqlparser.ParseAndExecute(insert, db, session)
	assert.NoError(t, err)
	assert.Equal(t, "inserted", res)

	// SELECT
	selectQ := "SELECT `key`, `value` FROM users WHERE `key` = 1"
	res, err = sqlparser.ParseAndExecute(selectQ, db, session)
	assert.NoError(t, err)
	result, ok := res.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, result["key"])
	assert.Equal(t, "rafael", result["value"])

	// DELETE
	deleteQ := "DELETE FROM users WHERE `key` = 1"
	res, err = sqlparser.ParseAndExecute(deleteQ, db, session)
	assert.NoError(t, err)
	assert.Equal(t, "deleted", res)

	// SELECT not found
	_, err = sqlparser.ParseAndExecute(selectQ, db, session)
	assert.Error(t, err)
}

func TestParseAndExecute_InvalidQueries(t *testing.T) {
	db := newMockDB()
	session := session.NewSessionManager().GetOrCreate("test")

	_, err := sqlparser.ParseAndExecute("UPDATE users SET value = 'x'", db, session)
	assert.Error(t, err)

	_, err = sqlparser.ParseAndExecute("SELECT FROM WHERE", db, session)
	assert.Error(t, err)
}

type mockTransaction struct {
	operations []string
	committed  bool
	rolledBack bool
}

func (mt *mockTransaction) PutBatch(table string, key int, value string) {
	mt.operations = append(mt.operations, "PUT")
}
func (mt *mockTransaction) DeleteBatch(table string, key int) {
	mt.operations = append(mt.operations, "DELETE")
}
func (mt *mockTransaction) Commit() error {
	mt.committed = true
	return nil
}
func (mt *mockTransaction) Rollback() {
	mt.rolledBack = true
	mt.operations = nil
}

type mockDBWithTransaction struct {
	mockDB
	transaction *mockTransaction
}

func (m *mockDBWithTransaction) BeginTransaction() litegodb.Transaction {
	m.transaction = &mockTransaction{}
	return m.transaction
}

func newMockDBWithTransaction() *mockDBWithTransaction {
	return &mockDBWithTransaction{
		mockDB:      *newMockDB(),
		transaction: &mockTransaction{},
	}
}

func TestParseAndExecute_TransactionCommands(t *testing.T) {
	db := &mockDBWithTransaction{}
	session := session.NewSessionManager().GetOrCreate("tx-session")

	// BEGIN
	res, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)
	assert.Equal(t, "transaction started", res)
	assert.NotNil(t, session.Transaction)

	// COMMIT
	res, err = sqlparser.ParseAndExecute("COMMIT", db, session)
	assert.NoError(t, err)
	assert.Equal(t, "transaction committed", res)
	assert.Nil(t, session.Transaction)
	assert.True(t, db.transaction.committed)

	// BEGIN + ROLLBACK
	sqlparser.ParseAndExecute("BEGIN", db, session)
	res, err = sqlparser.ParseAndExecute("ROLLBACK", db, session)
	assert.NoError(t, err)
	assert.Equal(t, "transaction rolled back", res)
	assert.Nil(t, session.Transaction)
	assert.True(t, db.transaction.rolledBack)
}

func TestParseAndExecute_TransactionWithOperations(t *testing.T) {
	db := newMockDBWithTransaction()
	session := session.NewSessionManager().GetOrCreate("tx-ops")

	// BEGIN transaction
	_, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)

	// INSERT operation within transaction
	_, err = sqlparser.ParseAndExecute("INSERT INTO users (`key`, `value`) VALUES (1, 'rafael')", db, session)
	assert.NoError(t, err)

	// DELETE operation within transaction
	_, err = sqlparser.ParseAndExecute("DELETE FROM users WHERE `key` = 1", db, session)
	assert.NoError(t, err)

	// COMMIT transaction
	_, err = sqlparser.ParseAndExecute("COMMIT", db, session)
	assert.NoError(t, err)

	// Validate transaction state
	assert.True(t, db.transaction.committed, "Transaction should be committed")
	assert.Equal(t, []string{"PUT", "DELETE"}, db.transaction.operations, "Operations should match queued actions")
}

func TestParseAndExecute_TransactionRollbackWithoutOperations(t *testing.T) {

	db := newMockDBWithTransaction()
	session := session.NewSessionManager().GetOrCreate("tx-empty-rollback")

	_, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)

	// ROLLBACK transaction without any operations
	_, err = sqlparser.ParseAndExecute("ROLLBACK", db, session)
	assert.NoError(t, err)

	// Validate transaction state
	assert.True(t, db.transaction.rolledBack, "Transaction should be rolled back")
	assert.Empty(t, db.transaction.operations, "No operations should be queued")

}

func TestParseAndExecute_TransactionCommitWithoutOperations(t *testing.T) {
	db := newMockDBWithTransaction()
	session := session.NewSessionManager().GetOrCreate("tx-empty-commit")

	// BEGIN transaction
	_, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)

	// COMMIT transaction without any operations
	_, err = sqlparser.ParseAndExecute("COMMIT", db, session)
	assert.NoError(t, err)

	// Validate transaction state
	assert.True(t, db.transaction.committed, "Transaction should be committed")
	assert.Empty(t, db.transaction.operations, "No operations should be queued")
}

func TestParseAndExecute_NestedTransactions(t *testing.T) {
	db := newMockDBWithTransaction()
	session := session.NewSessionManager().GetOrCreate("tx-nested")

	// BEGIN first transaction
	_, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)

	// Attempt to BEGIN a nested transaction
	_, err = sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.Error(t, err, "Nested transactions should not be allowed")

	// ROLLBACK the first transaction
	_, err = sqlparser.ParseAndExecute("ROLLBACK", db, session)
	assert.NoError(t, err)
}

func TestParseAndExecute_TransactionWithMultipleOperations(t *testing.T) {
	db := newMockDBWithTransaction()
	session := session.NewSessionManager().GetOrCreate("tx-multi-ops")

	// BEGIN transaction
	_, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)

	// Perform multiple operations
	_, err = sqlparser.ParseAndExecute("INSERT INTO users (`key`, `value`) VALUES (1, 'rafael')", db, session)
	assert.NoError(t, err)

	_, err = sqlparser.ParseAndExecute("INSERT INTO users (`key`, `value`) VALUES (2, 'maria')", db, session)
	assert.NoError(t, err)

	_, err = sqlparser.ParseAndExecute("DELETE FROM users WHERE `key` = 1", db, session)
	assert.NoError(t, err)

	// COMMIT transaction
	_, err = sqlparser.ParseAndExecute("COMMIT", db, session)
	assert.NoError(t, err)

	// Validate transaction state
	assert.True(t, db.transaction.committed, "Transaction should be committed")
	assert.Equal(t, []string{"PUT", "PUT", "DELETE"}, db.transaction.operations, "Operations should match queued actions")
}

func TestParseAndExecute_TransactionRollbackAfterFailure(t *testing.T) {
	db := newMockDBWithTransaction()
	session := session.NewSessionManager().GetOrCreate("tx-rollback-failure")

	// BEGIN transaction
	_, err := sqlparser.ParseAndExecute("BEGIN", db, session)
	assert.NoError(t, err)

	// Perform a valid operation
	_, err = sqlparser.ParseAndExecute("INSERT INTO users (`key`, `value`) VALUES (1, 'rafael')", db, session)
	assert.NoError(t, err)

	// Perform an invalid operation
	_, err = sqlparser.ParseAndExecute("INVALID QUERY", db, session)
	assert.Error(t, err)

	// ROLLBACK transaction
	_, err = sqlparser.ParseAndExecute("ROLLBACK", db, session)
	assert.NoError(t, err)

	// Validate transaction state
	assert.True(t, db.transaction.rolledBack, "Transaction should be rolled back")
	assert.Empty(t, db.transaction.operations, "No operations should remain after rollback")
}
