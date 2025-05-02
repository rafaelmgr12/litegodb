package kvstore

import (
	"fmt"
)

// Transaction represents a batch of operations to be performed on the key-value store.
type Transaction struct {
	operations  []LogEntry
	rollbackLog []operationBackup
	kv          *BTreeKVStore
}

// operationBackup holds the previous state of a key for rollback purposes.
type operationBackup struct {
	Table    string
	Key      int
	OldValue string
	Existed  bool
}

// BeginTransaction starts a new transaction.
func (kv *BTreeKVStore) BeginTransaction() *Transaction {
	return &Transaction{
		operations:  make([]LogEntry, 0),
		rollbackLog: make([]operationBackup, 0),
		kv:          kv,
	}
}

// PutBatch queues a PUT operation for later commit.
func (tx *Transaction) PutBatch(table string, key int, value string) {
	tx.operations = append(tx.operations, LogEntry{
		Operation: "PUT",
		Table:     table,
		Key:       key,
		Value:     value,
	})
}

// DeleteBatch queues a DELETE operation for later commit.
func (tx *Transaction) DeleteBatch(table string, key int) {
	tx.operations = append(tx.operations, LogEntry{
		Operation: "DELETE",
		Table:     table,
		Key:       key,
	})
}

// Commit applies all queued operations to the key-value store.
func (tx *Transaction) Commit() error {
	for _, op := range tx.operations {
		val, found, _ := tx.kv.Get(op.Table, op.Key)
		tx.rollbackLog = append(tx.rollbackLog, operationBackup{
			Table:    op.Table,
			Key:      op.Key,
			OldValue: val,
			Existed:  found,
		})

		var err error
		switch op.Operation {
		case "PUT":
			err = tx.kv.Put(op.Table, op.Key, op.Value)
		case "DELETE":
			err = tx.kv.Delete(op.Table, op.Key)
		default:
			err = fmt.Errorf("unknown operation: %s", op.Operation)
		}

		if err != nil {
			tx.rollback()
			return fmt.Errorf("commit failed and was rolled back: %w", err)
		}
	}

	tx.operations = nil
	tx.rollbackLog = nil
	return nil
}

// rollback reverts all successfully applied operations in reverse order.
func (tx *Transaction) rollback() {
	for i := len(tx.rollbackLog) - 1; i >= 0; i-- {
		b := tx.rollbackLog[i]
		if b.Existed {
			_ = tx.kv.Put(b.Table, b.Key, b.OldValue)
		} else {
			_ = tx.kv.Delete(b.Table, b.Key)
		}
	}
}

// Rollback discards all queued operations (if not yet committed).
func (tx *Transaction) Rollback() {
	tx.operations = nil
	tx.rollbackLog = nil
}
