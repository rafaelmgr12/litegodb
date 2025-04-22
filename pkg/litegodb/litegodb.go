// Package litegodb provides an interface for interacting with a lightweight
// key-value database using a B-Tree as the underlying storage mechanism.
package litegodb

import (
	"fmt"

	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

// DB defines the interface for interacting with the database.
// It includes methods for basic CRUD operations, table management, and lifecycle management.
type DB interface {
	// Put inserts or updates a key-value pair in the specified table.
	Put(table string, key int, value string) error

	// Get retrieves the value associated with the given key in the specified table.
	// Returns the value, a boolean indicating if the key was found, and an error if any.
	Get(table string, key int) (string, bool, error)

	// Delete removes the key-value pair associated with the given key in the specified table.
	Delete(table string, key int) error

	// Flush persists all changes in the specified table to disk.
	Flush(table string) error

	// CreateTable creates a new table with the specified degree.
	CreateTable(table string, degree int) error

	// DropTable deletes the specified table and all its data.
	DropTable(table string) error

	// Load reloads the database from disk.
	Load() error

	// Close closes the database and releases all resources.
	Close() error
}

// btreeAdapter is an implementation of the DB interface that uses a B-Tree
// as the underlying storage mechanism.
type btreeAdapter struct {
	kv *kvstore.BTreeKVStore
}

// Put inserts or updates a key-value pair in the specified table.
// If the table does not exist, it is automatically created.
func (b *btreeAdapter) Put(table string, key int, value string) error {
	if exists := b.kv.IsTableExists(table); !exists {
		if err := b.kv.CreateTableName(table, 3); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table, err)
		}
	}
	return b.kv.Put(table, key, value)
}

// Get retrieves the value associated with the given key in the specified table.
func (b *btreeAdapter) Get(table string, key int) (string, bool, error) {
	return b.kv.Get(table, key)
}

// Delete removes the key-value pair associated with the given key in the specified table.
func (b *btreeAdapter) Delete(table string, key int) error {
	return b.kv.Delete(table, key)
}

// Flush persists all changes in the specified table to disk.
func (b *btreeAdapter) Flush(table string) error {
	return b.kv.Flush(table)
}

// Load reloads the database from disk.
func (b *btreeAdapter) Load() error {
	return b.kv.Load()
}

// Close closes the database and releases all resources.
func (b *btreeAdapter) Close() error {
	return b.kv.Close()
}

// CreateTable creates a new table with the specified degree.
func (b *btreeAdapter) CreateTable(table string, degree int) error {
	if _, exists, _ := b.kv.Get(table, 0); exists {
		return nil
	}
	return b.kv.CreateTableName(table, degree)
}

// DropTable deletes the specified table and all its data.
func (b *btreeAdapter) DropTable(table string) error {
	return b.kv.DropTable(table)
}
