// Package litegodb provides an interface for interacting with a lightweight
// key-value database using a B-Tree as the underlying storage mechanism.
package litegodb

import "github.com/rafaelmgr12/litegodb/internal/storage/kvstore"

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

	// BeginTransaction starts a new transaction.
	BeginTransaction() *kvstore.Transaction
}
