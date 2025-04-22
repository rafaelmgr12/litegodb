package litegodb

import (
	"fmt"

	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

type DB interface {
	Put(table string, key int, value string) error
	Get(table string, key int) (string, bool, error)
	Delete(table string, key int) error
	Flush(table string) error
	CreateTable(table string, degree int) error
	DropTable(table string) error
	Load() error
	Close() error
}

type btreeAdapter struct {
	kv *kvstore.BTreeKVStore
}

func (b *btreeAdapter) Put(table string, key int, value string) error {
	if exists := b.kv.IsTableExists(table); !exists {
		// create table
		if err := b.kv.CreateTableName(table, 3); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table, err)
		}
	}

	return b.kv.Put(table, key, value)
}
func (b *btreeAdapter) Get(table string, key int) (string, bool, error) {
	return b.kv.Get(table, key)
}
func (b *btreeAdapter) Delete(table string, key int) error {
	return b.kv.Delete(table, key)
}
func (b *btreeAdapter) Flush(table string) error {
	return b.kv.Flush(table)
}
func (b *btreeAdapter) Load() error {
	return b.kv.Load()
}
func (b *btreeAdapter) Close() error {
	return b.kv.Close()
}
func (b *btreeAdapter) CreateTable(table string, degree int) error {
	if _, exists, _ := b.kv.Get(table, 0); exists {
		return nil
	}
	return b.kv.CreateTableName(table, degree)
}
func (b *btreeAdapter) DropTable(table string) error {

	return b.kv.DropTable(table)
}
