package lsmtree

import (
	"fmt"
	"os"

	"github.com/rafaelmgr12/litegodb/internal/storage/btree"
)

// LSMTree represents a Log-Structured Merge Tree.
type LSMTree struct {
	memtable *btree.BTree
	wal      *os.File
}

// NewLSMTree initializes a new LSMTree
func NewLSMTree(walPath string, degree int) (*LSMTree, error) {
	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &LSMTree{
		memtable: btree.NewBTree(degree),
		wal:      wal,
	}, nil
}

// Insert adds a key-value pair to the LSMTree
func (l *LSMTree) Insert(key int, value string) error {
	// Write to Wal
	if _, err := l.wal.WriteString(fmt.Sprintf("%d:%s\n", key, value)); err != nil {
		return err
	}
	if err := l.wal.Sync(); err != nil {
		return err
	}

	// Insert into memtable
	l.memtable.Insert(key, value)
	// TODO: Trigger a flush to disk if memtable size exceeds threshold
	return nil
}

// Search finds a key in the LSMTree
func (l *LSMTree) Search(key int) (string, bool) {
	value, found := l.memtable.Search(key)
	if found {
		return value.(string), true
	}
	// TODO: Search in SSTables
	return "", false
}

// Flush memtable to an SSTable
func (l *LSMTree) Flush() error {
	// TODO: Serialize memtable to a disk as an SSTable
	return nil
}
