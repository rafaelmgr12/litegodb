package kvstore

import (
	"bytes"
	"encoding/binary"

	"github.com/rafaelmgr12/litegodb/internal/storage/btree"
	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
)

// BTreeKVStore represents a key-value store backed by a B-Tree and persistent storage.
type BTreeKVStore struct {
	btree       *btree.BTree
	diskManager disk.DiskManager
	log         *AppendOnlyLog
}

// NewBTreeKVStore initializes a new KVStore with a B-Tree, DiskManager, and AppendOnlyLog.
func NewBTreeKVStore(degree int, diskManager disk.DiskManager, logFilename string) (*BTreeKVStore, error) {
	log, err := NewAppendOnlyLog(logFilename)
	if err != nil {
		return nil, err
	}
	return &BTreeKVStore{
		btree:       btree.NewBTree(degree),
		diskManager: diskManager,
		log:         log,
	}, nil
}

// Put inserts or updates a key-value pair in the KVStore.
func (kv *BTreeKVStore) Put(key int, value string) error {
	entry := &LogEntry{Operation: "PUT", Key: key, Value: value}
	if err := kv.log.Append(entry); err != nil {
		return err
	}
	kv.btree.Insert(key, value)
	return kv.Flush()
}

// Get retrieves the value associated with a key.
func (kv *BTreeKVStore) Get(key int) (string, bool, error) {
	value, found := kv.btree.Search(key)
	if !found {
		return "", false, nil
	}
	return value.(string), true, nil
}

// Delete removes a key-value pair from the KVStore.
func (kv *BTreeKVStore) Delete(key int) error {
	entry := &LogEntry{Operation: "DELETE", Key: key}
	if err := kv.log.Append(entry); err != nil {
		return err
	}
	kv.btree.Delete(key)
	return kv.Flush()
}

// Flush saves the in-memory B-Tree structure to disk.
func (kv *BTreeKVStore) Flush() error {
	rootID := int32(1) // Assign a unique ID to the root node
	if err := kv.saveMetadata(rootID, kv.btree.Degree()); err != nil {
		return err
	}
	return kv.saveNode(kv.btree.Root(), rootID)
}

// Load restores the KVStore state by replaying the append-only log.
func (kv *BTreeKVStore) Load() error {
	entries, err := kv.log.Replay()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch entry.Operation {
		case "PUT":
			kv.btree.Insert(entry.Key, entry.Value)
		case "DELETE":
			kv.btree.Delete(entry.Key)
		}
	}
	return nil
}

// saveNode recursively saves a B-Tree node and its children to disk.
func (kv *BTreeKVStore) saveNode(node *btree.Node, pageID int32) error {
	page := disk.NewFilePage(pageID)
	data, err := serializeNode(node)
	if err != nil {
		return err
	}
	page.SetData(data)
	if err := kv.diskManager.WritePage(page); err != nil {
		return err
	}
	for i, child := range node.Children() {
		childPageID := pageID*10 + int32(i+1)
		if err := kv.saveNode(child, childPageID); err != nil {
			return err
		}
	}
	return nil
}

// Close releases resources held by the KVStore.
func (kv *BTreeKVStore) Close() error {
	if err := kv.log.Close(); err != nil {
		return err
	}
	return kv.diskManager.Close()
}

// saveMetadata saves the metadata (e.g., root ID, degree) to disk.
func (kv *BTreeKVStore) saveMetadata(rootID int32, degree int) error {
	metaPage := disk.NewFilePage(0)
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, rootID)
	binary.Write(buffer, binary.LittleEndian, int32(degree))
	metaPage.SetData(buffer.Bytes())
	return kv.diskManager.WritePage(metaPage)
}

// serializeNode serializes a B-Tree node to a byte slice.
func serializeNode(node *btree.Node) ([]byte, error) {
	// Implementation of node serialization
	return nil, nil
}
