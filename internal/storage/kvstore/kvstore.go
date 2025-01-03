package kvstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

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
	metaPage, err := kv.diskManager.ReadPage(0)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(metaPage.Data())
	var rootID int32
	var degree int32
	binary.Read(buf, binary.LittleEndian, &rootID)
	binary.Read(buf, binary.LittleEndian, &degree)

	rootPage, err := kv.diskManager.ReadPage(rootID)
	if err != nil {
		return err
	}

	// Pass kv.GetPageDataByID as the fetch function
	root, err := deserializeNode(rootPage.Data(), kv.GetPageDataByID)
	if err != nil {
		return err
	}

	kv.btree = btree.NewBTree(int(degree))
	kv.btree.SetRoot(root)

	// Replay the log
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

// GetPageDataByID retrieves the raw page data for a given page ID.
func (kv *BTreeKVStore) GetPageDataByID(pageID int32) ([]byte, error) {
	page, err := kv.diskManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	return page.Data(), nil
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

// StartPeriodicFlush periodically saves the B-Tree to disk at the specified interval.
func (kv *BTreeKVStore) StartPeriodicFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			kv.Flush()
		}
	}()
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

func serializeNode(node *btree.Node) ([]byte, error) {
	buffer := new(bytes.Buffer)

	// ID
	if err := binary.Write(buffer, binary.LittleEndian, node.ID()); err != nil {
		return nil, err
	}

	// IsLeaf
	if err := binary.Write(buffer, binary.LittleEndian, node.IsLeaf()); err != nil {
		return nil, err
	}

	// Degree
	if err := binary.Write(buffer, binary.LittleEndian, int32(node.Degree())); err != nil {
		return nil, err
	}

	// Keys
	numKeys := int32(len(node.Keys()))
	if err := binary.Write(buffer, binary.LittleEndian, numKeys); err != nil {
		return nil, err
	}
	for _, key := range node.Keys() {
		if err := binary.Write(buffer, binary.LittleEndian, int32(key)); err != nil {
			return nil, err
		}
	}

	// Values
	for _, value := range node.Values() {
		valueStr, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid value type")
		}
		if err := binary.Write(buffer, binary.LittleEndian, int32(len(valueStr))); err != nil {
			return nil, err
		}
		if _, err := buffer.WriteString(valueStr); err != nil {
			return nil, err
		}
	}

	// Children
	numChildren := int32(len(node.Children()))
	if err := binary.Write(buffer, binary.LittleEndian, numChildren); err != nil {
		return nil, err
	}
	for _, child := range node.Children() {
		if err := binary.Write(buffer, binary.LittleEndian, child.ID()); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func deserializeNode(data []byte, fetchPageData func(int32) ([]byte, error)) (*btree.Node, error) {
	buffer := bytes.NewReader(data)

	var id int32
	if err := binary.Read(buffer, binary.LittleEndian, &id); err != nil {
		return nil, err
	}

	var isLeaf bool
	if err := binary.Read(buffer, binary.LittleEndian, &isLeaf); err != nil {
		return nil, err
	}

	var degree int32
	if err := binary.Read(buffer, binary.LittleEndian, &degree); err != nil {
		return nil, err
	}

	var numKeys int32
	if err := binary.Read(buffer, binary.LittleEndian, &numKeys); err != nil {
		return nil, err
	}

	keys := make([]int, numKeys)
	for i := 0; i < int(numKeys); i++ {
		var key int32
		if err := binary.Read(buffer, binary.LittleEndian, &key); err != nil {
			return nil, err
		}
		keys[i] = int(key)
	}

	values := make([]interface{}, numKeys)
	for i := 0; i < int(numKeys); i++ {
		var valueLen int32
		if err := binary.Read(buffer, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}
		valueStr := make([]byte, valueLen)
		if _, err := buffer.Read(valueStr); err != nil {
			return nil, err
		}
		values[i] = string(valueStr)
	}

	var numChildren int32
	if err := binary.Read(buffer, binary.LittleEndian, &numChildren); err != nil {
		return nil, err
	}

	children := make([]*btree.Node, numChildren)
	if !isLeaf {
		for i := 0; i < int(numChildren); i++ {
			var childID int32
			if err := binary.Read(buffer, binary.LittleEndian, &childID); err != nil {
				return nil, err
			}

			// Fetch serialized child data
			childData, err := fetchPageData(childID)
			if err != nil {
				return nil, err
			}

			// Deserialize child node
			children[i], err = deserializeNode(childData, fetchPageData)
			if err != nil {
				return nil, err
			}
		}
	}

	node := btree.NewNodeComplete(
		id,
		keys,
		values,
		children,
		isLeaf,
		int(degree),
	)

	return node, nil
}

// Exported function for testing serialization.
func SerializeNodeForTest(node *btree.Node) ([]byte, error) {
	return serializeNode(node)
}

// Exported function for testing deserialization.
func DeserializeNodeForTest(data []byte, fetchPageData func(int32) ([]byte, error)) (*btree.Node, error) {
	return deserializeNode(data, fetchPageData)
}
