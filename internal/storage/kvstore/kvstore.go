package kvstore

import (
	"fmt"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/btree"
	"github.com/rafaelmgr12/litegodb/internal/storage/catalog"
	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
)

// BTreeKVStore represents a key-value store backed by a B-Tree and persistent storage.
type BTreeKVStore struct {
	tables      map[string]*btree.BTree
	diskManager disk.DiskManager
	log         *AppendOnlyLog
	catalog     *catalog.Catalog
}

// NewBTreeKVStore initializes a new KVStore with a B-Tree, DiskManager, and AppendOnlyLog.
func NewBTreeKVStore(degree int, diskManager disk.DiskManager, logFilename string) (*BTreeKVStore, error) {
	log, err := NewAppendOnlyLog(logFilename)
	if err != nil {
		return nil, err
	}

	cat := catalog.NewCatalog(diskManager)
	_ = cat.Load()

	return &BTreeKVStore{
		tables:      make(map[string]*btree.BTree),
		diskManager: diskManager,
		log:         log,
		catalog:     cat,
	}, nil
}

func (kv *BTreeKVStore) CreateTableName(name string, degree int) error {
	if _, exists := kv.catalog.Get(name); exists {
		return fmt.Errorf("table %s already exists", name)
	}

	bt := btree.NewBTree(degree)
	kv.tables[name] = bt

	page, err := kv.diskManager.AllocatePage()
	if err != nil {
		return err
	}
	rootID := page.ID()

	err = kv.catalog.CreateTable(name, rootID, int32(degree))
	if err != nil {
		return err
	}

	return kv.catalog.Save()

}

// Put inserts or updates a key-value pair in the KVStore.
func (kv *BTreeKVStore) Put(table string, key int, value string) error {
	bt, exists := kv.tables[table]
	if !exists {

		meta, ok := kv.catalog.Get(table)
		if !ok {
			return fmt.Errorf("table %s does not exist", table)
		}

		rootPage, err := kv.diskManager.ReadPage(meta.RootID)
		if err != nil {
			return err
		}

		bt, err = btree.Deserialize(rootPage.Data(), kv.GetPageDataByID)
		if err != nil {
			return err
		}

		kv.tables[table] = bt

	}
	entry := &LogEntry{Operation: "PUT", Key: key, Value: value, Table: table}
	if err := kv.log.Append(entry); err != nil {
		return err
	}

	bt.Insert(key, value)
	return kv.Flush(table)
}

// Get retrieves the value associated with a key.
func (kv *BTreeKVStore) Get(table string, key int) (string, bool, error) {
	bt, exists := kv.tables[table]
	if !exists {
		meta, ok := kv.catalog.Get(table)
		if !ok {
			return "", false, fmt.Errorf("table %s does not exists", table)
		}

		rootPage, err := kv.diskManager.ReadPage(meta.RootID)
		if err != nil {
			return "", false, err
		}

		bt, err = btree.Deserialize(rootPage.Data(), kv.GetPageDataByID)
		if err != nil {
			return "", false, err
		}

		kv.tables[table] = bt
	}

	value, found := bt.Search(key)
	if !found {
		return "", false, nil
	}
	return value.(string), true, nil
}

// Delete removes a key-value pair from the KVStore.
func (kv *BTreeKVStore) Delete(table string, key int) error {
	bt, exists := kv.tables[table]
	if !exists {
		meta, ok := kv.catalog.Get(table)
		if !ok {
			return fmt.Errorf("table %s does not exist", table)
		}

		rootPage, err := kv.diskManager.ReadPage(meta.RootID)
		if err != nil {
			return err
		}
		bt, err := btree.Deserialize(rootPage.Data(), kv.GetPageDataByID)
		if err != nil {
			return err
		}
		kv.tables[table] = bt
	}

	entry := &LogEntry{Operation: "DELETE", Table: table, Key: key}
	if err := kv.log.Append(entry); err != nil {
		return err
	}
	bt.Delete(key)
	return kv.Flush(table)
}

// Flush saves the in-memory B-Tree structure to disk.
func (kv *BTreeKVStore) Flush(table string) error {
	bt, exists := kv.tables[table]
	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}

	meta, ok := kv.catalog.Get(table)
	if !ok {
		return fmt.Errorf("table %s not registered on catalog", table)
	}

	data, err := bt.Serialize()
	if err != nil {
		return err
	}
	page := disk.NewFilePage(meta.RootID)
	page.SetData(data)

	if err := kv.diskManager.WritePage(page); err != nil {
		return err
	}

	return kv.catalog.Save()
}

// Load restores the KVStore state by replaying the append-only log.
func (kv *BTreeKVStore) Load() error {
	if err := kv.catalog.Load(); err != nil {
		return err
	}

	for name, meta := range kv.catalog.All() {
		rootPage, err := kv.diskManager.ReadPage(meta.RootID)
		if err != nil {
			return err
		}

		bt, err := btree.Deserialize(rootPage.Data(), kv.GetPageDataByID)
		if err != nil {
			return err
		}

		kv.tables[name] = bt
	}

	entries, err := kv.log.Replay()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		bt, ok := kv.tables[entry.Table]
		if !ok {
			meta, ok := kv.catalog.Get(entry.Table)
			if !ok {
				continue
			}

			rootPage, err := kv.diskManager.ReadPage(meta.RootID)
			if err != nil {
				return err
			}

			bt, err = btree.Deserialize(rootPage.Data(), kv.GetPageDataByID)
			if err != nil {
				return err
			}

			kv.tables[entry.Table] = bt
		}

		switch entry.Operation {
		case "PUT":
			bt.Insert(entry.Key, entry.Value)
		case "DELETE":
			bt.Delete(entry.Key)
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
func (kv *BTreeKVStore) saveNode(bt *btree.BTree, pageID int32) error {
	page := disk.NewFilePage(pageID)

	data, err := bt.Serialize()
	if err != nil {
		return err
	}

	page.SetData(data)
	return kv.diskManager.WritePage(page)
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
			for table := range kv.tables {
				kv.Flush(table)
			}
		}
	}()
}

// DropTable removes a table from the KVStore and the catalog.
func (kv *BTreeKVStore) DropTable(name string) error {
	if _, exists := kv.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	if err := kv.catalog.DropTable(name); err != nil {
		return err
	}

	delete(kv.tables, name)
	return nil
}

func SerializeNodeForTest(bt *btree.BTree) ([]byte, error) {
	return bt.Serialize()
}

func DeserializeNodeForTest(data []byte, fetchPageData func(int32) ([]byte, error)) (*btree.BTree, error) {
	return btree.Deserialize(data, fetchPageData)
}
