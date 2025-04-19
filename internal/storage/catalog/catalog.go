package catalog

import (
	"fmt"
	"sync"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
)

// Catalog manages the metadata of all tables in the database.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]*TableMetadata
	disk   disk.DiskManager
}

// NewCatalog creates a new in-memory catalog instance.
func NewCatalog(disk disk.DiskManager) *Catalog {
	return &Catalog{
		tables: make(map[string]*TableMetadata),
		disk:   disk,
	}
}

// CreateTable adds a new table to the catalog.
func (c *Catalog) CreateTable(name string, degree int32, rootID int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	c.tables[name] = &TableMetadata{
		Name:   name,
		Degree: degree,
		RootID: rootID,
	}

	return nil
}

// Get retrieves the metadata of a table by its name.
func (c *Catalog) Get(name string) (*TableMetadata, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tables[name]
	return t, ok
}

// List return the names of all registered tables.
func (c *Catalog) List() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	return names

}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	delete(c.tables, name)
	return nil
}
