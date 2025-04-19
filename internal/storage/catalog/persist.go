package catalog

import (
	"bytes"
	"encoding/binary"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
)

const catalogPageID int32 = 0

// Save persists the current catalog state to disk.
func (c *Catalog) Save() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, int32(len(c.tables))); err != nil {
		return err
	}

	for _, meta := range c.tables {
		nameBytes := []byte(meta.Name)
		nameLen := int32(len(nameBytes))

		if err := binary.Write(buf, binary.LittleEndian, nameLen); err != nil {
			return err
		}

		if _, err := buf.Write(nameBytes); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, int32(meta.RootID)); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, int32(meta.Degree)); err != nil {
			return err
		}
	}

	page := disk.NewFilePage(catalogPageID)
	page.SetData(buf.Bytes())
	return c.disk.WritePage(page)
}

// Load reads the catalog state from disk and rebuilds the in-memory map.
func (c *Catalog) Load() error {
	page, err := c.disk.ReadPage(catalogPageID)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables = make(map[string]*TableMetadata)

	buf := bytes.NewReader(page.Data())

	var count int32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return err
	}

	for i := int32(0); i < count; i++ {
		var nameLen int32
		if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
			return err
		}

		nameBytes := make([]byte, nameLen)
		if _, err := buf.Read(nameBytes); err != nil {
			return err
		}

		var rootID int32
		if err := binary.Read(buf, binary.LittleEndian, &rootID); err != nil {
			return err
		}

		var degree int32
		if err := binary.Read(buf, binary.LittleEndian, &degree); err != nil {
			return err
		}

		name := string(nameBytes)
		c.tables[name] = &TableMetadata{
			Name:   name,
			RootID: rootID,
			Degree: degree,
		}
	}

	return nil
}
