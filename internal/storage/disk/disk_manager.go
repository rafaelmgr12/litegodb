// The `FileDiskManager` struct in the `disk` package provides methods for managing disk storage of
// pages in a file-based system.
package disk

import (
	"os"
	"sync"

	"github.com/rafaelmgr12/litegodb/internal/storage/freelist"
)

// FileDiskManager is a concrete implementation of the DiskManager interface.
// It uses a file to persist pages.
type FileDiskManager struct {
	file   *os.File
	mu     sync.Mutex
	fl     *freelist.Freelist
	nextID int32
}

// NewFileDiskManager creates a new FileDiskManager instance.
// It initializes the storage file and determines the next available page ID.
func NewFileDiskManager(filePath string) (*FileDiskManager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Calculate the next ID based on the file size.
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	nextID := int32(info.Size() / PageSize)
	return &FileDiskManager{
		file:   file,
		fl:     freelist.NewFreelist(),
		nextID: nextID,
	}, nil
}

// NextID returns the next available page ID.
func (dm *FileDiskManager) NextID() int32 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.nextID
}

// AllocatePage allocates a new page with a unique ID.
func (dm *FileDiskManager) AllocatePage() (Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var pageID int32
	if id, ok := dm.fl.GetFreePage(); ok {
		pageID = id
	} else {
		pageID = dm.nextID
		dm.nextID++
	}

	page := NewFilePage(pageID)
	return page, nil
}

// WritePage writes the given page to the file at the appropriate offset.
func (dm *FileDiskManager) WritePage(page Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	data, err := page.Serialize()
	if err != nil {
		return err
	}

	offset := int64(page.ID()) * PageSize
	_, err = dm.file.WriteAt(data, offset)
	return err
}

// ReadPage reads a page from the file using its ID.
func (dm *FileDiskManager) ReadPage(id int32) (Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(id) * PageSize
	data := make([]byte, PageSize)
	_, err := dm.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	page := NewFilePage(id)
	err = page.Deserialize(data)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// GetLastAllocatedPageID returns the ID of the last allocated page.
func (dm *FileDiskManager) GetLastAllocatedPageID() int32 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.nextID - 1
}

// The `FreePage` method in the `FileDiskManager` struct is responsible for adding the given page ID to
// the freelist.
func (dm *FileDiskManager) FreePage(id int32) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.fl.Add(id)
}

// Close closes the underlying file.
func (dm *FileDiskManager) Close() error {
	return dm.file.Close()
}
