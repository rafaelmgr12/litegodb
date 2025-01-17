package disk

// DiskManager handles the allocation, reading, and writing of pages.
// It abstracts the underlying storage mechanism and provides a consistent interface for managing pages.
type DiskManager interface {
	// AllocatePage allocates a new page and returns it.
	// The page is assigned a unique ID.
	AllocatePage() (Page, error)

	// WritePage writes the given page to the storage medium.
	WritePage(page Page) error

	// ReadPage reads a page with the given ID from the storage medium.
	// It returns an error if the page does not exist.
	ReadPage(id int32) (Page, error)

	// GetLastAllocatedPageID returns the ID of the last allocated page.
	GetLastAllocatedPageID() int32

	// FreePage adds the page ID back to the freelist, making it available for future allocation.
	FreePage(id int32)

	// Close closes the DiskManager, releasing any open resources.
	Close() error
}
