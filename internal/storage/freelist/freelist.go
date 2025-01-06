package freelist

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// Freelist represents a stack of free page IDs.
type Freelist struct {
	mu    sync.Mutex
	pages []int32
}

// NewFreelist creates a new empty Freelist.
func NewFreelist() *Freelist {
	return &Freelist{
		pages: make([]int32, 0),
	}
}

// Add adds a page ID to the Freelist.
func (f *Freelist) Add(id int32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pages = append(f.pages, id)
}

// GetFreePage retrieves and removes a free page ID from the freelist.
// Returns the page ID and true if available, otherwise 0 and false.
func (f *Freelist) GetFreePage() (int32, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pages) == 0 {
		return 0, false
	}
	pageID := f.pages[len(f.pages)-1]
	f.pages = f.pages[:len(f.pages)-1]
	return pageID, true
}

// Serializes converts the freelist into a byte slice for storage
func (f *Freelist) Serialize() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	buffer := new(bytes.Buffer)
	for _, pageID := range f.pages {
		if err := binary.Write(buffer, binary.LittleEndian, pageID); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

// Deserialize reconstructs the freelist from a byte slice.
func Deserialize(data []byte) (*Freelist, error) {
	buffer := bytes.NewReader(data)
	freelist := NewFreelist()

	for buffer.Len() > 0 {
		var pageID int32
		if err := binary.Read(buffer, binary.LittleEndian, &pageID); err != nil {
			return nil, err
		}
		freelist.pages = append(freelist.pages, pageID)
	}
	return freelist, nil
}

// Len returns the number of free pages in the freelist.
func (f *Freelist) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pages)
}
