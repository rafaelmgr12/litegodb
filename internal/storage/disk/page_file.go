package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const PageSize = 4096

// FilePage is a concrete implementation of the Page interface.
// It stores a fixed-size structure that supports serialization and deserialization.
type FilePage struct {
	id   int32  // Unique identifier of the page.
	data []byte // Data stored in the page.
}

// NewFilePage creates a new empty page with the given ID.
// The page is initialized with a fixed size of PageSize.
func NewFilePage(id int32) *FilePage {
	return &FilePage{
		id:   id,
		data: make([]byte, PageSize),
	}
}

// ID retuns the unique identifier of the page.
func (p *FilePage) ID() int32 {
	return p.id
}

// SetId sets the unique identifier of the page.
func (p *FilePage) SetId(id int32) {
	p.id = id
}

// Data returns the data stored in the page.
func (p *FilePage) Data() []byte {
	return p.data
}

// SetData sets the data for the page.
// If the data exceeds the fixed page size, it panics.
func (p *FilePage) SetData(data []byte) {
	if len(data) > PageSize {
		panic(fmt.Sprintf("data exceeds page size: %d bytes", len(data)))
	}
	copy(p.data, data)
}

// Serialize converts the page into a byte slice for storage.
// The first part of the slice contains the page ID, followed by the page data.
func (p *FilePage) Serialize() ([]byte, error) {
	buffer := make([]byte, PageSize)
	idBuffer := make([]byte, 4)

	binary.LittleEndian.PutUint32(idBuffer, uint32(p.id))
	copy(buffer[:4], idBuffer)

	copy(buffer[4:], p.data)

	return buffer, nil
}

// Deserialize populates the page fields from a byte slice.
// The input slice must match the fixed page size.
func (p *FilePage) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(data))
	}
	buffer := bytes.NewReader(data)
	if err := binary.Read(buffer, binary.LittleEndian, &p.id); err != nil {
		return err
	}
	p.data = make([]byte, PageSize)
	if _, err := buffer.Read(p.data); err != nil {
		return err
	}
	return nil
}
