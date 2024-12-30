package disk

// Page represents a unit of storage with a fixed size.
// Pages are identified by a unique ID and hold data that can be serialized and deserialized.
type Page interface {
	ID() int32                     // ID returns the page ID.
	SetId(id int32)                // SetId sets unique identifier of the page.
	Data() []byte                  // Data returns the data stored in the page.s
	SetData(data []byte)           // SetData sets the data stored in the page.
	Serialize() []byte             // Serialize serializes the page to a byte slice.
	Deserialize(data []byte) error // Deserialize deserializes the page from a byte slice.
}
