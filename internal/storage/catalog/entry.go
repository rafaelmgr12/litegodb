package catalog

import "time"

// TableMetadata holds persistent metadata for a user-defined table.
// It allows recovery and reconstruction of the table state during database load.
type TableMetadata struct {
	// Name is the unique identifier for the table.
	Name string

	// RootID is the page ID of the root node of the table's B-Tree.
	RootID int32

	// Degree is the degree (minimum branching factor) of the B-Tree.
	Degree int32

	// CreatedAt is the timestamp when the table was created.
	CreatedAt time.Time

	// RowCount is the number of rows in the table.
	RowCount int32
}
