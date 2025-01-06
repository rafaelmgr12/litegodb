package freelist_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/freelist"
)

func TestNewFreelist(t *testing.T) {
	fl := freelist.NewFreelist()
	if fl.Len() != 0 {
		t.Errorf("expected freelist to be empty, got length %d", fl.Len())
	}
}

func TestFreelist_AddAndGet(t *testing.T) {
	fl := freelist.NewFreelist()

	fl.Add(1)
	fl.Add(2)
	fl.Add(3)

	if fl.Len() != 3 {
		t.Errorf("expected freelist length 3, got %d", fl.Len())
	}

	pageID, ok := fl.GetFreePage()
	if !ok || pageID != 3 {
		t.Errorf("expected pageID 3, got %d, ok: %v", pageID, ok)
	}

	pageID, ok = fl.GetFreePage()
	if !ok || pageID != 2 {
		t.Errorf("expected pageID 2, got %d, ok: %v", pageID, ok)
	}

	pageID, ok = fl.GetFreePage()
	if !ok || pageID != 1 {
		t.Errorf("expected pageID 1, got %d, ok: %v", pageID, ok)
	}

	// Ensure the freelist is empty
	pageID, ok = fl.GetFreePage()
	if ok {
		t.Errorf("expected no free page, got %d", pageID)
	}
}

func TestFreelist_SerializeDeserialize(t *testing.T) {
	fl := freelist.NewFreelist()

	// Add pages
	fl.Add(1)
	fl.Add(2)
	fl.Add(3)

	// Serialize the freelist
	data, err := fl.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize freelist: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty serialized data")
	}

	// Deserialize the freelist
	flRestored, err := freelist.Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize freelist: %v", err)
	}

	// Check the length of the restored freelist
	if flRestored.Len() != 3 {
		t.Errorf("expected restored freelist length 3, got %d", flRestored.Len())
	}

	// Check if the values are correct
	pageID, ok := flRestored.GetFreePage()
	if !ok || pageID != 3 {
		t.Errorf("expected pageID 3, got %d, ok: %v", pageID, ok)
	}

	pageID, ok = flRestored.GetFreePage()
	if !ok || pageID != 2 {
		t.Errorf("expected pageID 2, got %d, ok: %v", pageID, ok)
	}

	pageID, ok = flRestored.GetFreePage()
	if !ok || pageID != 1 {
		t.Errorf("expected pageID 1, got %d, ok: %v", pageID, ok)
	}

	// Ensure the restored freelist is now empty
	pageID, ok = flRestored.GetFreePage()
	if ok {
		t.Errorf("expected no free page, got %d", pageID)
	}
}

func TestFreelist_ConcurrentAddAndGet(t *testing.T) {
	fl := freelist.NewFreelist()

	// Use goroutines to simulate concurrent access
	numPages := 100
	for i := 1; i <= numPages; i++ {
		go fl.Add(int32(i))
	}

	for i := 1; i <= numPages; i++ {
		go func() {
			if _, ok := fl.GetFreePage(); !ok {
				t.Errorf("expected to get a free page, but got none")
			}
		}()
	}
}
