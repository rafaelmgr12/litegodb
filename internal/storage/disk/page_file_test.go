package disk_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
)

func TestNewFilePage(t *testing.T) {
	pageId := int32(1)
	page := disk.NewFilePage(pageId)

	if page.ID() != pageId {
		t.Errorf("expected page ID to be %d, got %d", pageId, page.ID())
	}

	if len(page.Data()) != disk.PageSize {
		t.Errorf("expected page data size to be %d bytes, got %d", disk.PageSize, len(page.Data()))
	}
}

func TestSetID(t *testing.T) {
	pageId := int32(1)
	page := disk.NewFilePage(pageId)

	newPageId := int32(2)
	page.SetId(newPageId)

	if page.ID() != newPageId {
		t.Errorf("expected page ID to be %d, got %d", newPageId, page.ID())
	}
}

func TestSetData(t *testing.T) {
	page := disk.NewFilePage(1)
	data := []byte("test data")
	page.SetData(data)

	if string(page.Data()[:len(data)]) != string(data) {
		t.Errorf("Expected data %s, got %s", data, page.Data())
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for exceeding PageSize, but did not get one")
		}
	}()
	page.SetData(make([]byte, disk.PageSize+1))
}

func TestSerializeDeserialize(t *testing.T) {
	pageID := int32(1)
	data := []byte("test data")
	page := disk.NewFilePage(pageID)
	page.SetData(data)

	bytes, err := page.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %s", err)
	}

	newPage := disk.NewFilePage(0)
	if err := newPage.Deserialize(bytes); err != nil {
		t.Fatalf("Deserialize failed: %s", err)
	}

	if newPage.ID() != pageID {
		t.Errorf("Expected ID %d, got %d", pageID, newPage.ID())
	}

	if string(newPage.Data()[:len(data)]) != string(data) {
		t.Errorf("Expected data %s, got %s", data, newPage.Data())
	}
}
