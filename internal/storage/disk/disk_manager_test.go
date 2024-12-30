package disk_test

import (
	"os"
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
)

func setupFileDiskManager(t *testing.T) (*disk.FileDiskManager, func()) {
	tmpfile, err := os.CreateTemp("", "diskmanager_test")
	if err != nil {
		t.Fatalf("error creating temporary file: %v", err)
	}

	dm, err := disk.NewFileDiskManager(tmpfile.Name())
	if err != nil {
		t.Fatalf("error creating disk manager: %v", err)
	}

	cleanup := func() {
		dm.Close()
		os.Remove(tmpfile.Name())
	}

	return dm, cleanup
}

func TestNewFileDiskManager(t *testing.T) {
	_, cleanup := setupFileDiskManager(t)
	defer cleanup()

}

func TestAllocatePage(t *testing.T) {
	dm, cleanup := setupFileDiskManager(t)
	defer cleanup()

	page, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("error allocating page: %v", err)
	}

	if page.ID() != 0 {
		t.Fatalf("expected page ID to be 0, got %d", page.ID())
	}
}

func TestWriteReadPage(t *testing.T) {
	dm, cleanup := setupFileDiskManager(t)
	defer cleanup()

	page, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("error allocating page: %v", err)
	}

	data := []byte("hello, world!")
	page.SetData(data)

	if err := dm.WritePage(page); err != nil {
		t.Fatalf("error writing page: %v", err)
	}

	readPage, err := dm.ReadPage(page.ID())
	if err != nil {
		t.Fatalf("error reading page: %v", err)
	}

	if readPage.ID() != page.ID() {
		t.Errorf("Expected page ID %d, got %d", page.ID(), readPage.ID())
	}

	if string(readPage.Data()[:len(data)]) != string(data) {
		t.Errorf("Expected data %s, got %s", data, readPage.Data())
	}
}

func TestDiskManagerCloser(t *testing.T) {
	dm, cleanup := setupFileDiskManager(t)
	defer cleanup()

	if err := dm.Close(); err != nil {
		t.Fatalf("error closing disk manager: %v", err)
	}

}
