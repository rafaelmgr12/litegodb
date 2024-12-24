package lsmtree_test

import (
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/lsmtree"
)

func TestLSMTreeInsertAndSearch(t *testing.T) {
	lsm, err := lsmtree.NewLSMTree("test.wal", 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := lsm.Insert(10, "ten"); err != nil {
		t.Fatal(err)
	}
	if err := lsm.Insert(20, "twenty"); err != nil {
		t.Fatal(err)
	}

	value, found := lsm.Search(10)
	if !found || value != "ten" {
		t.Fatalf("expected to find 'ten', found %s", value)
	}

	value, found = lsm.Search(20)
	if !found || value != "twenty" {
		t.Fatalf("expected to find 'twenty', found %s", value)
	}
}
