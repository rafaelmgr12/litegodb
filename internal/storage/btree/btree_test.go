package btree_test

import (
	"strings"
	"testing"
	"time"

	"math/rand"

	"github.com/rafaelmgr12/litegodb/internal/storage/btree"
)

func TestBTreeInsertSmallDegree(t *testing.T) {
	btree := btree.NewBTree(2) // Smallest valid degree.

	testCases := []struct {
		key   int
		value string
	}{
		{10, "ten"},
		{20, "twenty"},
		{5, "five"},
		{6, "six"},
		{12, "twelve"},
		{30, "thirty"},
		{7, "seven"},
		{17, "seventeen"},
		{15, "fifteen"},
		{16, "sixteen"},
	}

	for _, tc := range testCases {
		btree.Insert(tc.key, tc.value)
	}

	for _, tc := range testCases {
		value, found := btree.Search(tc.key)
		if !found {
			t.Fatalf("key %d not found", tc.key)
		}
		if value != tc.value {
			t.Fatalf("expected value %s for key %d, got %v", tc.value, tc.key, value)
		}
	}
}

func TestBTreeInsertLargeDegree(t *testing.T) {

	btree := btree.NewBTree(4) // Larger degree.

	testCases := []struct {
		key   int
		value string
	}{
		{10, "ten"},
		{20, "twenty"},
		{5, "five"},
		{6, "six"},
		{12, "twelve"},
		{30, "thirty"},
		{7, "seven"},
		{17, "seventeen"},
		{15, "fifteen"},
		{16, "sixteen"},
		{25, "twenty five"},
		{1, "one"},
		{50, "fifty"},
		{11, "eleven"},
	}
	for _, tc := range testCases {
		btree.Insert(tc.key, tc.value)
	}

	for _, tc := range testCases {
		value, found := btree.Search(tc.key)
		if !found {
			t.Fatalf("key %d not found", tc.key)
		}
		if value != tc.value {
			t.Fatalf("expected value %s for key %d, got %v", tc.value, tc.key, value)
		}
	}

}

func TestBTreeInsertStress(t *testing.T) {
	btree := btree.NewBTree(3)

	testCases := make([]struct {
		key   int
		value string
	}, 1000000)

	for i := 0; i < 1000000; i++ {
		key := i
		value := generateRandomString(10, "abcdefghijklmnopqrstuvwxyz")
		testCases[i] = struct {
			key   int
			value string
		}{key, value}
	}

	for _, tc := range testCases {
		btree.Insert(tc.key, tc.value)
	}

	for _, tc := range testCases {
		value, found := btree.Search(tc.key)
		if !found {
			t.Fatalf("key %d not found", tc.key)
		}
		if value != tc.value {
			t.Fatalf("expected value %s for key %d, got %v", tc.value, tc.key, value)
		}
	}
}

func TestBTreeSearchNonExistentKeys(t *testing.T) {
	btree := btree.NewBTree(2)

	// Insert some keys
	btree.Insert(10, "ten")
	btree.Insert(20, "twenty")
	btree.Insert(5, "five")
	btree.Insert(6, "six")

	nonExistentKeys := []int{0, 15, 100, -10}

	for _, key := range nonExistentKeys {
		if _, found := btree.Search(key); found {
			t.Fatalf("unexpectedly found non-existent key %d", key)
		}
	}
}

func TestBTreeDuplicates(t *testing.T) {
	btree := btree.NewBTree(2)

	// Insert key with initial value
	btree.Insert(10, "ten")

	// Insert duplicate key with new value
	btree.Insert(10, "new ten")

	value, found := btree.Search(10)
	if !found {
		t.Fatalf("key 10 not found")
	}
	if value != "new ten" {
		t.Fatalf("expected value 'new ten' for key 10, got %v", value)
	}
}

func TestBTreeInsertAndDelete(t *testing.T) {
	btree := btree.NewBTree(2)

	testCases := []struct {
		key   int
		value string
	}{
		{10, "ten"},
		{20, "twenty"},
		{5, "five"},
		{6, "six"},
		{12, "twelve"},
	}

	for _, tc := range testCases {
		btree.Insert(tc.key, tc.value)
	}

	for _, tc := range testCases {
		value, found := btree.Search(tc.key)
		if !found {
			t.Fatalf("key %d not found after insertion", tc.key)
		}
		if value != tc.value {
			t.Fatalf("expected value %s for key %d, got %v", tc.value, tc.key, value)
		}
	}

	for _, tc := range testCases {
		btree.Delete(tc.key)
		_, found := btree.Search(tc.key)
		if found {
			t.Fatalf("key %d found after deletion", tc.key)
		}
	}
}

func TestBTreeBoundaries(t *testing.T) {
	btree := btree.NewBTree(3)

	// Insert boundary values
	btree.Insert(0, "zero")
	btree.Insert(int(^uint(0)>>1), "maxInt")
	btree.Insert(-int(^uint(0)>>1)-1, "minInt")

	testCases := []struct {
		key   int
		value string
	}{
		{0, "zero"},
		{int(^uint(0) >> 1), "maxInt"},
		{-int(^uint(0)>>1) - 1, "minInt"},
	}

	for _, tc := range testCases {
		value, found := btree.Search(tc.key)
		if !found {
			t.Fatalf("boundary key %d not found", tc.key)
		}
		if value != tc.value {
			t.Fatalf("expected value %s for key %d, got %v", tc.value, tc.key, value)
		}
	}
}

func generateRandomString(length int, charset string) string {
	rand.Seed(time.Now().UnixNano()) // Seed the random number generator

	var sb strings.Builder
	for i := 0; i < length; i++ {
		randomChar := charset[rand.Intn(len(charset))]
		sb.WriteByte(randomChar)
	}

	return sb.String()
}
