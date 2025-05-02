package btree_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"math/rand"

	"github.com/rafaelmgr12/litegodb/internal/storage/btree"
	"github.com/stretchr/testify/require"
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

func TestBtreeSerialize(t *testing.T) {
	bt := btree.NewBTree(2)

	testCases := []struct {
		key   int
		value string
	}{
		{10, "ten"},
		{20, "twenty"},
		{5, "five"},
	}

	for _, tc := range testCases {
		bt.Insert(tc.key, tc.value)
	}

	// Serialize the tree
	data, err := bt.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize B-tree: %v", err)
	}

	// Dummy fetchPage (not needed for single node)
	dummyFetch := func(id int32) ([]byte, error) {
		return nil, fmt.Errorf("unexpected fetchPage call")
	}

	// Deserialize
	deserialized, err := btree.Deserialize(data, dummyFetch)
	if err != nil {
		t.Fatalf("failed to deserialize B-tree: %v", err)
	}

	// Assert all keys exist in the new tree
	for _, tc := range testCases {
		value, found := deserialized.Search(tc.key)
		if !found {
			t.Fatalf("key %d not found after deserialization", tc.key)
		}
		if value != tc.value {
			t.Fatalf("expected value %s for key %d, got %v", tc.value, tc.key, value)
		}
	}
}

func TestEmptyTreeOperations(t *testing.T) {
	btree := btree.NewBTree(2)

	// Search in an empty tree
	if _, found := btree.Search(10); found {
		t.Fatalf("unexpectedly found key in empty tree")
	}

	// Delete in an empty tree
	btree.Delete(10) // Should not panic
}

func TestTreeGrowth(t *testing.T) {
	btree := btree.NewBTree(2)

	// Insert enough keys to grow the tree height
	for i := 0; i < 100; i++ {
		btree.Insert(i, fmt.Sprintf("value%d", i))
	}

	// Verify all keys exist
	for i := 0; i < 100; i++ {
		value, found := btree.Search(i)
		if !found {
			t.Fatalf("key %d not found", i)
		}
		if value != fmt.Sprintf("value%d", i) {
			t.Fatalf("expected value%d, got %v", i, value)
		}
	}
}

func TestSmallestDegree(t *testing.T) {
	btree := btree.NewBTree(2)

	// Insert and delete keys
	btree.Insert(1, "one")
	btree.Insert(2, "two")
	btree.Delete(1)

	if _, found := btree.Search(1); found {
		t.Fatalf("key 1 found after deletion")
	}

	value, found := btree.Search(2)
	if !found || value != "two" {
		t.Fatalf("key 2 not found or incorrect value")
	}
}

func TestConcurrentAccess(t *testing.T) {
	btree := btree.NewBTree(3)
	var wg sync.WaitGroup

	// Concurrent inserts
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			btree.Insert(i, fmt.Sprintf("value%d", i))
		}
	}()

	// Concurrent searches
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			btree.Search(i)
		}
	}()

	wg.Wait()
}

func TestInsertNilValuePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil value")
		}
	}()
	b := btree.NewBTree(2)
	b.Insert(1, nil)
}

func TestDeserializeWithInvalidData(t *testing.T) {
	_, err := btree.Deserialize([]byte{1, 2}, func(id int32) ([]byte, error) {
		return nil, fmt.Errorf("should not be called")
	})
	if err == nil {
		t.Fatalf("expected error when deserializing invalid data")
	}
}

func TestBTreeDeleteTriggersMerge(t *testing.T) {
	b := btree.NewBTree(2)

	keys := []int{1, 2, 3, 4, 5}
	for _, k := range keys {
		b.Insert(k, fmt.Sprintf("val%d", k))
	}
	b.Delete(3)

	for _, k := range []int{1, 2, 4, 5} {
		v, found := b.Search(k)
		require.True(t, found)
		require.Equal(t, fmt.Sprintf("val%d", k), v)
	}
}

func TestSerializeNonStringValue(t *testing.T) {
	b := btree.NewBTree(2)
	b.Insert(1, 123)

	_, err := b.Serialize()
	require.Error(t, err)
	require.Contains(t, err.Error(), "value is not string")
}

func TestNewBTreeWithInvalidDegree(t *testing.T) {
	b := btree.NewBTree(1)
	require.Equal(t, 2, b.Degree()) // Verifica que foi ajustado para mínimo válido
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
