package kvstore_test

import (
	"os"
	"sync"
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

func TestAppendOnlyLog(t *testing.T) {
	// Create a temporary log file
	tmpfile, err := os.CreateTemp("", "append_only_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	log, err := kvstore.NewAppendOnlyLog(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create append-only log: %v", err)
	}
	defer log.Close()

	// Append entries to the log, including the table name
	entries := []*kvstore.LogEntry{
		{Operation: "PUT", Key: 1, Value: "one", Table: "table1"},
		{Operation: "PUT", Key: 2, Value: "two", Table: "table1"},
		{Operation: "DELETE", Key: 1, Table: "table1"},
	}

	for _, entry := range entries {
		if err := log.Append(entry); err != nil {
			t.Fatalf("Failed to append log entry: %v", err)
		}
	}

	// Replay the log and verify entries
	replayedEntries, err := log.Replay()
	if err != nil {
		t.Fatalf("Failed to replay log: %v", err)
	}

	if len(replayedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(replayedEntries))
	}

	for i, entry := range replayedEntries {
		if entries[i].Operation != entry.Operation || entries[i].Key != entry.Key || entries[i].Value != entry.Value || entries[i].Table != entry.Table {
			t.Errorf("Mismatch at entry %d: expected %+v, got %+v", i, entries[i], entry)
		}
	}
}

func TestSerialization(t *testing.T) {
	originalEntry := &kvstore.LogEntry{
		Operation: "PUT",
		Key:       1,
		Value:     "one",
		Table:     "table1",
	}
	data, err := originalEntry.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize log entry: %v", err)
	}

	deserializedEntry, err := kvstore.DeserializeLogEntry(data)
	if err != nil {
		t.Fatalf("Failed to deserialize log entry: %v", err)
	}

	if originalEntry.Operation != deserializedEntry.Operation || originalEntry.Key != deserializedEntry.Key || originalEntry.Value != deserializedEntry.Value || originalEntry.Table != deserializedEntry.Table {
		t.Errorf("Mismatch after serialization/deserialization: expected %+v, got %+v", originalEntry, deserializedEntry)
	}

}

func TestClose(t *testing.T) {
	// Create a temporary log file
	tmpfile, err := os.CreateTemp("", "append_only_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	log, err := kvstore.NewAppendOnlyLog(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create append-only log: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Failed to close log: %v", err)
	}

	// Ensure we can't append to a closed log
	entry := &kvstore.LogEntry{Operation: "PUT", Key: 1, Value: "one", Table: "table1"}
	if err := log.Append(entry); err == nil {
		t.Fatal("Expected error when appending to a closed log, but got nil")
	}
}

func TestReplayEmptyLog(t *testing.T) {
	// Create a temporary log file
	tmpfile, err := os.CreateTemp("", "append_only_log_test_empty")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	log, err := kvstore.NewAppendOnlyLog(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create append-only log: %v", err)
	}
	defer log.Close()

	// Replay the log and verify entries
	replayedEntries, err := log.Replay()
	if err != nil {
		t.Fatalf("Failed to replay log: %v", err)
	}

	if len(replayedEntries) != 0 {
		t.Fatalf("Expected 0 entries, got %d", len(replayedEntries))
	}
}

func TestCorruptedLogEntries(t *testing.T) {
	// Create a temporary log file
	tmpfile, err := os.CreateTemp("", "corrupt_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	log, err := kvstore.NewAppendOnlyLog(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create append-only log: %v", err)
	}
	defer log.Close()

	// Append a valid entry and then corrupted data
	validEntry := &kvstore.LogEntry{Operation: "PUT", Key: 1, Value: "one", Table: "table1"}
	err = log.Append(validEntry)
	if err != nil {
		t.Fatalf("Failed to append log entry: %v", err)
	}

	// Write corrupted data
	if _, err := log.WriteString("corrupted data\n"); err != nil {
		t.Fatalf("Failed to write corrupted data: %v", err)
	}

	// Replay the log and verify entries
	replayedEntries, err := log.Replay()
	if err != nil {
		t.Fatalf("Failed to replay log: %v", err)
	}

	// Ensure the valid entry is preserved
	if len(replayedEntries) != 1 {
		t.Fatalf("Expected 1 valid entry, got %d", len(replayedEntries))
	}

	if replayedEntries[0].Operation != validEntry.Operation || replayedEntries[0].Key != validEntry.Key || replayedEntries[0].Value != validEntry.Value || replayedEntries[0].Table != validEntry.Table {
		t.Errorf("Mismatch in valid entry after corruption: expected %+v, got %+v", validEntry, replayedEntries[0])
	}
}

func TestConcurrentAppend(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "concurrent_append_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	log, err := kvstore.NewAppendOnlyLog(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create append-only log: %v", err)
	}
	defer log.Close()

	var wg sync.WaitGroup
	entryCount := 100

	for i := 1; i <= entryCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entry := &kvstore.LogEntry{Operation: "PUT", Key: i, Value: "some value", Table: "table1"}
			if err := log.Append(entry); err != nil {
				t.Errorf("Failed to append log entry: %v", err)
			}
		}(i)
	}

	wg.Wait()

	replayedEntries, err := log.Replay()
	if err != nil {
		t.Fatalf("Failed to replay log: %v", err)
	}

	if len(replayedEntries) != entryCount {
		t.Fatalf("Expected %d entries, got %d", entryCount, len(replayedEntries))
	}

	for i := 1; i <= entryCount; i++ {
		found := false
		for _, entry := range replayedEntries {
			if entry.Key == i && entry.Table == "table1" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected to find entry with key %d and table 'table1', but it was not found", i)
		}
	}
}
