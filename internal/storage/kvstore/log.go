package kvstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// LogEntry represents an operation in the append-only log.
type LogEntry struct {
	Operation string `json:"operation"` // "PUT" or "DELETE"
	Key       int    `json:"key"`
	Value     string `json:"value,omitempty"` // Only used for "PUT" operations
	Table     string `json:"table"`           // Table name
}

// Serialize converts a LogEntry to a byte slice for writing to the log.
func (entry *LogEntry) Serialize() ([]byte, error) {
	return json.Marshal(entry)
}

// DeserializeLogEntry converts a byte slice back into a LogEntry.
func DeserializeLogEntry(data []byte) (*LogEntry, error) {
	var entry LogEntry
	err := json.Unmarshal(data, &entry)
	return &entry, err
}

// AppendOnlyLog manages an append-only log file.
type AppendOnlyLog struct {
	file *os.File
}

// NewAppendOnlyLog opens or creates the log file.
func NewAppendOnlyLog(filename string) (*AppendOnlyLog, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &AppendOnlyLog{file: file}, nil
}

// Append writes a LogEntry to the log file.
func (log *AppendOnlyLog) Append(entry *LogEntry) error {
	data, err := entry.Serialize()
	if err != nil {
		return err
	}
	data = append(data, '\n') // Add newline for readability
	_, err = log.file.Write(data)
	return err
}

// Replay reads all log entries from the beginning of the file.
func (log *AppendOnlyLog) Replay() ([]*LogEntry, error) {
	_, err := log.file.Seek(0, 0) // Move to the beginning of the file
	if err != nil {
		return nil, err
	}

	entries := []*LogEntry{}
	scanner := bufio.NewScanner(log.file)
	for scanner.Scan() {
		entry, err := DeserializeLogEntry(scanner.Bytes())
		if err != nil {
			// Log the error and continue with the next entry
			fmt.Printf("Warning: Skipping corrupted log entry: %v\n", err)
			continue
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func (log *AppendOnlyLog) WriteString(s string) (int, error) {
	return log.file.WriteString(s)
}

// Close closes the log file.
func (log *AppendOnlyLog) Close() error {
	return log.file.Close()
}
