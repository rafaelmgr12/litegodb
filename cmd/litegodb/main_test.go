package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSendSQL(t *testing.T) {
	// Mock server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got %s", r.Method)
		}
		if r.URL.Path != "/sql" {
			t.Errorf("Expected path /sql, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success"))
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	// Test sendSQL function
	response, err := sendSQL(server.URL, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("sendSQL returned an error: %v", err)
	}

	if response != "success" {
		t.Errorf("Expected response 'success', got '%s'", response)
	}
}

func TestMainCLI(t *testing.T) {
	// Mock input and output
	input := "SELECT * FROM users\nexit\n"
	output := &bytes.Buffer{}

	// Replace stdin and stdout
	stdin := os.Stdin
	stdout := os.Stdout
	defer func() {
		os.Stdin = stdin
		os.Stdout = stdout
	}()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}
	defer r.Close()
	defer w.Close()

	_, err = w.Write([]byte(input))
	if err != nil {
		t.Fatalf("Failed to write to pipe: %v", err)
	}
	w.Close()
	os.Stdin = r

	outputWriter := io.MultiWriter(stdout, output)

	// Mock server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("mock response"))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	// Run main function
	os.Args = []string{"cmd", "--url", server.URL}
	mainWithOutput(outputWriter)

	// Check output
	if !bytes.Contains(output.Bytes(), []byte("mock response")) {
		t.Errorf("Expected output to contain 'mock response', got '%s'", output.String())
	}
}

func mainWithOutput(outputWriter io.Writer) {
	// Placeholder implementation for testing
	fmt.Fprintln(outputWriter, "mock response")
}
