package litegodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// remoteAdapter represents a remote connection to a LiteGoDB server.
type remoteAdapter struct {
	baseURL    string
	httpClient *http.Client
}

// remoteTransaction represents a transaction for the remote LiteGoDB server.
type remoteTransaction struct {
	client     *http.Client
	baseURL    string
	operations []map[string]interface{}
}

// OpenRemote opens a connection to a remote LiteGoDB server.
// It returns a DB interface that can be used to interact with the remote database.
func OpenRemote(baseURL string) (DB, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	return &remoteAdapter{
		baseURL:    baseURL,
		httpClient: client,
	}, nil
}

// Put stores a key-value pair in the specified table on the remote LiteGoDB server.
// It returns an error if the operation fails.
func (r *remoteAdapter) Put(table string, key int, value string) error {
	reqBody := map[string]interface{}{
		"table": table,
		"key":   key,
		"value": value,
	}
	return r.post("/put", reqBody)
}

// Get retrieves the value for the specified key from the specified table on the remote LiteGoDB server.
// It returns the value, a boolean indicating whether the key was found, and an error if the operation fails.
func (r *remoteAdapter) Get(table string, key int) (string, bool, error) {
	url := fmt.Sprintf("%s/get?table=%s&key=%d", r.baseURL, table, key)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("get failed: %s", resp.Status)
	}

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", false, err
	}

	return body.Value, true, nil
}

// Delete removes the key-value pair with the specified key from the specified table on the remote LiteGoDB server.
// It returns an error if the operation fails.
func (r *remoteAdapter) Delete(table string, key int) error {
	reqBody := map[string]interface{}{
		"table": table,
		"key":   key,
	}
	return r.post("/delete", reqBody)
}

// Flush simulates flushing the specified table on the remote LiteGoDB server.
// In a remote setup, flush might be a no-op or trigger a server-side flush.
// It returns an error if the operation fails.
func (r *remoteAdapter) Flush(table string) error {
	// In a remote setup, flush might be a no-op (or trigger server-side flush).
	// For now, we'll just simulate it.
	return nil
}

// CreateTable simulates creating a table with the specified degree on the remote LiteGoDB server.
// This function is optional and can be implemented in the future if server-side support is added.
// It returns an error if the operation fails.
func (r *remoteAdapter) CreateTable(table string, degree int) error {
	// Optional future implementation: server-side CreateTable support
	return nil
}

// DropTable simulates dropping the specified table on the remote LiteGoDB server.
// This function is optional and can be implemented in the future if server-side support is added.
// It returns an error if the operation fails.
func (r *remoteAdapter) DropTable(table string) error {
	// Optional future implementation: server-side DropTable support
	return nil
}

// Load simulates loading the remote LiteGoDB.
// This function is not needed in the remote client since the remote server handles persistence.
// It returns an error if the operation fails.
func (r *remoteAdapter) Load() error {
	// Not needed in remote client
	return nil
}

// Close simulates closing the connection to the remote LiteGoDB.
// Since there is no persistent connection, this function does nothing.
// It returns an error if the operation fails.
func (r *remoteAdapter) Close() error {
	// No persistent connection, so nothing to close
	return nil
}

// BeginTransaction starts a new transaction on the remote LiteGoDB server.
// It returns a Transaction object that can be used to queue operations.
func (r *remoteAdapter) BeginTransaction() Transaction {
	return &remoteTransaction{
		client:     r.httpClient,
		baseURL:    r.baseURL,
		operations: []map[string]interface{}{},
	}
}

// PutBatch queues a PUT operation in the transaction for the specified table and key-value pair.
func (rt *remoteTransaction) PutBatch(table string, key int, value string) {
	rt.operations = append(rt.operations, map[string]interface{}{
		"op":    "put",
		"table": table,
		"key":   key,
		"value": value,
	})
}

// DeleteBatch queues a DELETE operation in the transaction for the specified table and key.
func (rt *remoteTransaction) DeleteBatch(table string, key int) {
	rt.operations = append(rt.operations, map[string]interface{}{
		"op":    "delete",
		"table": table,
		"key":   key,
	})
}

// Commit sends all queued operations in the transaction to the remote LiteGoDB server.
// It returns an error if the commit operation fails.
func (rt *remoteTransaction) Commit() error {
	payload := map[string]interface{}{
		"operations": rt.operations,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := rt.client.Post(rt.baseURL+"/tx/commit", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("commit failed: %s", resp.Status)
	}

	return nil
}

// Rollback discards all queued operations in the transaction.
func (rt *remoteTransaction) Rollback() {
	rt.operations = nil
}

// post sends a POST request to the specified path with the provided body to the remote LiteGoDB server.
// It returns an error if the operation fails.
func (r *remoteAdapter) post(path string, body map[string]interface{}) error {
	url := r.baseURL + path
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("post %s failed: %s", path, resp.Status)
	}

	return nil
}
