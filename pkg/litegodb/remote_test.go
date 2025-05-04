package litegodb_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
	"github.com/stretchr/testify/assert"
)

func setupFakeServer() (*httptest.Server, map[string]map[int]string) {
	db := make(map[string]map[int]string)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/put":
			handlePut(w, r, db)
		case "/get":
			handleGet(w, r, db)
		case "/delete":
			handleDelete(w, r, db)
		case "/update":
			handleUpdate(w, r, db)
		default:
			http.NotFound(w, r)
		}
	}))

	return server, db
}

// Handlers for different operations
func handlePut(w http.ResponseWriter, r *http.Request, db map[string]map[int]string) {
	var req struct {
		Table string `json:"table"`
		Key   int    `json:"key"`
		Value string `json:"value"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if db[req.Table] == nil {
		db[req.Table] = make(map[int]string)
	}
	db[req.Table][req.Key] = req.Value
	w.WriteHeader(http.StatusOK)
}

func handleGet(w http.ResponseWriter, r *http.Request, db map[string]map[int]string) {
	table := r.URL.Query().Get("table")
	keyParam := r.URL.Query().Get("key")
	var key int
	_, _ = fmt.Sscanf(keyParam, "%d", &key)

	value, exists := db[table][key]
	if !exists {
		http.NotFound(w, r)
		return
	}

	resp := map[string]string{"value": value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleDelete(w http.ResponseWriter, r *http.Request, db map[string]map[int]string) {
	var req struct {
		Table string `json:"table"`
		Key   int    `json:"key"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	if db[req.Table] != nil {
		delete(db[req.Table], req.Key)
	}
	w.WriteHeader(http.StatusOK)
}

func handleUpdate(w http.ResponseWriter, r *http.Request, db map[string]map[int]string) {
	var req struct {
		Table string `json:"table"`
		Key   int    `json:"key"`
		Value string `json:"value"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	if db[req.Table] == nil {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	if _, exists := db[req.Table][req.Key]; !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	db[req.Table][req.Key] = req.Value
	w.WriteHeader(http.StatusOK)
}

// Refactored tests
func TestRemoteAdapter_PutGetDelete(t *testing.T) {
	server, _ := setupFakeServer()
	defer server.Close()

	remoteDB, err := litegodb.OpenRemote(server.URL)
	assert.NoError(t, err)

	// Test PUT
	err = remoteDB.Put("users", 1, "rafael")
	assert.NoError(t, err)

	// Test GET (key exists)
	val, found, err := remoteDB.Get("users", 1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "rafael", val)

	// Test GET (key does not exist)
	_, found, err = remoteDB.Get("users", 999)
	assert.NoError(t, err)
	assert.False(t, found)

	// Test DELETE
	err = remoteDB.Delete("users", 1)
	assert.NoError(t, err)

	// Test GET after DELETE
	_, found, err = remoteDB.Get("users", 1)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestRemoteAdapter_Update(t *testing.T) {
	server, db := setupFakeServer()
	defer server.Close()

	remoteDB, err := litegodb.OpenRemote(server.URL)
	assert.NoError(t, err)

	// Prepare the table and key-value pair
	table := "users"
	key := 1
	initialValue := "rafael"
	updatedValue := "rafael_updated"

	// Simulate initial PUT
	if db[table] == nil {
		db[table] = make(map[int]string)
	}
	db[table][key] = initialValue

	// Test UPDATE
	err = remoteDB.Update(table, key, updatedValue)
	assert.NoError(t, err)

	// Verify the update
	assert.Equal(t, updatedValue, db[table][key])
}

func TestRemoteAdapter_PutEdgeCases(t *testing.T) {
	server, db := setupFakeServer()
	defer server.Close()

	remoteDB, err := litegodb.OpenRemote(server.URL)
	assert.NoError(t, err)

	// Test PUT with an empty table name
	err = remoteDB.Put("", 1, "value")
	assert.Error(t, err)

	// Test PUT with a negative key
	err = remoteDB.Put("users", -1, "value")
	assert.NoError(t, err)
	assert.Equal(t, "value", db["users"][-1])

	// Test PUT with an empty value
	err = remoteDB.Put("users", 2, "")
	assert.NoError(t, err)
	assert.Equal(t, "", db["users"][2])
}

func TestRemoteAdapter_GetEdgeCases(t *testing.T) {
	server, db := setupFakeServer()
	defer server.Close()

	remoteDB, err := litegodb.OpenRemote(server.URL)
	assert.NoError(t, err)

	// Prepare the table and key-value pair
	table := "users"
	key := 1
	value := "rafael"
	db[table] = map[int]string{key: value}

	// Test GET with an empty table name
	_, found, err := remoteDB.Get("", key)
	assert.Error(t, err)
	assert.False(t, found)

	// Test GET with a non-existent table
	_, found, err = remoteDB.Get("nonexistent", key)
	assert.NoError(t, err)
	assert.False(t, found)

	// Test GET with a non-existent key
	_, found, err = remoteDB.Get(table, 999)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestRemoteAdapter_DeleteEdgeCases(t *testing.T) {
	server, db := setupFakeServer()
	defer server.Close()

	remoteDB, err := litegodb.OpenRemote(server.URL)
	assert.NoError(t, err)

	// Prepare the table and key-value pair
	table := "users"
	key := 1
	value := "rafael"
	db[table] = map[int]string{key: value}

	// Test DELETE with an empty table name
	err = remoteDB.Delete("", key)
	assert.Error(t, err)

	// Test DELETE with a non-existent table
	err = remoteDB.Delete("nonexistent", key)
	assert.NoError(t, err)

	// Test DELETE with a non-existent key
	err = remoteDB.Delete(table, 999)
	assert.NoError(t, err)

	// Test DELETE with a valid key
	err = remoteDB.Delete(table, key)
	assert.NoError(t, err)
	assert.NotContains(t, db[table], key)
}

func TestRemoteAdapter_UpdateEdgeCases(t *testing.T) {
	server, db := setupFakeServer()
	defer server.Close()

	remoteDB, err := litegodb.OpenRemote(server.URL)
	assert.NoError(t, err)

	// Prepare the table and key-value pair
	table := "users"
	key := 1
	initialValue := "rafael"
	db[table] = map[int]string{key: initialValue}

	// Test UPDATE with an empty table name
	err = remoteDB.Update("", key, "new_value")
	assert.Error(t, err)

	// Test UPDATE with a non-existent table
	err = remoteDB.Update("nonexistent", key, "new_value")
	assert.Error(t, err)

	// Test UPDATE with a non-existent key
	err = remoteDB.Update(table, 999, "new_value")
	assert.Error(t, err)

	// Test UPDATE with an empty value
	err = remoteDB.Update(table, key, "")
	assert.NoError(t, err)
	assert.Equal(t, "", db[table][key])
}
