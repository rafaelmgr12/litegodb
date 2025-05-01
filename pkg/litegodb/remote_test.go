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

func TestRemoteAdapter_PutGetDelete(t *testing.T) {
	// Fake in-memory database to simulate server-side
	db := make(map[string]map[int]string)

	// Create a fake HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/put":
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

		case "/get":
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

		case "/delete":
			var req struct {
				Table string `json:"table"`
				Key   int    `json:"key"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			if db[req.Table] != nil {
				delete(db[req.Table], req.Key)
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// Connect the remote client to the fake server
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
