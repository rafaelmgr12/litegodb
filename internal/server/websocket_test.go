package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
	"github.com/stretchr/testify/require"
)

type fakeDB struct {
	putFn    func(string, int, string) error
	getFn    func(string, int) (string, bool, error)
	deleteFn func(string, int) error
}

func (f *fakeDB) Put(table string, key int, value string) error {
	if f.putFn != nil {
		return f.putFn(table, key, value)
	}
	return nil
}

func (f *fakeDB) Get(table string, key int) (string, bool, error) {
	if f.getFn != nil {
		return f.getFn(table, key)
	}
	return "", false, nil
}

func (f *fakeDB) Delete(table string, key int) error {
	if f.deleteFn != nil {
		return f.deleteFn(table, key)
	}
	return nil
}

func (f *fakeDB) Flush(table string) error               { return nil }
func (f *fakeDB) CreateTable(table string, d int) error  { return nil }
func (f *fakeDB) DropTable(table string) error           { return nil }
func (f *fakeDB) Load() error                            { return nil }
func (f *fakeDB) Close() error                           { return nil }
func (f *fakeDB) BeginTransaction() litegodb.Transaction { return nil }

func TestWebSocketHandler(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	// Convert HTTP server URL to WebSocket URL
	wsURL := "ws" + ts.URL[4:]

	// Connect to WebSocket server
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket server: %v", err)
	}
	defer conn.Close()

	// Test sending a ping message
	message := WSRequest{Op: "ping"}
	if err := conn.WriteJSON(message); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	var response WSResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if response.Status != "ok" || response.Message != "pong" {
		t.Errorf("Unexpected response: %+v", response)
	}
}

func TestWebSocketHandlerPutOperation(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
		DB:          &fakeDB{},
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket server: %v", err)
	}
	defer conn.Close()

	message := WSRequest{Op: "put", Table: "testTable", Key: 1, Value: "testValue"}
	if err := conn.WriteJSON(message); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	var response WSResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if response.Status != "ok" {
		t.Errorf("Unexpected response: %+v", response)
	}
}

func TestWebSocketHandlerUnknownOperation(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket server: %v", err)
	}
	defer conn.Close()

	message := WSRequest{Op: "unknown"}
	if err := conn.WriteJSON(message); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	var response WSResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if response.Status != "error" || response.Message != "unknown operation" {
		t.Errorf("Unexpected response: %+v", response)
	}
}

func TestWebSocketHandlerGetOperation(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
		DB: &fakeDB{
			getFn: func(table string, key int) (string, bool, error) {
				if key == 1 {
					return "value1", true, nil
				}
				return "", false, nil
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	message := WSRequest{Op: "get", Table: "test", Key: 1}
	require.NoError(t, conn.WriteJSON(message))

	var response WSResponse
	require.NoError(t, conn.ReadJSON(&response))
	require.Equal(t, "ok", response.Status)
	require.Equal(t, "value1", response.Value)

}

func TestWebSocketHandlerGetKeyNotFound(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
		DB: &fakeDB{
			getFn: func(table string, key int) (string, bool, error) {
				return "", false, nil
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[4:], nil)
	require.NoError(t, err)
	defer conn.Close()

	msg := WSRequest{Op: "get", Table: "test", Key: 1}
	require.NoError(t, conn.WriteJSON(msg))

	var response WSResponse
	require.NoError(t, conn.ReadJSON(&response))
	require.Equal(t, "error", response.Status)
	require.Equal(t, "key not found", response.Message)
}

func TestWebSocketHandlerDeleteWithError(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
		DB: &fakeDB{
			deleteFn: func(table string, key int) error {
				return fmt.Errorf("forced delete failure")
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[4:], nil)
	require.NoError(t, err)
	defer conn.Close()

	msg := WSRequest{Op: "delete", Table: "test", Key: 1}
	require.NoError(t, conn.WriteJSON(msg))

	var res WSResponse
	require.NoError(t, conn.ReadJSON(&res))
	require.Equal(t, "error", res.Status)
	require.Contains(t, res.Message, "forced delete failure")
}

func TestWebSocketHandlerPutWithError(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
		DB: &fakeDB{
			putFn: func(table string, key int, value string) error {
				return fmt.Errorf("simulated put failure")
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[4:], nil)
	require.NoError(t, err)
	defer conn.Close()

	msg := WSRequest{Op: "put", Table: "t", Key: 1, Value: "v"}
	require.NoError(t, conn.WriteJSON(msg))

	var res WSResponse
	require.NoError(t, conn.ReadJSON(&res))
	require.Equal(t, "error", res.Status)
	require.Contains(t, res.Message, "simulated put failure")
}

func TestWebSocketConcurrentConnections(t *testing.T) {
	s := &Server{
		connections: make(map[*websocket.Conn]bool),
		DB:          &fakeDB{},
	}
	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()

	url := "ws" + ts.URL[4:]
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			require.NoError(t, err)
			defer conn.Close()

			msg := WSRequest{Op: "ping"}
			require.NoError(t, conn.WriteJSON(msg))

			var res WSResponse
			require.NoError(t, conn.ReadJSON(&res))
			require.Equal(t, "ok", res.Status)
			require.Equal(t, "pong", res.Message)
		}(i)
	}

	wg.Wait()
}
