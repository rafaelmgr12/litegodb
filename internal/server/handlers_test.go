package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rafaelmgr12/litegodb/config"
	"github.com/stretchr/testify/require"
)

func TestPingHandler(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()

	s.pingHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.StatusCode)
	}

	body := w.Body.String()
	if body != "pong" {
		t.Errorf("Expected body 'pong', got '%s'", body)
	}
}

func TestPutHandlerSuccess(t *testing.T) {
	s := &Server{
		DB: &fakeDB{
			putFn: func(table string, key int, value string) error {
				return nil
			},
		},
	}

	body := bytes.NewBufferString(`{"table":"t","key":1,"value":"v"}`)
	req := httptest.NewRequest(http.MethodPost, "/put", body)
	w := httptest.NewRecorder()

	s.putHandler(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestPutHandlerInvalidJson(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodPost, "/put", bytes.NewBufferString("invalid"))
	w := httptest.NewRecorder()

	s.putHandler(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPutHandlerMethodNotAllowed(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/put", nil)
	w := httptest.NewRecorder()

	s.putHandler(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestGetHandlerSuccess(t *testing.T) {
	s := &Server{
		DB: &fakeDB{
			getFn: func(table string, key int) (string, bool, error) {
				return "rafael", true, nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/get?table=t&key=1", nil)
	w := httptest.NewRecorder()

	s.getHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	require.Equal(t, "rafael", resp["value"])

}

func TestGetHandlerKeyNotFound(t *testing.T) {
	s := &Server{
		DB: &fakeDB{
			getFn: func(string, int) (string, bool, error) {
				return "", false, nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/get?table=t&key=1", nil)
	w := httptest.NewRecorder()

	s.getHandler(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetHandlerInvalidKey(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/get?table=t&key=abc", nil)
	w := httptest.NewRecorder()

	s.getHandler(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDeleteHandlerSuccess(t *testing.T) {
	s := &Server{
		DB: &fakeDB{
			deleteFn: func(table string, key int) error {
				return nil
			},
		},
	}

	body := bytes.NewBufferString(`{"table":"t","key":1}`)
	req := httptest.NewRequest(http.MethodDelete, "/delete", body)
	w := httptest.NewRecorder()

	s.deleteHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestDeleteHandlerInvalidJSON(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodDelete, "/delete", bytes.NewBufferString("invalid"))
	w := httptest.NewRecorder()

	s.deleteHandler(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWithAuthTokenMissing(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{AuthToken: "secret"}}
	s := &Server{Cfg: cfg}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	called := false
	protected := s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	protected(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.False(t, called)
}

func TestWithAuthTokenValid(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{AuthToken: "secret"}}
	s := &Server{Cfg: cfg}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()

	called := false
	protected := s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	protected(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, called)
}

func TestSQLHandlerEmptyQuery(t *testing.T) {
	s := &Server{}
	body := bytes.NewBufferString(`{"query": ""}`)
	req := httptest.NewRequest(http.MethodPost, "/sql", body)
	w := httptest.NewRecorder()

	s.sqlHandler(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpdateHandlerSuccess(t *testing.T) {
	s := &Server{
		DB: &fakeDB{
			updateFn: func(table string, key int, value string) error {
				return nil
			},
		},
	}

	body := bytes.NewBufferString(`{"table":"users","key":1,"value":"updated_value"}`)
	req := httptest.NewRequest(http.MethodPut, "/update", body)
	w := httptest.NewRecorder()

	s.updateHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestUpdateHandlerInvalidJSON(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodPut, "/update", bytes.NewBufferString("invalid"))
	w := httptest.NewRecorder()

	s.updateHandler(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpdateHandlerMethodNotAllowed(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/update", nil)
	w := httptest.NewRecorder()

	s.updateHandler(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestUpdateHandlerWithError(t *testing.T) {
	s := &Server{
		DB: &fakeDB{
			updateFn: func(table string, key int, value string) error {
				return fmt.Errorf("update failed")
			},
		},
	}

	body := bytes.NewBufferString(`{"table":"users","key":1,"value":"updated_value"}`)
	req := httptest.NewRequest(http.MethodPut, "/update", body)
	w := httptest.NewRecorder()

	s.updateHandler(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
