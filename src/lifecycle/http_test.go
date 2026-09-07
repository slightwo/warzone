package lifecycle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestHandlerHealthReadyAndDrain(t *testing.T) {
	var mu sync.Mutex
	status := Status{ActiveConnections: 3}
	ready := true
	drainCalls := 0
	handler := NewHandler(Callbacks{
		Status: func() Status {
			mu.Lock()
			defer mu.Unlock()
			return status
		},
		Ready: func() bool {
			mu.Lock()
			defer mu.Unlock()
			return ready
		},
		Drain: func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			drainCalls++
			status.Draining = true
			ready = false
			return nil
		},
	})

	assertStatusCode(t, handler, http.MethodGet, "/healthz", http.StatusOK)
	assertStatusCode(t, handler, http.MethodGet, "/readyz", http.StatusOK)
	assertStatusCode(t, handler, http.MethodPost, "/drain", http.StatusAccepted)
	assertStatusCode(t, handler, http.MethodGet, "/healthz", http.StatusOK)
	assertStatusCode(t, handler, http.MethodGet, "/readyz", http.StatusServiceUnavailable)
	assertStatusCode(t, handler, http.MethodPost, "/drain", http.StatusAccepted)

	mu.Lock()
	defer mu.Unlock()
	if drainCalls != 2 || !status.Draining || ready {
		t.Fatalf("drain 后状态错误: calls=%d status=%+v ready=%t", drainCalls, status, ready)
	}
}

func TestHandlerRejectsUnsupportedMethods(t *testing.T) {
	handler := NewHandler(Callbacks{})
	assertStatusCode(t, handler, http.MethodPost, "/healthz", http.StatusMethodNotAllowed)
	assertStatusCode(t, handler, http.MethodPost, "/readyz", http.StatusMethodNotAllowed)
	assertStatusCode(t, handler, http.MethodGet, "/drain", http.StatusMethodNotAllowed)
}

func assertStatusCode(t *testing.T, handler http.Handler, method, path string, want int) {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if got := response.Code; got != want {
		t.Fatalf("%s %s status=%d, want %d body=%s", method, path, got, want, response.Body.String())
	}
}
