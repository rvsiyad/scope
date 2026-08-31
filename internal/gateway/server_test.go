package gateway

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSPreflight(t *testing.T) {
	srv := New(Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("OPTIONS", "/v1/chat/completions", nil)
	req.Header.Set("Origin", "https://scope.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	h := rec.Header()
	if got := h.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Allow-Origin = %q, want *", got)
	}
	if got := h.Get("Access-Control-Allow-Headers"); got != "Authorization, Content-Type" {
		t.Fatalf("Allow-Headers = %q", got)
	}
}

func TestCORSHeadersOnEveryResponse(t *testing.T) {
	srv := New(Config{})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Allow-Origin = %q, want *", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Scope-Trace-Id, Retry-After" {
		t.Fatalf("Expose-Headers = %q", got)
	}
}

func TestHealthzAllDependenciesUp(t *testing.T) {
	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"version":"0.0.0"}`))
	}))
	defer ollama.Close()

	// A bare TCP listener stands in for Postgres: the health check only dials.
	pg, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	srv := New(Config{OllamaURLs: []string{ollama.URL}, PostgresAddr: pg.Addr().String()})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body)
	}
	var got healthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "ok" || got.Ollama != "ok" || got.Postgres != "ok" {
		t.Fatalf("unexpected health: %+v", got)
	}
}

func TestHealthzDegradedWhenOllamaDown(t *testing.T) {
	pg, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	// Port 1 is reserved and unbound: connection refused immediately.
	srv := New(Config{OllamaURLs: []string{"http://127.0.0.1:1"}, PostgresAddr: pg.Addr().String()})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var got healthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "degraded" || got.Ollama == "ok" {
		t.Fatalf("unexpected health: %+v", got)
	}
}
