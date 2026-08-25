package gateway

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
