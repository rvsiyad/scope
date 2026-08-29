package collector

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// The dashboard must be reachable on a bare collector — no tsdb, no trace
// store, no WAL — because serving static files has no store dependency,
// and the demo path ("open the collector's address") should never 404.
func TestDashboardServed(t *testing.T) {
	srv, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/ui/", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "<title>Scope") {
		t.Fatalf("GET /ui/: code %d, want the dashboard page", rec.Code)
	}

	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 302 || rec.Header().Get("Location") != "/ui/" {
		t.Fatalf("GET /: got %d -> %q, want a 302 to /ui/", rec.Code, rec.Header().Get("Location"))
	}
}
