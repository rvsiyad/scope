package ui

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// The embed either compiled in the whole static tree or it didn't; one
// request per file class (page, script, stylesheet) proves it did and that
// the content types survive the trip — what a browser needs to execute
// rather than display them.
func TestHandlerServesStaticTree(t *testing.T) {
	h := Handler()
	for _, tc := range []struct {
		path, contentType, mustContain string
	}{
		{"/", "text/html", "<title>Scope"},
		{"/chart.js", "text/javascript", "makeChart"},
		{"/dashboard.js", "text/javascript", "query_range"},
		{"/style.css", "text/css", "--series-1"},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", tc.path, nil))
		if rec.Code != 200 {
			t.Fatalf("GET %s: got %d", tc.path, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, tc.contentType) {
			t.Errorf("GET %s: content type %q, want %q", tc.path, ct, tc.contentType)
		}
		if !strings.Contains(rec.Body.String(), tc.mustContain) {
			t.Errorf("GET %s: body missing %q", tc.path, tc.mustContain)
		}
	}
}
