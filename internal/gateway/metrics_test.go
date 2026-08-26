package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzReportsBudgets(t *testing.T) {
	// Budget 60 (estimate 54 fits once): first request admitted and settled
	// at the provider's 55, second rejected.
	srv := tenantServer(t, 60, 50, 5)
	postChatAs(t, srv, "sk-acme", smallChat)
	postChatAs(t, srv, "sk-acme", smallChat)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	var st healthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("healthz body not JSON: %s", rec.Body)
	}
	if len(st.Budgets) != 1 {
		t.Fatalf("budgets = %+v, want one tenant", st.Budgets)
	}
	got := st.Budgets[0]
	want := TenantStatus{
		Name: "acme", Available: 5, Capacity: 60,
		Admitted: 1, Rejected: 1, TokensCharged: 55,
	}
	if got != want {
		t.Fatalf("budget status = %+v, want %+v", got, want)
	}
}

func TestHealthzOmitsBudgetsInOpenMode(t *testing.T) {
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {})
	srv := New(Config{OllamaURLs: []string{ollama.URL}})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("healthz body not JSON: %s", rec.Body)
	}
	if _, present := raw["budgets"]; present {
		t.Fatalf("open mode must omit budgets entirely: %s", rec.Body)
	}
}

func TestBudgetsAreSortedByName(t *testing.T) {
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {})
	srv := New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants: []TenantConfig{
			{Name: "zulu", APIKey: "sk-z", TokensPerMinute: 100},
			{Name: "acme", APIKey: "sk-a", TokensPerMinute: 100},
		},
	})

	st := srv.tenantStatus()
	if len(st) != 2 || st[0].Name != "acme" || st[1].Name != "zulu" {
		t.Fatalf("tenantStatus() = %+v, want sorted by name", st)
	}
}
