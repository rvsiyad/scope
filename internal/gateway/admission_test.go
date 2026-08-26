package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func postChatAs(t *testing.T, srv *Server, apiKey, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	srv.ServeHTTP(rec, req)
	return rec
}

// tenantServer builds a server with one tenant ("acme" / key "sk-acme") in
// front of a fake Ollama that answers with the given usage counts.
func tenantServer(t *testing.T, tokensPerMinute, promptTokens, evalTokens int) *Server {
	t.Helper()
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		json.NewEncoder(w).Encode(ollamaChatResponse{
			Model:           req.Model,
			Message:         Message{Role: "assistant", Content: "ok"},
			Done:            true,
			DoneReason:      "stop",
			PromptEvalCount: promptTokens,
			EvalCount:       evalTokens,
		})
	})
	return New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants:    []TenantConfig{{Name: "acme", APIKey: "sk-acme", TokensPerMinute: tokensPerMinute}},
	})
}

const smallChat = `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`

func TestOpenModeSkipsAuth(t *testing.T) {
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		json.NewEncoder(w).Encode(ollamaChatResponse{Message: Message{Content: "ok"}, Done: true})
	})
	srv := New(Config{OllamaURLs: []string{ollama.URL}})

	if rec := postChatAs(t, srv, "", smallChat); rec.Code != http.StatusOK {
		t.Fatalf("open mode without key: status = %d, body = %s", rec.Code, rec.Body)
	}
}

func TestMissingAndUnknownKeysAre401(t *testing.T) {
	srv := tenantServer(t, 1000, 10, 5)

	for name, key := range map[string]string{"missing": "", "unknown": "sk-wrong"} {
		rec := postChatAs(t, srv, key, smallChat)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s key: status = %d, want 401; body = %s", name, rec.Code, rec.Body)
		}
		var apiErr apiError
		if err := json.Unmarshal(rec.Body.Bytes(), &apiErr); err != nil || apiErr.Error.Code != "invalid_api_key" {
			t.Fatalf("%s key: body not the OpenAI error envelope: %s", name, rec.Body)
		}
	}
}

func TestAdmittedRequestSettlesToProviderUsage(t *testing.T) {
	srv := tenantServer(t, 1000, 12, 3) // real cost 15
	rec := postChatAs(t, srv, "sk-acme", smallChat)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	// The reserve debited the estimate (~59); settle must true it up to the
	// provider's reported 15.
	if got := srv.tenants["sk-acme"].budget.Available(); got != 985 {
		t.Fatalf("available = %d, want 985 (1000 - actual 15)", got)
	}
}

func TestExhaustedBudgetGets429WithRetryAfter(t *testing.T) {
	// Estimate for smallChat is 4 + len("hi")/4 + 50 = 54; a 60-token budget
	// admits one request, and the fake's usage (55) leaves 5 available.
	srv := tenantServer(t, 60, 50, 5)
	if rec := postChatAs(t, srv, "sk-acme", smallChat); rec.Code != http.StatusOK {
		t.Fatalf("first request: status = %d, body = %s", rec.Code, rec.Body)
	}

	rec := postChatAs(t, srv, "sk-acme", smallChat)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: status = %d, want 429; body = %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatalf("429 must carry Retry-After; headers = %v", rec.Header())
	}
	var apiErr apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &apiErr); err != nil || apiErr.Error.Code != "token_budget_exhausted" {
		t.Fatalf("429 body not the OpenAI error envelope: %s", rec.Body)
	}
}

func TestEstimateOverCapacityGets429WithoutRetryAfter(t *testing.T) {
	srv := tenantServer(t, 60, 0, 0)
	big := `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":500}`

	rec := postChatAs(t, srv, "sk-acme", big)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body = %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Fatal("never-admittable request must not promise a Retry-After")
	}
}

func TestRejectedRequestNeverReachesProvider(t *testing.T) {
	calls := 0
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		calls++
		json.NewEncoder(w).Encode(ollamaChatResponse{Message: Message{Content: "ok"}, Done: true})
	})
	srv := New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants:    []TenantConfig{{Name: "acme", APIKey: "sk-acme", TokensPerMinute: 10}},
	})

	if rec := postChatAs(t, srv, "sk-acme", smallChat); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if calls != 0 {
		t.Fatalf("provider saw %d calls from a rejected request, want 0", calls)
	}
}

func TestStreamingSettlesToFinalUsage(t *testing.T) {
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		enc := json.NewEncoder(w)
		enc.Encode(ollamaChatResponse{Model: req.Model, Message: Message{Content: "hel"}})
		enc.Encode(ollamaChatResponse{Model: req.Model, Message: Message{Content: "lo"}})
		enc.Encode(ollamaChatResponse{
			Model: req.Model, Done: true, DoneReason: "stop",
			PromptEvalCount: 12, EvalCount: 8,
		})
	})
	srv := New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants:    []TenantConfig{{Name: "acme", APIKey: "sk-acme", TokensPerMinute: 1000}},
	})

	rec := postChatAs(t, srv, "sk-acme",
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":50,"stream":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("stream did not finish cleanly: %s", rec.Body)
	}
	if got := srv.tenants["sk-acme"].budget.Available(); got != 980 {
		t.Fatalf("available = %d, want 980 (1000 - final usage 20)", got)
	}
}

func TestStreamWithoutUsageSettlesToMeteredContent(t *testing.T) {
	// Upstream dies before its done=true line: no usage ever arrives, so the
	// charge must be the prompt estimate plus the metered content.
	content := strings.Repeat("x", 400) // meters to 100 tokens
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		json.NewEncoder(w).Encode(ollamaChatResponse{Model: req.Model, Message: Message{Content: content}})
		// connection closes without a final chunk
	})
	srv := New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants:    []TenantConfig{{Name: "acme", APIKey: "sk-acme", TokensPerMinute: 1000}},
	})

	rec := postChatAs(t, srv, "sk-acme",
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":200,"stream":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	// Prompt estimate: 4 + len("hi")/4 = 4; metered content: 100.
	if got := srv.tenants["sk-acme"].budget.Available(); got != 896 {
		t.Fatalf("available = %d, want 896 (1000 - prompt 4 - metered 100)", got)
	}
}

func TestParseTenants(t *testing.T) {
	got, err := ParseTenants(" acme:sk-a:6000, globex:sk-g:1200 ")
	if err != nil {
		t.Fatal(err)
	}
	want := []TenantConfig{
		{Name: "acme", APIKey: "sk-a", TokensPerMinute: 6000},
		{Name: "globex", APIKey: "sk-g", TokensPerMinute: 1200},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if got, err := ParseTenants("  "); err != nil || got != nil {
		t.Fatalf("empty spec: got %v, %v; want nil, nil", got, err)
	}

	for _, bad := range []string{"acme", "acme:sk-a", "acme:sk-a:zero", "acme:sk-a:0", ":sk-a:10", "a:k:10,b:k:10"} {
		if _, err := ParseTenants(bad); err == nil {
			t.Errorf("ParseTenants(%q) accepted invalid spec", bad)
		}
	}
}
