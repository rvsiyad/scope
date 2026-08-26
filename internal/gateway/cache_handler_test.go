package gateway

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// countingServer builds a tenant-configured server in front of a fake Ollama
// that counts provider calls — the cache's job is making that number stay
// flat while requests keep succeeding.
func countingServer(t *testing.T, tokensPerMinute int) (*Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		calls.Add(1)
		if req.Stream {
			enc := json.NewEncoder(w)
			enc.Encode(ollamaChatResponse{Model: req.Model, Message: Message{Content: "hel"}})
			enc.Encode(ollamaChatResponse{Model: req.Model, Message: Message{Content: "lo"}})
			enc.Encode(ollamaChatResponse{
				Model: req.Model, Done: true, DoneReason: "stop",
				PromptEvalCount: 10, EvalCount: 5,
			})
			return
		}
		json.NewEncoder(w).Encode(ollamaChatResponse{
			Model: req.Model, Message: Message{Role: "assistant", Content: "hello"},
			Done: true, DoneReason: "stop", PromptEvalCount: 10, EvalCount: 5,
		})
	})
	srv := New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants:    []TenantConfig{{Name: "acme", APIKey: "sk-acme", TokensPerMinute: tokensPerMinute}},
	})
	return srv, &calls
}

const deterministicChat = `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0,"max_tokens":50}`

func TestCachedHitSkipsProviderAndBudget(t *testing.T) {
	srv, calls := countingServer(t, 1000)

	if rec := postChatAs(t, srv, "sk-acme", deterministicChat); rec.Code != http.StatusOK {
		t.Fatalf("first request: status = %d, body = %s", rec.Code, rec.Body)
	}
	afterMiss := srv.tenants["sk-acme"].budget.Available()

	rec := postChatAs(t, srv, "sk-acme", deterministicChat)
	if rec.Code != http.StatusOK {
		t.Fatalf("second request: status = %d, body = %s", rec.Code, rec.Body)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1 (second request must hit the cache)", got)
	}
	// A hit consumes no provider tokens, so it must not touch the budget.
	if got := srv.tenants["sk-acme"].budget.Available(); got != afterMiss {
		t.Fatalf("hit changed the budget: %d -> %d", afterMiss, got)
	}
	var resp ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Choices[0].Message.Content != "hello" {
		t.Fatalf("hit body wrong: %s", rec.Body)
	}
}

func TestNonDeterministicRequestBypassesCache(t *testing.T) {
	srv, calls := countingServer(t, 1000)
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`

	postChatAs(t, srv, "sk-acme", body)
	postChatAs(t, srv, "sk-acme", body)
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want 2 (default temperature must not cache)", got)
	}
}

func TestStreamedRequestFillsAndHitsCache(t *testing.T) {
	srv, calls := countingServer(t, 1000)
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0,"max_tokens":50,"stream":true}`

	if rec := postChatAs(t, srv, "sk-acme", body); !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("first stream did not finish cleanly: %s", rec.Body)
	}

	rec := postChatAs(t, srv, "sk-acme", body)
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1 (second stream must replay from cache)", got)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("replay Content-Type = %q, want SSE", ct)
	}
	if !strings.Contains(rec.Body.String(), `"content":"hello"`) {
		t.Fatalf("replay missing assembled content: %s", rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"finish_reason":"stop"`) {
		t.Fatalf("replay missing finish reason: %s", rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("replay must end with [DONE]: %s", rec.Body)
	}
}

func TestStreamAndJSONShareOneEntry(t *testing.T) {
	srv, calls := countingServer(t, 1000)
	streamed := `{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0,"max_tokens":50,"stream":true}`

	// Filled by a streamed request, served to a JSON one: same completion.
	postChatAs(t, srv, "sk-acme", streamed)
	rec := postChatAs(t, srv, "sk-acme", deterministicChat)
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1 (JSON request must hit the stream-filled entry)", got)
	}
	var resp ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Choices[0].Message.Content != "hello" {
		t.Fatalf("JSON hit body wrong: %s", rec.Body)
	}
}

func TestCachedHitServedDespiteExhaustedBudget(t *testing.T) {
	// Budget admits exactly one request (estimate 4+50=54, actual 15). A
	// second, uncached request is 429'd — but the cached one still serves,
	// because it costs no provider tokens.
	srv, _ := countingServer(t, 60)
	if rec := postChatAs(t, srv, "sk-acme", deterministicChat); rec.Code != http.StatusOK {
		t.Fatalf("first request: status = %d", rec.Code)
	}
	// Drain the budget with a different (uncacheable) request shape.
	drain := `{"model":"m","messages":[{"role":"user","content":"drain"}],"max_tokens":40}`
	postChatAs(t, srv, "sk-acme", drain)

	if rec := postChatAs(t, srv, "sk-acme", drain); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("uncached request on drained budget: status = %d, want 429", rec.Code)
	}
	if rec := postChatAs(t, srv, "sk-acme", deterministicChat); rec.Code != http.StatusOK {
		t.Fatalf("cached request on drained budget: status = %d, want 200", rec.Code)
	}
}

func TestTenantsDoNotShareCacheEntries(t *testing.T) {
	var calls atomic.Int64
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		calls.Add(1)
		json.NewEncoder(w).Encode(ollamaChatResponse{
			Model: req.Model, Message: Message{Content: "ok"}, Done: true, DoneReason: "stop",
		})
	})
	srv := New(Config{
		OllamaURLs: []string{ollama.URL},
		Tenants: []TenantConfig{
			{Name: "acme", APIKey: "sk-acme", TokensPerMinute: 1000},
			{Name: "globex", APIKey: "sk-globex", TokensPerMinute: 1000},
		},
	})

	postChatAs(t, srv, "sk-acme", deterministicChat)
	postChatAs(t, srv, "sk-globex", deterministicChat)
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want 2 (tenants must not share entries)", got)
	}
}

func TestHealthzReportsCache(t *testing.T) {
	srv, _ := countingServer(t, 1000)
	postChatAs(t, srv, "sk-acme", deterministicChat) // miss + fill
	postChatAs(t, srv, "sk-acme", deterministicChat) // hit, saves 15

	st := srv.cache.Status()
	if st.Hits != 1 || st.Misses != 1 || st.Entries != 1 {
		t.Fatalf("cache status = %+v, want 1 hit / 1 miss / 1 entry", st)
	}
	if st.TokensSaved != 15 {
		t.Fatalf("tokens saved = %d, want 15 (the provider-reported usage)", st.TokensSaved)
	}
}
