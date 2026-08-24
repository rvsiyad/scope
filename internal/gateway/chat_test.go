package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeOllama returns a test server speaking Ollama's native /api/chat dialect
// and captures the request it received, so tests can assert on the outbound
// translation as well as the inbound one.
func fakeOllama(t *testing.T, respond func(w http.ResponseWriter, req ollamaChatRequest)) (*httptest.Server, *ollamaChatRequest) {
	t.Helper()
	captured := &ollamaChatRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(captured); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		respond(w, *captured)
	}))
	t.Cleanup(srv.Close)
	return srv, captured
}

func postChat(t *testing.T, srv *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(rec, req)
	return rec
}

func TestChatCompletionTranslatesBothWays(t *testing.T) {
	ollama, captured := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		json.NewEncoder(w).Encode(ollamaChatResponse{
			Model:           req.Model,
			Message:         Message{Role: "assistant", Content: "hi there"},
			Done:            true,
			DoneReason:      "stop",
			PromptEvalCount: 12,
			EvalCount:       3,
		})
	})

	srv := New(Config{OllamaURL: ollama.URL})
	rec := postChat(t, srv, `{
		"model": "llama3.2:1b",
		"messages": [{"role":"user","content":"hello"}],
		"temperature": 0.2,
		"max_tokens": 50
	}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}

	// Outbound: OpenAI knobs must land in Ollama's dialect.
	if captured.Model != "llama3.2:1b" || len(captured.Messages) != 1 {
		t.Fatalf("outbound request mistranslated: %+v", captured)
	}
	if captured.Stream {
		t.Fatal("non-streaming request must set stream=false upstream")
	}
	if got := captured.Options["temperature"]; got != 0.2 {
		t.Fatalf("temperature = %v, want 0.2", got)
	}
	if got := captured.Options["num_predict"]; got != float64(50) {
		t.Fatalf("num_predict = %v, want 50", got)
	}

	// Inbound: Ollama's response must come back OpenAI-shaped.
	var resp ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.ID, "chatcmpl-") || resp.Object != "chat.completion" {
		t.Fatalf("bad envelope: %+v", resp)
	}
	c := resp.Choices[0]
	if c.Message.Content != "hi there" || c.FinishReason != "stop" {
		t.Fatalf("bad choice: %+v", c)
	}
	if resp.Usage.PromptTokens != 12 || resp.Usage.CompletionTokens != 3 || resp.Usage.TotalTokens != 15 {
		t.Fatalf("bad usage: %+v", resp.Usage)
	}
}

func TestChatCompletionValidation(t *testing.T) {
	srv := New(Config{OllamaURL: "http://127.0.0.1:1"})
	cases := []struct {
		name, body string
		wantStatus int
	}{
		{"bad json", `{not json`, http.StatusBadRequest},
		{"missing model", `{"messages":[{"role":"user","content":"x"}]}`, http.StatusBadRequest},
		{"empty messages", `{"model":"m","messages":[]}`, http.StatusBadRequest},
		{"stream not implemented yet", `{"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}`, http.StatusNotImplemented},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postChat(t, srv, tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body)
			}
			var e apiError
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error.Message == "" {
				t.Fatalf("error not in OpenAI envelope: %s", rec.Body)
			}
		})
	}
}

func TestChatCompletionProviderDown(t *testing.T) {
	srv := New(Config{OllamaURL: "http://127.0.0.1:1"})
	rec := postChat(t, srv, `{"model":"m","messages":[{"role":"user","content":"x"}]}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
}
