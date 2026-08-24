package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- adapter: Ollama NDJSON -> normalized chunks ---

func TestOllamaChatStreamParsesNDJSON(t *testing.T) {
	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"model":"m","message":{"role":"assistant","content":"Hel"},"done":false}`+"\n")
		io.WriteString(w, `{"model":"m","message":{"role":"assistant","content":"lo"},"done":false}`+"\n")
		io.WriteString(w, `{"model":"m","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":5,"eval_count":2}`+"\n")
	}))
	defer ollama.Close()

	p := NewOllamaProvider(ollama.URL)
	stream, err := p.ChatStream(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	var text strings.Builder
	var last StreamChunk
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(chunk.Content)
		last = chunk
	}
	if text.String() != "Hello" {
		t.Fatalf("content = %q, want %q", text.String(), "Hello")
	}
	if last.FinishReason != "stop" || last.Usage == nil || last.Usage.TotalTokens != 7 {
		t.Fatalf("final chunk = %+v", last)
	}
}

// --- gateway: normalized chunks -> OpenAI SSE ---

// scriptedProvider replays chunks from a channel, so tests control exactly
// when each chunk becomes available.
type scriptedProvider struct {
	chunks    chan StreamChunk
	closed    chan struct{}
	closeOnce sync.Once
}

func (p *scriptedProvider) Name() string { return "scripted" }

func (p *scriptedProvider) Chat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	return ChatResponse{}, io.ErrUnexpectedEOF
}

func (p *scriptedProvider) ChatStream(ctx context.Context, req ChatRequest) (ChunkStream, error) {
	return &scriptedStream{p: p}, nil
}

type scriptedStream struct{ p *scriptedProvider }

func (s *scriptedStream) Recv() (StreamChunk, error) {
	// Like a real stream: Close unblocks a pending Recv with an error.
	select {
	case chunk, ok := <-s.p.chunks:
		if !ok {
			return StreamChunk{}, io.EOF
		}
		return chunk, nil
	case <-s.p.closed:
		return StreamChunk{}, io.ErrClosedPipe
	}
}

func (s *scriptedStream) Close() error {
	s.p.closeOnce.Do(func() { close(s.p.closed) })
	return nil
}

func newScriptedProvider() *scriptedProvider {
	return &scriptedProvider{chunks: make(chan StreamChunk), closed: make(chan struct{})}
}

// readSSEEvent reads one "data: ..." line (plus its blank separator) from the
// stream, failing the test on timeout so a buffering bug can't hang the suite.
func readSSEEvent(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		for {
			line, err := r.ReadString('\n')
			if err != nil || strings.TrimSpace(line) != "" {
				ch <- result{strings.TrimSpace(line), err}
				return
			}
		}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("reading SSE event: %v", res.err)
		}
		return res.line
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE event — is the gateway buffering the stream?")
		return ""
	}
}

func TestStreamingRelaysChunksIncrementally(t *testing.T) {
	provider := newScriptedProvider()
	gw := httptest.NewServer(NewWithProvider(Config{}, provider))
	defer gw.Close()

	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}
	reader := bufio.NewReader(resp.Body)

	// The proof of no buffering: each event must be readable on the wire
	// while the provider is still holding the NEXT chunk back. A buffering
	// gateway would block here until the stream completed.
	provider.chunks <- StreamChunk{Content: "Hel", Model: "m"}
	first := readSSEEvent(t, reader)

	provider.chunks <- StreamChunk{Content: "lo", Model: "m"}
	second := readSSEEvent(t, reader)

	provider.chunks <- StreamChunk{FinishReason: "stop", Model: "m"}
	final := readSSEEvent(t, reader)
	close(provider.chunks)
	done := readSSEEvent(t, reader)

	// Fresh target per unmarshal: json.Unmarshal merges into existing slice
	// elements, so reusing one would leak fields between events.
	var ev1, ev2, ev3 ChatChunkResponse
	if err := json.Unmarshal([]byte(strings.TrimPrefix(first, "data: ")), &ev1); err != nil {
		t.Fatal(err)
	}
	if ev1.Object != "chat.completion.chunk" || ev1.Choices[0].Delta.Role != "assistant" || ev1.Choices[0].Delta.Content != "Hel" {
		t.Fatalf("first event = %+v", ev1)
	}
	if ev1.Choices[0].FinishReason != nil {
		t.Fatal("finish_reason must be null before the final chunk")
	}

	if err := json.Unmarshal([]byte(strings.TrimPrefix(second, "data: ")), &ev2); err != nil {
		t.Fatal(err)
	}
	if ev2.Choices[0].Delta.Content != "lo" || ev2.Choices[0].Delta.Role != "" {
		t.Fatalf("second event = %+v", ev2)
	}

	if err := json.Unmarshal([]byte(strings.TrimPrefix(final, "data: ")), &ev3); err != nil {
		t.Fatal(err)
	}
	if ev3.Choices[0].FinishReason == nil || *ev3.Choices[0].FinishReason != "stop" {
		t.Fatalf("final event = %+v", ev3)
	}

	if done != "data: [DONE]" {
		t.Fatalf("terminator = %q, want %q", done, "data: [DONE]")
	}
}

func TestStreamingClosesUpstreamOnClientDisconnect(t *testing.T) {
	provider := newScriptedProvider()
	gw := httptest.NewServer(NewWithProvider(Config{}, provider))
	defer gw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req, _ := http.NewRequestWithContext(ctx, "POST", gw.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	provider.chunks <- StreamChunk{Content: "Hel", Model: "m"}
	readSSEEvent(t, bufio.NewReader(resp.Body))

	// Client walks away mid-stream; the gateway must Close() the upstream
	// stream instead of leaking it (which also unblocks the pending Recv).
	cancel()
	resp.Body.Close()

	select {
	case <-provider.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream stream was not closed after client disconnect")
	}
}
