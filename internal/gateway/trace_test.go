package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rvsiyad/scope/internal/telemetry"
)

// capturedTelemetry accumulates everything a test server emits, so tests
// assert on the actual wire traffic, not on internals.
type capturedTelemetry struct {
	mu      sync.Mutex
	spans   []telemetry.Span
	metrics []telemetry.MetricPoint
}

func (c *capturedTelemetry) snapshot() ([]telemetry.Span, []telemetry.MetricPoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]telemetry.Span(nil), c.spans...), append([]telemetry.MetricPoint(nil), c.metrics...)
}

// span returns the first captured span with the given name, if any.
func (c *capturedTelemetry) span(name string) (telemetry.Span, bool) {
	spans, _ := c.snapshot()
	for _, sp := range spans {
		if sp.Name == name {
			return sp, true
		}
	}
	return telemetry.Span{}, false
}

func (c *capturedTelemetry) metricValues(name string) []telemetry.MetricPoint {
	_, metrics := c.snapshot()
	var out []telemetry.MetricPoint
	for _, m := range metrics {
		if m.Name == name {
			out = append(out, m)
		}
	}
	return out
}

// tracedServer builds a tenant-configured gateway in front of a fake Ollama,
// wired to a capturing collector with a batch size of 1 so every record
// ships immediately.
func tracedServer(t *testing.T, price float64) (*Server, *capturedTelemetry) {
	t.Helper()
	cap := &capturedTelemetry{}
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b telemetry.Batch
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Errorf("bad batch: %v", err)
		}
		cap.mu.Lock()
		cap.spans = append(cap.spans, b.Spans...)
		cap.metrics = append(cap.metrics, b.Metrics...)
		cap.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(col.Close)

	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		if req.Stream {
			enc := json.NewEncoder(w)
			enc.Encode(ollamaChatResponse{Model: req.Model, Message: Message{Content: "hello"}})
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
		OllamaURLs:      []string{ollama.URL},
		Tenants:         []TenantConfig{{Name: "acme", APIKey: "sk-acme", TokensPerMinute: 1000}},
		CollectorURL:    col.URL,
		PricePerMTokens: price,
	})
	// Replace the default emitter with an eager one so tests never wait on
	// the flush interval.
	srv.emitter = telemetry.NewEmitter(telemetry.Config{CollectorURL: col.URL, BatchSize: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv.emitter.Start(ctx)
	return srv, cap
}

// waitFor polls until check passes or the deadline hits — emission is
// asynchronous by design, so tests wait for arrival rather than sleeping.
func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for !check() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRequestEmitsFullSpanTree(t *testing.T) {
	srv, cap := tracedServer(t, 0)
	if rec := postChatAs(t, srv, "sk-acme", deterministicChat); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	wantPhases := []string{"auth", "cache_lookup", "reserve", "provider", "settle"}
	waitFor(t, "the full span tree", func() bool {
		root, ok := cap.span("request")
		if !ok {
			return false
		}
		for _, name := range wantPhases {
			if _, ok := cap.span(name); !ok {
				return false
			}
		}
		_ = root
		return true
	})

	root, _ := cap.span("request")
	if root.Attrs["tenant"] != "acme" || root.Attrs["model"] != "m" ||
		root.Attrs["outcome"] != "ok" || root.Attrs["cache"] != "miss" {
		t.Fatalf("root attrs = %v", root.Attrs)
	}
	if root.Attrs["tokens_total"] != "15" {
		t.Fatalf("tokens_total = %q, want provider usage 15", root.Attrs["tokens_total"])
	}
	if root.End < root.Start {
		t.Fatal("root span ends before it starts")
	}
	for _, name := range wantPhases {
		sp, _ := cap.span(name)
		if sp.TraceID != root.TraceID {
			t.Fatalf("%s span has trace %s, want root's %s", name, sp.TraceID, root.TraceID)
		}
		if sp.ParentID != root.SpanID {
			t.Fatalf("%s span parented to %s, want root %s", name, sp.ParentID, root.SpanID)
		}
	}
	provider, _ := cap.span("provider")
	if provider.Attrs["ttft_ms"] == "" {
		t.Fatalf("provider span missing ttft marker: %v", provider.Attrs)
	}
	settle, _ := cap.span("settle")
	if settle.Attrs["tokens_charged"] != "15" {
		t.Fatalf("settle attrs = %v, want tokens_charged 15", settle.Attrs)
	}
	lookup, _ := cap.span("cache_lookup")
	if lookup.Attrs["result"] != "miss" {
		t.Fatalf("cache_lookup attrs = %v", lookup.Attrs)
	}
}

func TestRequestEmitsMetrics(t *testing.T) {
	srv, cap := tracedServer(t, 400) // $400 per 1M tokens, so 15 tokens = $0.006
	postChatAs(t, srv, "sk-acme", deterministicChat)

	waitFor(t, "request metrics", func() bool {
		return len(cap.metricValues("gateway_requests_total")) > 0 &&
			len(cap.metricValues("gateway_tokens_total")) > 0 &&
			len(cap.metricValues("gateway_ttft_ms")) > 0 &&
			len(cap.metricValues("gateway_request_duration_ms")) > 0 &&
			len(cap.metricValues("gateway_cost_usd")) > 0
	})

	reqs := cap.metricValues("gateway_requests_total")[0]
	if reqs.Value != 1 || reqs.Labels["tenant"] != "acme" || reqs.Labels["model"] != "m" ||
		reqs.Labels["outcome"] != "ok" || reqs.Labels["cache"] != "miss" {
		t.Fatalf("requests_total = %+v", reqs)
	}
	if got := cap.metricValues("gateway_tokens_total")[0].Value; got != 15 {
		t.Fatalf("tokens_total = %v, want 15", got)
	}
	if got := cap.metricValues("gateway_cost_usd")[0].Value; got != 0.006 {
		t.Fatalf("cost_usd = %v, want 0.006 (15 tokens at $400/M)", got)
	}
}

func TestCacheHitTraceSkipsProviderPhases(t *testing.T) {
	srv, cap := tracedServer(t, 0)
	postChatAs(t, srv, "sk-acme", deterministicChat) // miss + fill
	waitFor(t, "the miss trace", func() bool {
		return len(cap.metricValues("gateway_requests_total")) == 1
	})
	postChatAs(t, srv, "sk-acme", deterministicChat) // hit
	waitFor(t, "the hit trace", func() bool {
		return len(cap.metricValues("gateway_requests_total")) == 2
	})

	spans, _ := cap.snapshot()
	var hitRoot telemetry.Span
	providerSpans := 0
	for _, sp := range spans {
		if sp.Name == "request" && sp.Attrs["cache"] == "hit" {
			hitRoot = sp
		}
		if sp.Name == "provider" {
			providerSpans++
		}
	}
	if hitRoot.SpanID == "" {
		t.Fatal("no request span with cache=hit")
	}
	if hitRoot.Attrs["outcome"] != "ok" || hitRoot.Attrs["tokens_total"] != "0" {
		t.Fatalf("hit root attrs = %v (a hit buys zero provider tokens)", hitRoot.Attrs)
	}
	if providerSpans != 1 {
		t.Fatalf("provider spans = %d, want 1 (the hit must not add one)", providerSpans)
	}
}

func TestRejectedRequestTrace(t *testing.T) {
	srv, cap := tracedServer(t, 0)
	// max_tokens far beyond the 1000-token capacity: never admittable.
	postChatAs(t, srv, "sk-acme",
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":5000}`)

	waitFor(t, "the rejection trace", func() bool {
		root, ok := cap.span("request")
		return ok && root.Attrs["outcome"] == "rejected"
	})
	if _, ok := cap.span("provider"); ok {
		t.Fatal("rejected request must not have a provider span")
	}
	if _, ok := cap.span("reserve"); !ok {
		t.Fatal("rejected request must still show its reserve phase")
	}
}

func TestStreamingTraceHasTTFT(t *testing.T) {
	srv, cap := tracedServer(t, 0)
	rec := postChatAs(t, srv, "sk-acme",
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":50,"stream":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	waitFor(t, "the streaming trace", func() bool {
		root, ok := cap.span("request")
		return ok && root.Attrs["outcome"] == "ok"
	})
	root, _ := cap.span("request")
	if root.Attrs["stream"] != "true" || root.Attrs["tokens_total"] != "15" {
		t.Fatalf("streaming root attrs = %v", root.Attrs)
	}
	provider, _ := cap.span("provider")
	if provider.Attrs["ttft_ms"] == "" {
		t.Fatalf("streaming provider span missing ttft: %v", provider.Attrs)
	}
	if len(cap.metricValues("gateway_ttft_ms")) == 0 {
		t.Fatal("no ttft metric emitted")
	}
}

// A gateway with no collector configured must run every path with a nil
// trace and never panic — the existing handler tests exercise most paths;
// this pins the config contract itself.
func TestNoCollectorMeansNilEmitter(t *testing.T) {
	ollama, _ := fakeOllama(t, func(w http.ResponseWriter, req ollamaChatRequest) {
		json.NewEncoder(w).Encode(ollamaChatResponse{Message: Message{Content: "ok"}, Done: true, DoneReason: "stop"})
	})
	srv := New(Config{OllamaURLs: []string{ollama.URL}})
	if srv.emitter != nil {
		t.Fatal("emitter must be nil without a collector URL")
	}
	if rec := postChatAs(t, srv, "", smallChat); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
