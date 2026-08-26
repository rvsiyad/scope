package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// captureCollector is a fake collector that hands every received batch to a
// channel, so tests wait on delivery instead of sleeping.
func captureCollector(t *testing.T) (*httptest.Server, chan Batch) {
	t.Helper()
	batches := make(chan Batch, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/ingest" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		var b Batch
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Errorf("bad batch body: %v", err)
		}
		batches <- b
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, batches
}

func waitBatch(t *testing.T, batches chan Batch) Batch {
	t.Helper()
	select {
	case b := <-batches:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("no batch arrived")
		return Batch{}
	}
}

func testSpan(name string) Span {
	return Span{
		TraceID: NewTraceID(), SpanID: NewSpanID(), Name: name,
		Start: 1000, End: 2000,
		Attrs: map[string]string{"tenant": "acme"},
	}
}

func TestFlushAtBatchSize(t *testing.T) {
	srv, batches := captureCollector(t)
	e := NewEmitter(Config{CollectorURL: srv.URL, BatchSize: 3, FlushInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	for i := 0; i < 3; i++ {
		e.RecordSpan(testSpan("provider"))
	}
	b := waitBatch(t, batches)
	if len(b.Spans) != 3 {
		t.Fatalf("batch has %d spans, want 3", len(b.Spans))
	}
	// The scoreboard updates after the POST round-trips, so wait for it.
	deadline := time.After(5 * time.Second)
	for e.Status().BatchesSent == 0 {
		select {
		case <-deadline:
			t.Fatalf("status = %+v, want 3 spans in 1 batch", e.Status())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if st := e.Status(); st.SpansEmitted != 3 || st.Dropped != 0 {
		t.Fatalf("status = %+v, want 3 spans emitted, 0 dropped", st)
	}
}

func TestFlushOnInterval(t *testing.T) {
	srv, batches := captureCollector(t)
	e := NewEmitter(Config{CollectorURL: srv.URL, BatchSize: 1000, FlushInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	e.RecordMetric(MetricPoint{Name: "gateway_ttft_ms", Value: 42, Timestamp: 1000,
		Labels: map[string]string{"model": "m"}})

	b := waitBatch(t, batches)
	if len(b.Metrics) != 1 || b.Metrics[0].Name != "gateway_ttft_ms" || b.Metrics[0].Value != 42 {
		t.Fatalf("interval flush delivered %+v", b)
	}
}

// The wire format is the contract phase B builds against: what goes in is
// what comes out, field for field, through real JSON.
func TestWireFormatRoundTrips(t *testing.T) {
	srv, batches := captureCollector(t)
	e := NewEmitter(Config{CollectorURL: srv.URL, BatchSize: 2, FlushInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	span := Span{
		TraceID: "aaaa", SpanID: "bbbb", ParentID: "cccc", Name: "provider",
		Start: 12345, End: 67890,
		Attrs: map[string]string{"ttft_ms": "210", "tokens_total": "57"},
	}
	metric := MetricPoint{
		Name: "gateway_tokens_total", Value: 57, Timestamp: 1700000000000,
		Labels: map[string]string{"tenant": "acme", "model": "llama3.2:1b"},
	}
	e.RecordSpan(span)
	e.RecordMetric(metric)

	b := waitBatch(t, batches)
	if len(b.Spans) != 1 || len(b.Metrics) != 1 {
		t.Fatalf("batch = %+v, want 1 span + 1 metric", b)
	}
	gotSpan, _ := json.Marshal(b.Spans[0])
	wantSpan, _ := json.Marshal(span)
	if string(gotSpan) != string(wantSpan) {
		t.Fatalf("span round-trip: got %s, want %s", gotSpan, wantSpan)
	}
	gotMetric, _ := json.Marshal(b.Metrics[0])
	wantMetric, _ := json.Marshal(metric)
	if string(gotMetric) != string(wantMetric) {
		t.Fatalf("metric round-trip: got %s, want %s", gotMetric, wantMetric)
	}
}

// The one law: a stalled collector must never stall a Record* caller. The
// collector here blocks forever, the buffer holds 4 records — recording far
// more must return promptly with the excess dropped and counted.
func TestFullBufferDropsInsteadOfBlocking(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	e := NewEmitter(Config{CollectorURL: srv.URL, BatchSize: 1, FlushInterval: time.Hour, BufferSize: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			e.RecordSpan(testSpan("s"))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RecordSpan blocked on a stalled collector")
	}
	if e.Status().Dropped == 0 {
		t.Fatal("overflow must be counted as dropped")
	}
}

func TestDeadCollectorCountsFlushErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // dead on arrival

	e := NewEmitter(Config{CollectorURL: srv.URL, BatchSize: 1, FlushInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	e.RecordSpan(testSpan("s"))
	deadline := time.After(5 * time.Second)
	for e.Status().FlushErrors == 0 {
		select {
		case <-deadline:
			t.Fatal("failed flush never counted")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// The emitter must still accept records afterward — a dead collector is
	// the collector's problem, never the caller's.
	e.RecordSpan(testSpan("s"))
}

func TestCancelFlushesPending(t *testing.T) {
	srv, batches := captureCollector(t)
	e := NewEmitter(Config{CollectorURL: srv.URL, BatchSize: 1000, FlushInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)

	e.RecordSpan(testSpan("s"))
	cancel()

	if b := waitBatch(t, batches); len(b.Spans) != 1 {
		t.Fatalf("final flush delivered %d spans, want 1", len(b.Spans))
	}
}

func TestNilEmitterIsSafe(t *testing.T) {
	var e *Emitter
	e.Start(context.Background())
	e.RecordSpan(testSpan("s"))
	e.RecordMetric(MetricPoint{Name: "m"})
	if st := e.Status(); st != nil {
		t.Fatal("nil emitter status must be nil")
	}
}

func TestIDWidths(t *testing.T) {
	if got := len(NewTraceID()); got != 32 {
		t.Fatalf("trace id hex length = %d, want 32", got)
	}
	if got := len(NewSpanID()); got != 16 {
		t.Fatalf("span id hex length = %d, want 16", got)
	}
	if NewTraceID() == NewTraceID() {
		t.Fatal("trace ids must not repeat")
	}
}
