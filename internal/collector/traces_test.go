package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/wal"
)

// newTraceServer builds a collector with WAL + trace store and no
// background maintenance — tests drive flushes explicitly.
func newTraceServer(t *testing.T, dir string, keepRatio float64) *Server {
	t.Helper()
	srv, err := New(Config{
		WALPath:        filepath.Join(dir, "collector.wal"),
		SyncPolicy:     wal.SyncAlways,
		TraceDir:       filepath.Join(dir, "traces"),
		TraceKeepRatio: keepRatio,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func ingestSpans(t *testing.T, srv *Server, spans ...telemetry.Span) {
	t.Helper()
	payload, _ := json.Marshal(telemetry.Batch{Spans: spans})
	rec := postIngest(t, srv, string(payload))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("ingest: status %d, body %s", rec.Code, rec.Body)
	}
}

func testSpan(traceID, spanID, parentID, name string, start, end int64, attrs map[string]string) telemetry.Span {
	return telemetry.Span{TraceID: traceID, SpanID: spanID, ParentID: parentID,
		Name: name, Start: start, End: end, Attrs: attrs}
}

type waterfallResponse struct {
	TraceID string `json:"trace_id"`
	Spans   int    `json:"spans"`
	Roots   []struct {
		SpanID     string            `json:"span_id"`
		Name       string            `json:"name"`
		DurationMS float64           `json:"duration_ms"`
		Attrs      map[string]string `json:"attrs"`
		Children   []struct {
			SpanID   string `json:"span_id"`
			Name     string `json:"name"`
			Start    int64  `json:"start"`
			Children []struct {
				SpanID string `json:"span_id"`
			} `json:"children"`
		} `json:"children"`
	} `json:"roots"`
}

func getTrace(t *testing.T, srv *Server, id string) waterfallResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/traces/"+id, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("trace %s: status %d, body %s", id, rec.Code, rec.Body)
	}
	var out waterfallResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

type listResponse struct {
	Traces []struct {
		TraceID    string            `json:"trace_id"`
		Root       string            `json:"root"`
		DurationMS float64           `json:"duration_ms"`
		Spans      int               `json:"spans"`
		Attrs      map[string]string `json:"attrs"`
	} `json:"traces"`
}

func listTraces(t *testing.T, srv *Server, query string) listResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/traces"+query, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list %q: status %d, body %s", query, rec.Code, rec.Body)
	}
	var out listResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWaterfallEndToEnd(t *testing.T) {
	srv := newTraceServer(t, t.TempDir(), 0)
	defer srv.Close()
	// One request's tree, the gateway's real shape: root with attrs, phase
	// children, a grandchild — delivered out of order across two batches.
	ingestSpans(t, srv,
		testSpan("t1", "s2", "s1", "provider", 200e6, 800e6, nil),
		testSpan("t1", "s4", "s2", "ttft", 200e6, 300e6, nil),
	)
	ingestSpans(t, srv,
		testSpan("t1", "s1", "", "request", 100e6, 1000e6,
			map[string]string{"tenant": "acme", "tokens_total": "42"}),
		testSpan("t1", "s3", "s1", "settle", 800e6, 900e6, nil),
	)

	out := getTrace(t, srv, "t1")
	if out.Spans != 4 || len(out.Roots) != 1 {
		t.Fatalf("waterfall = %+v, want 4 spans under 1 root", out)
	}
	root := out.Roots[0]
	if root.Name != "request" || root.Attrs["tenant"] != "acme" {
		t.Fatalf("root = %+v", root)
	}
	if root.DurationMS != 900 {
		t.Fatalf("root duration = %v ms, want 900", root.DurationMS)
	}
	// Children ordered by start; the grandchild hangs off the provider span.
	if len(root.Children) != 2 || root.Children[0].Name != "provider" || root.Children[1].Name != "settle" {
		t.Fatalf("children = %+v", root.Children)
	}
	if len(root.Children[0].Children) != 1 || root.Children[0].Children[0].SpanID != "s4" {
		t.Fatalf("grandchild lost: %+v", root.Children[0])
	}
}

func TestTraceNotFoundAndDisabled(t *testing.T) {
	srv := newTraceServer(t, t.TempDir(), 0)
	defer srv.Close()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/traces/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown trace: status %d, want 404", rec.Code)
	}

	off := newTestServer(t) // no TraceDir
	rec = httptest.NewRecorder()
	off.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/traces", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("trace endpoints without a store: status %d, want 404", rec.Code)
	}
	var st Status
	health := httptest.NewRecorder()
	off.ServeHTTP(health, httptest.NewRequest("GET", "/healthz", nil))
	if err := json.Unmarshal(health.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Traces != nil {
		t.Fatalf("healthz has a traces section with the store disabled: %+v", st.Traces)
	}
}

func TestRequestLogListing(t *testing.T) {
	srv := newTraceServer(t, t.TempDir(), 0)
	defer srv.Close()
	for i := int64(1); i <= 3; i++ {
		id := fmt.Sprintf("t%d", i)
		ingestSpans(t, srv,
			testSpan(id, id+"-root", "", "request", i*1000e6, i*1000e6+500e6,
				map[string]string{"tenant": "acme"}),
			testSpan(id, id+"-p", id+"-root", "provider", i*1000e6+100e6, i*1000e6+400e6, nil),
		)
	}
	out := listTraces(t, srv, "?limit=2")
	if len(out.Traces) != 2 || out.Traces[0].TraceID != "t3" || out.Traces[1].TraceID != "t2" {
		t.Fatalf("list = %+v, want [t3, t2] newest first", out.Traces)
	}
	row := out.Traces[0]
	if row.Root != "request" || row.Spans != 2 || row.DurationMS != 500 || row.Attrs["tenant"] != "acme" {
		t.Fatalf("row = %+v", row)
	}
	// Window filtering: only t1 starts before 1.5s.
	out = listTraces(t, srv, fmt.Sprintf("?maxt=%d", int64(1500e6)))
	if len(out.Traces) != 1 || out.Traces[0].TraceID != "t1" {
		t.Fatalf("windowed list = %+v, want just t1", out.Traces)
	}
	if rec := httptest.NewRecorder(); true {
		srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/traces?mint=abc", nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("bad mint: status %d, want 400", rec.Code)
		}
	}
}

func TestCrashReplayRebuildsTraces(t *testing.T) {
	// The trace half of the crash story: ingest, no flush, no Close, then a
	// new collector on the same WAL — the waterfall must come back whole.
	dir := t.TempDir()
	srv1 := newTraceServer(t, dir, 0)
	ingestSpans(t, srv1,
		testSpan("t1", "s1", "", "request", 100e6, 1000e6, nil),
		testSpan("t1", "s2", "s1", "provider", 200e6, 800e6, nil),
	)
	// No Close: a crash never flushes.

	srv2 := newTraceServer(t, dir, 0)
	defer srv2.Close()
	out := getTrace(t, srv2, "t1")
	if out.Spans != 2 || len(out.Roots) != 1 || len(out.Roots[0].Children) != 1 {
		t.Fatalf("after replay: %+v, want the full tree back", out)
	}
	if st := srv2.traces.status(); st.HeadTraces != 1 || st.HeadSpans != 2 {
		t.Fatalf("status = %+v, want 1 trace / 2 spans in the head", st)
	}
}

func TestGracefulCloseFlushesTracesAndReplayDedupes(t *testing.T) {
	// Close flushes the trace head into a segment; the WAL still replays
	// everything on restart, so the head re-holds the spans and reads must
	// serve each span once, not twice.
	dir := t.TempDir()
	srv1 := newTraceServer(t, dir, 0)
	ingestSpans(t, srv1, testSpan("t1", "s1", "", "request", 100e6, 1000e6, nil))
	if err := srv1.Close(); err != nil {
		t.Fatal(err)
	}

	srv2 := newTraceServer(t, dir, 0)
	defer srv2.Close()
	st := srv2.traces.status()
	if st.Segments != 1 || st.HeadSpans != 1 {
		t.Fatalf("status = %+v, want 1 segment + 1 replayed head span", st)
	}
	out := getTrace(t, srv2, "t1")
	if out.Spans != 1 {
		t.Fatalf("read must dedupe head/segment duplicates: %+v", out)
	}
	list := listTraces(t, srv2, "")
	if len(list.Traces) != 1 || list.Traces[0].Spans != 1 {
		t.Fatalf("list must show the trace once: %+v", list.Traces)
	}
}

func TestFlushSplitTraceReassembles(t *testing.T) {
	srv := newTraceServer(t, t.TempDir(), 0)
	defer srv.Close()
	ingestSpans(t, srv, testSpan("t1", "s1", "", "request", 100e6, 1000e6, nil))
	srv.traces.flushAndRetain(0)
	ingestSpans(t, srv, testSpan("t1", "s2", "s1", "settle", 800e6, 900e6, nil))
	out := getTrace(t, srv, "t1")
	if out.Spans != 2 || len(out.Roots) != 1 || len(out.Roots[0].Children) != 1 {
		t.Fatalf("split trace = %+v, want both halves under one root", out)
	}
}

func TestSamplingDropsAndCounts(t *testing.T) {
	// keepRatio in (0,1) installs the hash sampler; with many one-span
	// traces roughly half survive, and the dropped ones are counted.
	srv := newTraceServer(t, t.TempDir(), 0.5)
	defer srv.Close()
	spans := make([]telemetry.Span, 0, 200)
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("trace-%d", i)
		spans = append(spans, testSpan(id, id+"-s", "", "request", int64(i)*1000e6, int64(i)*1000e6+100e6, nil))
	}
	ingestSpans(t, srv, spans...)
	st := srv.traces.status()
	if st.SampledOut == 0 || st.HeadTraces == 0 || st.HeadTraces+int(st.SampledOut) != 200 {
		t.Fatalf("status = %+v, want kept+dropped == 200 with both nonzero", st)
	}
}
