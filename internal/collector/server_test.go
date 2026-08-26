package collector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvsiyad/scope/internal/telemetry"
)

// newTestServer builds a collector without a WAL — the validation and
// counting tests don't need durability.
func newTestServer(t *testing.T, consumers ...Consumer) *Server {
	t.Helper()
	srv, err := New(Config{}, consumers...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func postIngest(t *testing.T, srv *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/ingest", strings.NewReader(body))
	srv.ServeHTTP(rec, req)
	return rec
}

func TestIngestCountsValidBatch(t *testing.T) {
	srv := newTestServer(t)
	batch := telemetry.Batch{
		Spans: []telemetry.Span{{
			TraceID: "aa", SpanID: "bb", Name: "request", Start: 1, End: 2,
		}},
		Metrics: []telemetry.MetricPoint{{Name: "gateway_ttft_ms", Value: 42}},
	}
	payload, _ := json.Marshal(batch)

	rec := postIngest(t, srv, string(payload))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body = %s", rec.Code, rec.Body)
	}

	healthz := httptest.NewRecorder()
	srv.ServeHTTP(healthz, httptest.NewRequest("GET", "/healthz", nil))
	var st Status
	if err := json.Unmarshal(healthz.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Batches != 1 || st.Spans != 1 || st.Metrics != 1 || st.Invalid != 0 {
		t.Fatalf("status = %+v, want 1 batch / 1 span / 1 metric", st)
	}
}

func TestIngestRejectsGarbage(t *testing.T) {
	srv := newTestServer(t)
	cases := map[string]string{
		"malformed JSON":  `{"spans": [`,
		"span without id": `{"spans":[{"name":"request","start":1,"end":2}]}`,
		"span end<start":  `{"spans":[{"trace_id":"a","span_id":"b","name":"r","start":5,"end":1}]}`,
		"unnamed metric":  `{"metrics":[{"value":1}]}`,
	}
	for name, body := range cases {
		if rec := postIngest(t, srv, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}

	healthz := httptest.NewRecorder()
	srv.ServeHTTP(healthz, httptest.NewRequest("GET", "/healthz", nil))
	var st Status
	json.Unmarshal(healthz.Body.Bytes(), &st)
	if st.Invalid != uint64(len(cases)) || st.Batches != 0 {
		t.Fatalf("status = %+v, want %d invalid and 0 accepted", st, len(cases))
	}
}

// End to end through the real emitter: what the gateway-side facade ships
// is what the collector accepts — the two halves agree on the contract.
func TestEmitterToCollectorContract(t *testing.T) {
	srv := newTestServer(t)
	httpSrv := httptest.NewServer(srv)
	t.Cleanup(httpSrv.Close)

	e := telemetry.NewEmitter(telemetry.Config{CollectorURL: httpSrv.URL, BatchSize: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	e.RecordSpan(telemetry.Span{
		TraceID: telemetry.NewTraceID(), SpanID: telemetry.NewSpanID(),
		Name: "request", Start: 1, End: 2,
	})

	deadline := time.After(5 * time.Second)
	for {
		healthz := httptest.NewRecorder()
		srv.ServeHTTP(healthz, httptest.NewRequest("GET", "/healthz", nil))
		var st Status
		json.Unmarshal(healthz.Body.Bytes(), &st)
		if st.Spans == 1 && st.Invalid == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("span never arrived; collector status = %+v, emitter = %+v", st, e.Status())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
