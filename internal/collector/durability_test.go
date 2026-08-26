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

func batchJSON(traceID string) string {
	b := telemetry.Batch{
		Spans: []telemetry.Span{{TraceID: traceID, SpanID: "s", Name: "request", Start: 1, End: 2}},
		Metrics: []telemetry.MetricPoint{{
			Name: "gateway_requests_total", Value: 1,
			Labels: map[string]string{"tenant": "acme"},
		}},
	}
	payload, _ := json.Marshal(b)
	return string(payload)
}

// The ack contract, end to end: acked batches survive the process. A new
// collector over the same WAL rebuilds identical counters and replays
// history into its consumers before serving anything live.
func TestAckedBatchesSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collector.wal")

	srv, err := New(Config{WALPath: path, SyncPolicy: wal.SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if rec := postIngest(t, srv, batchJSON(fmt.Sprintf("trace-%d", i))); rec.Code != http.StatusNoContent {
			t.Fatalf("ingest %d: status = %d, body = %s", i, rec.Code, rec.Body)
		}
	}
	// No graceful shutdown on purpose: SyncAlways means the acks are
	// already on disk. (Close would also fsync, which would let a lazier
	// policy cheat this test.)

	var replayed []string
	restarted, err := New(Config{WALPath: path, SyncPolicy: wal.SyncAlways}, func(b telemetry.Batch) {
		replayed = append(replayed, b.Spans[0].TraceID)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	if len(replayed) != 5 || replayed[0] != "trace-0" || replayed[4] != "trace-4" {
		t.Fatalf("replayed %v, want trace-0..trace-4 in order", replayed)
	}
	st := statusOf(t, restarted)
	if st.Batches != 5 || st.Spans != 5 || st.Metrics != 5 {
		t.Fatalf("restarted counters = %+v, want 5/5/5", st)
	}
	if st.WAL == nil || st.WAL.Records != 5 {
		t.Fatalf("wal status = %+v, want 5 records", st.WAL)
	}

	// And the restarted collector keeps ingesting on the same log.
	if rec := postIngest(t, restarted, batchJSON("trace-5")); rec.Code != http.StatusNoContent {
		t.Fatalf("post-restart ingest: status = %d", rec.Code)
	}
	if st := statusOf(t, restarted); st.Batches != 6 || st.WAL.Records != 6 {
		t.Fatalf("post-restart counters = %+v, want 6 batches", st)
	}
}

// A rejected batch must leave no trace: 400s are not acks and must not be
// replayed into consumers after a restart.
func TestRejectedBatchesAreNotLogged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collector.wal")
	srv, err := New(Config{WALPath: path, SyncPolicy: wal.SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	postIngest(t, srv, batchJSON("good"))
	if rec := postIngest(t, srv, `{"spans":[{"name":"no-ids"}]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid batch: status = %d, want 400", rec.Code)
	}
	srv.Close()

	calls := 0
	restarted, err := New(Config{WALPath: path, SyncPolicy: wal.SyncAlways},
		func(b telemetry.Batch) { calls++ })
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if calls != 1 {
		t.Fatalf("replayed %d batches, want only the valid one", calls)
	}
}

func TestConsumersSeeLiveBatchesInOrder(t *testing.T) {
	var seen []string
	srv := newTestServer(t, func(b telemetry.Batch) {
		seen = append(seen, b.Spans[0].TraceID)
	})
	for i := 0; i < 3; i++ {
		postIngest(t, srv, batchJSON(fmt.Sprintf("t%d", i)))
	}
	if len(seen) != 3 || seen[0] != "t0" || seen[2] != "t2" {
		t.Fatalf("consumer saw %v", seen)
	}
}

func TestOversizeBatchRejected(t *testing.T) {
	srv := newTestServer(t)
	big := fmt.Sprintf(`{"metrics":[{"name":"m","labels":{"pad":"%s"}}]}`,
		string(make([]byte, maxBodyBytes)))
	if rec := postIngest(t, srv, big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize batch: status = %d, want 413", rec.Code)
	}
}

func TestUnopenableWALFailsFast(t *testing.T) {
	// A directory where the log file should be: refusing to start beats
	// serving un-durable acks that claim otherwise.
	if _, err := New(Config{WALPath: t.TempDir()}); err == nil {
		t.Fatal("New must fail when the WAL cannot be opened")
	}
}

func statusOf(t *testing.T, srv *Server) Status {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	var st Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	return st
}
