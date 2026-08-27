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

// newTSDBServer builds a collector with WAL + tsdb store and no background
// maintenance — tests drive flushes explicitly.
func newTSDBServer(t *testing.T, dir string, maxSeries int) *Server {
	t.Helper()
	srv, err := New(Config{
		WALPath:       filepath.Join(dir, "collector.wal"),
		SyncPolicy:    wal.SyncAlways,
		TSDBDir:       filepath.Join(dir, "tsdb"),
		TSDBMaxSeries: maxSeries,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func ingestMetrics(t *testing.T, srv *Server, points ...telemetry.MetricPoint) {
	t.Helper()
	payload, _ := json.Marshal(telemetry.Batch{Metrics: points})
	rec := postIngest(t, srv, string(payload))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("ingest: status %d, body %s", rec.Code, rec.Body)
	}
}

type selectResponse struct {
	Series []struct {
		Series  string `json:"series"`
		Samples []struct {
			T int64   `json:"t"`
			V float64 `json:"v"`
		} `json:"samples"`
	} `json:"series"`
}

func debugSelect(t *testing.T, srv *Server, query string) selectResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/tsdb/select?"+query, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("select %q: status %d, body %s", query, rec.Code, rec.Body)
	}
	var out selectResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func point(name string, labels map[string]string, ts int64, v float64) telemetry.MetricPoint {
	return telemetry.MetricPoint{Name: name, Labels: labels, Timestamp: ts, Value: v}
}

func TestMetricsFlowIntoTSDB(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	ingestMetrics(t, srv,
		point("gateway_tokens_total", map[string]string{"tenant": "acme"}, 1000, 10),
		point("gateway_tokens_total", map[string]string{"tenant": "acme"}, 2000, 25),
		point("gateway_tokens_total", map[string]string{"tenant": "globex"}, 1000, 5),
	)
	out := debugSelect(t, srv, "name=gateway_tokens_total&tenant=acme")
	if len(out.Series) != 1 || len(out.Series[0].Samples) != 2 {
		t.Fatalf("got %+v, want acme's 2 samples", out)
	}
	if s := out.Series[0].Samples[1]; s.T != 2000 || s.V != 25 {
		t.Fatalf("sample = %+v, want t=2000 v=25", s)
	}
}

func TestCrashReplayRepopulatesHead(t *testing.T) {
	// The crash-recovery story end to end, in-process: ingest without any
	// flush, abandon the server without Close (the crash), then start a
	// new collector on the same WAL and empty-head tsdb dir. Replay must
	// make every acknowledged sample queryable again.
	dir := t.TempDir()
	srv1 := newTSDBServer(t, dir, 0)
	for i := int64(1); i <= 20; i++ {
		ingestMetrics(t, srv1, point("m", nil, i*1000, float64(i)))
	}
	// No Close: a crash never flushes.

	srv2 := newTSDBServer(t, dir, 0)
	defer srv2.Close()
	out := debugSelect(t, srv2, "name=m")
	if len(out.Series) != 1 || len(out.Series[0].Samples) != 20 {
		t.Fatalf("after replay: %+v, want all 20 samples", out)
	}
	var st Status
	rec := httptest.NewRecorder()
	srv2.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.TSDB == nil || st.TSDB.HeadSamples != 20 {
		t.Fatalf("tsdb status = %+v, want 20 head samples", st.TSDB)
	}
}

func TestReplayAfterFlushDedupesAgainstSegments(t *testing.T) {
	// The WAL is never truncated, so after a flush a restart replays
	// samples that already live in a segment. The head re-holds them, a
	// query dedupes them, and a flush+compact cycle removes them
	// physically — the full life of a duplicate.
	dir := t.TempDir()
	srv1 := newTSDBServer(t, dir, 0)
	for i := int64(1); i <= 5; i++ {
		ingestMetrics(t, srv1, point("m", nil, i*1000, float64(i)))
	}
	srv1.tsdb.flushAndCompact(0) // segment now holds t=1000..5000
	// Crash: no Close.

	srv2 := newTSDBServer(t, dir, 0)
	defer srv2.Close()
	st := srv2.tsdb.status()
	if st.Segments != 1 || st.HeadSamples != 5 {
		t.Fatalf("status = %+v, want 1 segment + 5 replayed head samples", st)
	}
	out := debugSelect(t, srv2, "name=m")
	if len(out.Series) != 1 || len(out.Series[0].Samples) != 5 {
		t.Fatalf("query must dedupe head/segment duplicates: %+v", out)
	}
	// Flush the duplicates into a second segment, compact, and they're
	// physically gone.
	srv2.tsdb.flushAndCompact(0)
	if got := srv2.tsdb.db.NumSegments(); got != 1 {
		t.Fatalf("NumSegments = %d, want 1 after compaction", got)
	}
	out = debugSelect(t, srv2, "name=m")
	if len(out.Series) != 1 || len(out.Series[0].Samples) != 5 {
		t.Fatalf("post-compaction: %+v, want the same 5 samples", out)
	}
}

func TestCardinalityGuardAtIngest(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 2)
	defer srv.Close()
	for i := 0; i < 5; i++ {
		ingestMetrics(t, srv, point("m", map[string]string{"id": fmt.Sprint(i)}, 1000, 1))
	}
	// Known series keep working at the cap.
	ingestMetrics(t, srv, point("m", map[string]string{"id": "0"}, 2000, 2))

	st := srv.tsdb.status()
	if st.HeadSeries != 2 || st.SeriesRej != 3 {
		t.Fatalf("status = %+v, want 2 series kept / 3 rejected", st)
	}
	out := debugSelect(t, srv, "name=m&id=0")
	if len(out.Series) != 1 || len(out.Series[0].Samples) != 2 {
		t.Fatalf("known series must accept post-cap samples: %+v", out)
	}
}

func TestOutOfOrderDroppedNotFatal(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	// One batch: good, out-of-order, good. The batch was acked; the bad
	// sample is dropped and counted, its siblings survive.
	ingestMetrics(t, srv,
		point("m", nil, 5000, 5),
		point("m", nil, 1000, 1),
		point("m", nil, 6000, 6),
	)
	st := srv.tsdb.status()
	if st.OODDropped != 1 || st.HeadSamples != 2 {
		t.Fatalf("status = %+v, want 1 dropped / 2 kept", st)
	}
}

func TestTSDBDisabled(t *testing.T) {
	srv := newTestServer(t) // no TSDBDir
	ingestMetrics(t, srv, point("m", nil, 1000, 1))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/tsdb/select?name=m", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("debug endpoint without tsdb: status %d, want 404", rec.Code)
	}
	var st Status
	health := httptest.NewRecorder()
	srv.ServeHTTP(health, httptest.NewRequest("GET", "/healthz", nil))
	if err := json.Unmarshal(health.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.TSDB != nil {
		t.Fatalf("healthz has a tsdb section with the store disabled: %+v", st.TSDB)
	}
}

func TestGracefulCloseFlushesHead(t *testing.T) {
	dir := t.TempDir()
	srv := newTSDBServer(t, dir, 0)
	ingestMetrics(t, srv, point("m", nil, 1000, 1))
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// A graceful shutdown leaves its samples in a segment, not to the
	// next WAL replay.
	srv2 := newTSDBServer(t, dir, 0)
	defer srv2.Close()
	if st := srv2.tsdb.status(); st.Segments != 1 {
		t.Fatalf("status = %+v, want the closed head flushed into 1 segment", st)
	}
}
