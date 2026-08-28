package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func runQuery(t *testing.T, srv *Server, path string) (int, queryResult) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	var out queryResult
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s: %v (body %s)", path, err, rec.Body)
		}
	}
	return rec.Code, out
}

// seedCounter ingests a counter climbing 10/s for two tenants (acme twice
// as fast as globex) over 0..10s.
func seedCounter(t *testing.T, srv *Server) {
	t.Helper()
	for i := int64(0); i <= 10; i++ {
		ingestMetrics(t, srv,
			point("gateway_tokens_total", map[string]string{"tenant": "acme"}, i*1000, float64(20*i)),
			point("gateway_tokens_total", map[string]string{"tenant": "globex"}, i*1000, float64(10*i)),
		)
	}
}

func TestInstantQueryEndpoint(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	seedCounter(t, srv)

	code, out := runQuery(t, srv,
		"/v1/query?time=10000&query="+
			"rate(gateway_tokens_total%7Btenant%3D%22acme%22%7D%5B10s%5D)")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(out.Result) != 1 || len(out.Result[0].Samples) != 1 || out.Result[0].Samples[0].V != 20 {
		t.Fatalf("got %+v, want acme at exactly 20 tokens/s", out)
	}
	// rate() output drops the metric name from the series key.
	if out.Result[0].Series != `{tenant="acme"}` {
		t.Fatalf("series key = %q", out.Result[0].Series)
	}
}

func TestRangeQueryEndpointAlignsAndSums(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	seedCounter(t, srv)

	// sum by (tenant) of both rates, start deliberately misaligned (1234
	// must floor to 0 on the 5s grid). Steps at 5000 and 10000.
	code, out := runQuery(t, srv,
		"/v1/query_range?start=1234&end=10000&step=5s&query="+
			"sum%20by%20(tenant)%20(rate(gateway_tokens_total%5B10s%5D))")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(out.Result) != 2 {
		t.Fatalf("got %+v, want two tenants", out)
	}
	acme, globex := out.Result[0], out.Result[1]
	if acme.Series != `{tenant="acme"}` || globex.Series != `{tenant="globex"}` {
		t.Fatalf("series = %q, %q", acme.Series, globex.Series)
	}
	// Aligned steps only — no point at 1234.
	for _, s := range acme.Samples {
		if s.T%5000 != 0 {
			t.Fatalf("unaligned step %d", s.T)
		}
	}
	last := acme.Samples[len(acme.Samples)-1]
	if last.T != 10000 || last.V != 20 {
		t.Fatalf("acme last = %+v, want 20/s at t=10000", last)
	}
	if g := globex.Samples[len(globex.Samples)-1]; g.V != 10 {
		t.Fatalf("globex last = %+v, want 10/s", g)
	}
}

func TestQuantileQueryEndpoint(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	for i := int64(1); i <= 4; i++ {
		ingestMetrics(t, srv, point("gateway_ttft_ms", nil, i*1000, float64(i*10)))
	}
	code, out := runQuery(t, srv,
		"/v1/query?time=5000&query=quantile_over_time(0.5%2C%20gateway_ttft_ms%5B1m%5D)")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(out.Result) != 1 || out.Result[0].Samples[0].V != 25 {
		t.Fatalf("got %+v, want p50 = 25", out)
	}
}

func TestQueryEndpointErrors(t *testing.T) {
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	ingestMetrics(t, srv, point("m", nil, 1000, 1))

	cases := []struct {
		path string
		want int
	}{
		{"/v1/query?query=rate(m", http.StatusBadRequest},     // parse error
		{"/v1/query?query=m&time=abc", http.StatusBadRequest}, // bad time
		{"/v1/query_range?query=m&start=0&end=10&step=0", http.StatusBadRequest},
		{"/v1/query_range?query=m&start=0&end=10", http.StatusBadRequest}, // missing step
		{"/v1/query_range?query=m&start=abc&end=10&step=1s", http.StatusBadRequest},
		{"/v1/query_range?query=m&start=0&end=100000000&step=1", http.StatusBadRequest}, // step explosion
	}
	for _, c := range cases {
		if code, _ := runQuery(t, srv, c.path); code != c.want {
			t.Fatalf("%s: status %d, want %d", c.path, code, c.want)
		}
	}

	// A valid parse that the engine rejects (quantile out of range) is the
	// engine's error, not the parser's: 422.
	code, _ := runQuery(t, srv, "/v1/query?query=quantile_over_time(2%2C%20m%5B1m%5D)")
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("engine rejection: status %d, want 422", code)
	}
}

func TestQueryReadsAcrossFlushBoundary(t *testing.T) {
	// The query API must see one continuous series even when its samples
	// straddle head and segment — the engine rides the DB's unified reads.
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	for i := int64(0); i <= 5; i++ {
		ingestMetrics(t, srv, point("c", nil, i*1000, float64(10*i)))
	}
	srv.tsdb.flushAndCompact(0)
	for i := int64(6); i <= 10; i++ {
		ingestMetrics(t, srv, point("c", nil, i*1000, float64(10*i)))
	}
	code, out := runQuery(t, srv, "/v1/query?time=10000&query=rate(c%5B10s%5D)")
	if code != http.StatusOK || len(out.Result) != 1 || out.Result[0].Samples[0].V != 10 {
		t.Fatalf("got %d %+v, want a clean 10/s across the boundary", code, out)
	}
}

func TestQueryEndpointsDisabledWithoutTSDB(t *testing.T) {
	srv := newTestServer(t) // no TSDBDir
	for _, path := range []string{"/v1/query?query=m", "/v1/query_range?query=m&start=0&end=1&step=1s"} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s without tsdb: status %d, want 404", path, rec.Code)
		}
	}
}

func TestQueryDefaultsTimeToNow(t *testing.T) {
	// Ingest with a wall-clock-recent timestamp so the default lookback
	// window (5m ending now) can see it.
	srv := newTSDBServer(t, t.TempDir(), 0)
	defer srv.Close()
	ingestMetrics(t, srv, point("m", nil, time.Now().UnixMilli(), 7))
	code, out := runQuery(t, srv, "/v1/query?query=m")
	if code != http.StatusOK || len(out.Result) != 1 || out.Result[0].Samples[0].V != 7 {
		t.Fatalf("got %d %+v, want the sample under the default now", code, out)
	}
}
