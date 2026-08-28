package collector

// The query API: the collector's read surface for metrics, powered by the
// query engine over the live tsdb. Two endpoints, shaped like Prometheus's
// pair because the split is real: /v1/query answers "what is it now"
// (one point per series) and /v1/query_range answers "how did it move"
// (a point per step — what dashboards actually poll). This supersedes the
// /debug/tsdb/select surface for humans; that endpoint stays because it
// answers a different question (what is physically stored) with no engine
// semantics — lookback, windows — in between.

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rvsiyad/scope/internal/query"
)

// queryResult is the wire shape of both endpoints: series keyed by their
// canonical label string, samples as [t, v] pairs — the same vocabulary
// as /debug/tsdb/select, so a consumer needs one decoder.
type queryResult struct {
	Query  string      `json:"query"`
	Result []outSeries `json:"result"`
}

type outSeries struct {
	Series  string      `json:"series"`
	Samples []outSample `json:"samples"`
}

type outSample struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// handleQuery serves GET /v1/query?query=...&time=<unix ms, default now>:
// an instant evaluation.
func (ts *tsdbStore) handleQuery(w http.ResponseWriter, r *http.Request) {
	expr, err := query.Parse(r.URL.Query().Get("query"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t := time.Now().UnixMilli()
	if raw := r.URL.Query().Get("time"); raw != "" {
		if t, err = strconv.ParseInt(raw, 10, 64); err != nil {
			http.Error(w, "time: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	m, err := ts.engine.Instant(expr, t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	writeMatrix(w, r.URL.Query().Get("query"), m)
}

// handleQueryRange serves GET /v1/query_range?query=...&start=<ms>&
// end=<ms>&step=<duration|ms>: one point per aligned step. The start is
// floored onto the step grid so a refreshing dashboard sees stable
// buckets — the alignment the engine documents.
func (ts *tsdbStore) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	expr, err := query.Parse(q.Get("query"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	start, err := strconv.ParseInt(q.Get("start"), 10, 64)
	if err != nil {
		http.Error(w, "start: want unix milliseconds", http.StatusBadRequest)
		return
	}
	end, err := strconv.ParseInt(q.Get("end"), 10, 64)
	if err != nil {
		http.Error(w, "end: want unix milliseconds", http.StatusBadRequest)
		return
	}
	step, err := parseStep(q.Get("step"))
	if err != nil {
		http.Error(w, "step: "+err.Error(), http.StatusBadRequest)
		return
	}
	if step <= 0 {
		http.Error(w, "step: must be positive", http.StatusBadRequest)
		return
	}
	if (end-start)/step > 10_000 {
		http.Error(w, "too many steps: shrink the range or grow the step", http.StatusBadRequest)
		return
	}
	m, err := ts.engine.Range(expr, query.AlignStart(start, step), end, step)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	writeMatrix(w, q.Get("query"), m)
}

// parseStep accepts either a duration ("30s") or plain milliseconds
// ("30000") — dashboards compute steps in ms, humans think in durations.
func parseStep(raw string) (int64, error) {
	if raw == "" {
		return 0, strconv.ErrSyntax
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return ms, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	return d.Milliseconds(), nil
}

func writeMatrix(w http.ResponseWriter, q string, m query.Matrix) {
	out := queryResult{Query: q, Result: make([]outSeries, 0, len(m))}
	for _, s := range m {
		os := outSeries{Series: s.Labels.Key(), Samples: make([]outSample, 0, len(s.Samples))}
		for _, smp := range s.Samples {
			os.Samples = append(os.Samples, outSample{smp.T, smp.V})
		}
		out.Result = append(out.Result, os)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
