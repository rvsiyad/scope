package collector

// This file is the metrics half of the handoff: every accepted batch's
// MetricPoints flow into the tsdb engine through the same Consumer socket
// everything else uses — which is what makes crash recovery free. On
// restart the collector replays its WAL through accept() before serving
// (see New), so the head block repopulates from the log; samples that were
// already flushed into segments come back as head/segment duplicates,
// which the read path dedupes on every query and the next compaction
// removes physically. The WAL itself is never truncated yet —
// checkpointing it after a flush (so replay covers only the unflushed
// tail) is the known upgrade, deliberately out of scope until the log's
// replay time earns it.

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/tsdb"
)

// tsdbStore adapts the engine to the Consumer interface and keeps the
// drop scoreboard. Per-sample failures never fail a batch: the batch was
// already acknowledged durable, and one bad sample (out-of-order, or a
// series past the cardinality cap) must not take its siblings down — drop
// it, count it, keep it visible on /healthz.
type tsdbStore struct {
	db             *tsdb.DB
	oodDropped     atomic.Uint64
	seriesRejected atomic.Uint64
	maintErrors    atomic.Uint64
}

func (ts *tsdbStore) consume(b telemetry.Batch) {
	for _, m := range b.Metrics {
		ls := tsdb.NewLabels(m.Name, m.Labels)
		switch err := ts.db.Append(ls, m.Timestamp, m.Value); {
		case err == nil:
		case errors.Is(err, tsdb.ErrOutOfOrder):
			ts.oodDropped.Add(1)
		case errors.Is(err, tsdb.ErrTooManySeries):
			ts.seriesRejected.Add(1)
		}
	}
}

// maintain is the engine's heartbeat: every interval, flush the head into
// a segment and compact segments (enforcing retention, when configured).
// Runs until the stop channel closes; errors are counted and logged, not
// fatal — the WAL upstream means a failed flush risks memory growth, not
// data.
func (ts *tsdbStore) maintain(every, retention time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			ts.flushAndCompact(retention)
		}
	}
}

func (ts *tsdbStore) flushAndCompact(retention time.Duration) {
	if err := ts.db.Flush(); err != nil {
		ts.maintErrors.Add(1)
		log.Printf("collector: tsdb flush: %v", err)
		return
	}
	var cutoff int64
	if retention > 0 {
		cutoff = time.Now().Add(-retention).UnixMilli()
	}
	if _, err := ts.db.Compact(cutoff); err != nil {
		ts.maintErrors.Add(1)
		log.Printf("collector: tsdb compact: %v", err)
	}
}

// TSDBStatus is the engine's section of /healthz.
type TSDBStatus struct {
	HeadSeries  int    `json:"head_series"`
	HeadSamples int    `json:"head_samples"`
	Segments    int    `json:"segments"`
	OODDropped  uint64 `json:"ood_dropped"`
	SeriesRej   uint64 `json:"series_rejected"`
	MaintErrors uint64 `json:"maintenance_errors,omitempty"`
}

func (ts *tsdbStore) status() *TSDBStatus {
	return &TSDBStatus{
		HeadSeries:  ts.db.Head().NumSeries(),
		HeadSamples: ts.db.Head().NumSamples(),
		Segments:    ts.db.NumSegments(),
		OODDropped:  ts.oodDropped.Load(),
		SeriesRej:   ts.seriesRejected.Load(),
		MaintErrors: ts.maintErrors.Load(),
	}
}

// handleSelect is a pre-query-engine debug read surface (the real query
// service is a later session): every query parameter except mint/maxt
// becomes an equality matcher, with name meaning the metric name. It
// exists so demos and tests can prove ingested samples are queryable
// through the REAL read path — head, segments, merge, dedupe — rather
// than trusting counters.
func (ts *tsdbStore) handleSelect(w http.ResponseWriter, r *http.Request) {
	var matchers []tsdb.Matcher
	mint, maxt := int64(0), int64(math.MaxInt64)
	for key, vals := range r.URL.Query() {
		v := vals[0]
		switch key {
		case "mint", "maxt":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				http.Error(w, key+": "+err.Error(), http.StatusBadRequest)
				return
			}
			if key == "mint" {
				mint = n
			} else {
				maxt = n
			}
		case "name":
			matchers = append(matchers, tsdb.Eq(tsdb.MetricName, v))
		default:
			matchers = append(matchers, tsdb.Eq(key, v))
		}
	}
	series, err := ts.db.Select(matchers, mint, maxt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type sample struct {
		T int64   `json:"t"`
		V float64 `json:"v"`
	}
	type outSeries struct {
		Series  string   `json:"series"`
		Samples []sample `json:"samples"`
	}
	out := struct {
		Series []outSeries `json:"series"`
	}{Series: []outSeries{}}
	for _, s := range series {
		os := outSeries{Series: s.Labels.Key(), Samples: make([]sample, 0, len(s.Samples))}
		for _, smp := range s.Samples {
			os.Samples = append(os.Samples, sample{smp.T, smp.V})
		}
		out.Series = append(out.Series, os)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
