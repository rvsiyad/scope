package collector

// This file is the trace half of the handoff, the mirror of tsdb.go:
// every accepted batch's Spans flow into the trace store through the same
// Consumer socket, so WAL replay rebuilds the trace head on restart for
// free, exactly as it rebuilds the metrics head. It also serves the read
// side — the request log listing and the waterfall — because the trace
// store's whole reason to exist is "click any request, see its span tree".

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/tracestore"
)

// traceStore adapts the store to the Consumer interface and keeps the
// drop scoreboard. Sampled-out spans are policy outcomes, not errors —
// counted and visible on /healthz, never failing a batch that was already
// acknowledged durable.
type traceStore struct {
	store       *tracestore.Store
	sampledOut  atomic.Uint64
	maintErrors atomic.Uint64
}

func (ts *traceStore) consume(b telemetry.Batch) {
	for _, sp := range b.Spans {
		if !ts.store.Append(sp) {
			ts.sampledOut.Add(1)
		}
	}
}

// maintain is the trace store's heartbeat: every interval, flush the head
// into a segment and enforce retention. Errors are counted and logged, not
// fatal — the WAL upstream means a failed flush risks memory growth, not
// data.
func (ts *traceStore) maintain(every, retention time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			ts.flushAndRetain(retention)
		}
	}
}

func (ts *traceStore) flushAndRetain(retention time.Duration) {
	if err := ts.store.Flush(); err != nil {
		ts.maintErrors.Add(1)
		log.Printf("collector: trace flush: %v", err)
		return
	}
	if retention > 0 {
		cutoff := time.Now().Add(-retention).UnixNano()
		if _, err := ts.store.EnforceRetention(cutoff); err != nil {
			ts.maintErrors.Add(1)
			log.Printf("collector: trace retention: %v", err)
		}
	}
}

// TraceStatus is the trace store's section of /healthz.
type TraceStatus struct {
	HeadTraces  int    `json:"head_traces"`
	HeadSpans   int    `json:"head_spans"`
	Segments    int    `json:"segments"`
	SampledOut  uint64 `json:"sampled_out"`
	MaintErrors uint64 `json:"maintenance_errors,omitempty"`
}

func (ts *traceStore) status() *TraceStatus {
	return &TraceStatus{
		HeadTraces:  ts.store.Head().NumTraces(),
		HeadSpans:   ts.store.Head().NumSpans(),
		Segments:    ts.store.NumSegments(),
		SampledOut:  ts.sampledOut.Load(),
		MaintErrors: ts.maintErrors.Load(),
	}
}

// waterfallSpan is one node of the rendered span tree. Duration is
// precomputed in milliseconds because that is the unit every consumer of
// the waterfall (UI, curl-ing human) actually thinks in; starts and ends
// stay nanos, the spans' native unit.
type waterfallSpan struct {
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id,omitempty"`
	Name       string            `json:"name"`
	Start      int64             `json:"start"`
	End        int64             `json:"end"`
	DurationMS float64           `json:"duration_ms"`
	Attrs      map[string]string `json:"attrs,omitempty"`
	Children   []*waterfallSpan  `json:"children,omitempty"`
}

// handleTrace serves GET /v1/traces/{id}: the full span tree, nested and
// ordered, ready to render as a waterfall.
func (ts *traceStore) handleTrace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	spans, err := ts.store.Trace(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if spans == nil {
		http.Error(w, "unknown trace id", http.StatusNotFound)
		return
	}
	out := struct {
		TraceID string           `json:"trace_id"`
		Spans   int              `json:"spans"`
		Roots   []*waterfallSpan `json:"roots"`
	}{TraceID: id, Spans: len(spans), Roots: buildTree(spans)}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// buildTree nests spans by parent id. Spans arrive sorted by start time
// and stay that way at every level. A span whose parent is missing (its
// half of a sampled boundary, or simply not arrived yet) is promoted to a
// root rather than dropped — a partial waterfall beats a vanished one.
func buildTree(spans []telemetry.Span) []*waterfallSpan {
	nodes := make(map[string]*waterfallSpan, len(spans))
	ordered := make([]*waterfallSpan, 0, len(spans))
	for _, sp := range spans {
		n := &waterfallSpan{
			SpanID:     sp.SpanID,
			ParentID:   sp.ParentID,
			Name:       sp.Name,
			Start:      sp.Start,
			End:        sp.End,
			DurationMS: float64(sp.End-sp.Start) / 1e6,
			Attrs:      sp.Attrs,
		}
		nodes[sp.SpanID] = n
		ordered = append(ordered, n)
	}
	var roots []*waterfallSpan
	for _, n := range ordered {
		if parent, ok := nodes[n.ParentID]; ok && n.ParentID != n.SpanID {
			parent.Children = append(parent.Children, n)
		} else {
			roots = append(roots, n)
		}
	}
	return roots
}

// traceSummary is one row of the request log: the trace directory entry
// plus what the root span knows (name, attrs — tenant, model, tokens,
// cost, outcome all live there).
type traceSummary struct {
	TraceID    string            `json:"trace_id"`
	Root       string            `json:"root"`
	Start      int64             `json:"start"`
	End        int64             `json:"end"`
	DurationMS float64           `json:"duration_ms"`
	Spans      int               `json:"spans"`
	Attrs      map[string]string `json:"attrs,omitempty"`
}

// handleList serves GET /v1/traces: the request log, newest first.
// Query parameters: mint/maxt (unix nanoseconds, defaulting to
// everything) and limit (default 50). The listing walks the store's time
// index for the window, then fetches full spans only for the page it
// returns — bounded work regardless of history size.
func (ts *traceStore) handleList(w http.ResponseWriter, r *http.Request) {
	mint, maxt := int64(0), int64(math.MaxInt64)
	limit := 50
	for key, vals := range r.URL.Query() {
		n, err := strconv.ParseInt(vals[0], 10, 64)
		if err != nil {
			http.Error(w, key+": "+err.Error(), http.StatusBadRequest)
			return
		}
		switch key {
		case "mint":
			mint = n
		case "maxt":
			maxt = n
		case "limit":
			limit = int(n)
		default:
			http.Error(w, "unknown parameter "+key, http.StatusBadRequest)
			return
		}
	}
	if limit <= 0 || limit > 1000 {
		limit = 50
	}

	infos := ts.store.List(mint, maxt, limit)
	out := struct {
		Traces []traceSummary `json:"traces"`
	}{Traces: make([]traceSummary, 0, len(infos))}
	for _, info := range infos {
		spans, err := ts.store.Trace(info.TraceID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s := traceSummary{
			TraceID:    info.TraceID,
			Start:      info.MinStart,
			End:        info.MaxEnd,
			DurationMS: float64(info.MaxEnd-info.MinStart) / 1e6,
			Spans:      len(spans),
		}
		if root := rootSpan(spans); root != nil {
			s.Root = root.Name
			s.Attrs = root.Attrs
		}
		out.Traces = append(out.Traces, s)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// rootSpan picks the span the summary should speak for: the first span
// with no parent in the trace, or failing that the earliest span.
func rootSpan(spans []telemetry.Span) *telemetry.Span {
	ids := make(map[string]struct{}, len(spans))
	for _, sp := range spans {
		ids[sp.SpanID] = struct{}{}
	}
	best := -1
	for i, sp := range spans {
		if _, ok := ids[sp.ParentID]; sp.ParentID == "" || !ok {
			if best == -1 || sp.Start < spans[best].Start {
				best = i
			}
		}
	}
	if best == -1 {
		if len(spans) == 0 {
			return nil
		}
		best = 0
		for i, sp := range spans {
			if sp.Start < spans[best].Start {
				best = i
			}
		}
	}
	return &spans[best]
}
