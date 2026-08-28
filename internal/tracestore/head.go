// Package tracestore is the trace half of the observability backend:
// span trees keyed by trace id, stored for the waterfall read. It shares
// the tsdb's skeleton — an in-memory head, immutable segment files, WAL
// durability upstream — but diverges where the read pattern diverges:
// metrics are scanned by label over a time range, traces are fetched whole
// by id. So instead of Gorilla streams and an inverted label index, the
// unit of storage is the complete trace, and the index is a plain id →
// trace map. Storage layout follows read pattern; forcing both shapes into
// one store is how "one store for all telemetry" becomes a trap.
package tracestore

import (
	"hash/fnv"
	"sort"
	"sync"

	"github.com/rvsiyad/scope/internal/telemetry"
)

// A Sampler decides at the door whether a trace is kept, from the trace id
// alone. Deciding on the id — not per span — is the contract that keeps
// traces whole: one trace's spans can arrive across many batches, and every
// one of them must get the same verdict or the waterfall renders half a
// request. A nil Sampler keeps everything.
type Sampler func(traceID string) bool

// KeepRatio returns a Sampler keeping roughly the given fraction of traces
// (<= 0 keeps none, >= 1 keeps all). The decision hashes the trace id, so
// it is stable across restarts and across collectors — the property that
// lets a scaled-out ingest tier sample consistently with no coordination.
func KeepRatio(ratio float64) Sampler {
	if ratio >= 1 {
		return func(string) bool { return true }
	}
	if ratio <= 0 {
		return func(string) bool { return false }
	}
	threshold := uint64(ratio * (1 << 32))
	return func(traceID string) bool {
		h := fnv.New64a()
		h.Write([]byte(traceID))
		return h.Sum64()&0xffffffff < threshold
	}
}

// TraceInfo is one trace's directory entry: its id, time bounds, and span
// count. Bounds are unix nanoseconds, like the spans themselves.
type TraceInfo struct {
	TraceID  string
	MinStart int64
	MaxEnd   int64
	Spans    int
}

// memTrace is one trace accumulating in the head.
type memTrace struct {
	spans    []telemetry.Span
	spanIDs  map[string]struct{}
	minStart int64
	maxEnd   int64
}

// Head is the in-memory trace head: every span since the last flush,
// grouped by trace id. Durability is not its job — the collector's WAL
// sits in front, exactly as it does for the metrics head, so a crash
// loses nothing acknowledged. Safe for concurrent use.
type Head struct {
	mu     sync.RWMutex
	traces map[string]*memTrace
	spans  int
}

func NewHead() *Head {
	return &Head{traces: map[string]*memTrace{}}
}

// Append adds one span to its trace, registering the trace on first sight.
// Spans arrive in whatever order batches deliver them — unlike the metrics
// head there is no ordering constraint, because nothing here is
// delta-encoded. A span id already present in the trace is dropped
// (first write wins, the same duplicate rule the tsdb applies): the WAL
// replays history through the same path as live ingest, so replay after a
// graceful shutdown re-delivers spans that were already flushed.
func (h *Head) Append(sp telemetry.Span) {
	h.mu.Lock()
	defer h.mu.Unlock()
	tr, ok := h.traces[sp.TraceID]
	if !ok {
		tr = &memTrace{spanIDs: map[string]struct{}{}, minStart: sp.Start, maxEnd: sp.End}
		h.traces[sp.TraceID] = tr
	}
	if _, dup := tr.spanIDs[sp.SpanID]; dup {
		return
	}
	tr.spanIDs[sp.SpanID] = struct{}{}
	tr.spans = append(tr.spans, sp)
	if sp.Start < tr.minStart {
		tr.minStart = sp.Start
	}
	if sp.End > tr.maxEnd {
		tr.maxEnd = sp.End
	}
	h.spans++
}

// Trace returns the trace's spans sorted by start time, or nil if the head
// has never seen the id. The slice is a copy — callers hold results across
// appends and flushes.
func (h *Head) Trace(id string) []telemetry.Span {
	h.mu.RLock()
	defer h.mu.RUnlock()
	tr, ok := h.traces[id]
	if !ok {
		return nil
	}
	out := make([]telemetry.Span, len(tr.spans))
	copy(out, tr.spans)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

// List returns the directory entries of every trace overlapping
// [mint, maxt] (a trace overlaps if any part of it does), sorted newest
// first by start time. This is the scan the time index answers; at head
// scale it is a walk over the live traces, and the segment meta plays the
// same role for frozen ones.
func (h *Head) List(mint, maxt int64) []TraceInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]TraceInfo, 0, len(h.traces))
	for id, tr := range h.traces {
		if tr.maxEnd < mint || tr.minStart > maxt {
			continue
		}
		out = append(out, TraceInfo{TraceID: id, MinStart: tr.minStart, MaxEnd: tr.maxEnd, Spans: len(tr.spans)})
	}
	sortInfos(out)
	return out
}

// sortInfos orders directory entries newest first, id as tiebreak so equal
// timestamps still list deterministically.
func sortInfos(infos []TraceInfo) {
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].MinStart != infos[j].MinStart {
			return infos[i].MinStart > infos[j].MinStart
		}
		return infos[i].TraceID < infos[j].TraceID
	})
}

// NumTraces and NumSpans are the head's scoreboard.
func (h *Head) NumTraces() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.traces)
}

func (h *Head) NumSpans() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.spans
}
