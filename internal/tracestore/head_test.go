package tracestore

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/rvsiyad/scope/internal/telemetry"
)

// span builds a test span with the fields the head cares about.
func span(traceID, spanID, parentID, name string, start, end int64) telemetry.Span {
	return telemetry.Span{TraceID: traceID, SpanID: spanID, ParentID: parentID,
		Name: name, Start: start, End: end}
}

func TestHeadGroupsSpansByTrace(t *testing.T) {
	h := NewHead()
	// One request's tree arriving out of order, interleaved with a second
	// trace — arrival order must not matter, and traces must not bleed.
	h.Append(span("t1", "s2", "s1", "provider", 200, 800))
	h.Append(span("t2", "x1", "", "request", 5000, 6000))
	h.Append(span("t1", "s1", "", "request", 100, 1000))
	h.Append(span("t1", "s3", "s1", "settle", 800, 900))

	got := h.Trace("t1")
	if len(got) != 3 {
		t.Fatalf("Trace(t1) returned %d spans, want 3", len(got))
	}
	// Sorted by start time regardless of arrival order.
	wantOrder := []string{"s1", "s2", "s3"}
	for i, id := range wantOrder {
		if got[i].SpanID != id {
			t.Fatalf("span %d = %s, want %s (order: %v)", i, got[i].SpanID, id, got)
		}
	}
	if h.Trace("t2")[0].Name != "request" {
		t.Fatal("t2 lost its span")
	}
	if h.Trace("missing") != nil {
		t.Fatal("unknown trace should return nil")
	}
	if h.NumTraces() != 2 || h.NumSpans() != 4 {
		t.Fatalf("scoreboard = %d traces / %d spans, want 2/4", h.NumTraces(), h.NumSpans())
	}
}

func TestHeadDropsDuplicateSpanIDs(t *testing.T) {
	// WAL replay after a graceful shutdown re-delivers flushed spans through
	// the same path as live ingest; the head must keep the first copy and
	// count nothing twice.
	h := NewHead()
	h.Append(span("t1", "s1", "", "request", 100, 1000))
	h.Append(span("t1", "s1", "", "request-replayed", 100, 1000))
	got := h.Trace("t1")
	if len(got) != 1 || got[0].Name != "request" {
		t.Fatalf("Trace = %v, want the single first-written span", got)
	}
	if h.NumSpans() != 1 {
		t.Fatalf("NumSpans = %d, want 1", h.NumSpans())
	}
}

func TestHeadListWindowAndOrder(t *testing.T) {
	h := NewHead()
	h.Append(span("old", "a", "", "request", 100, 200))
	h.Append(span("mid", "b", "", "request", 1000, 2000))
	h.Append(span("new", "c", "", "request", 5000, 6000))
	// A trace overlaps the window if ANY part of it does: "edge" starts
	// before the window but ends inside it.
	h.Append(span("edge", "d", "", "request", 900, 1100))

	got := h.List(1000, 5500)
	wantIDs := []string{"new", "mid", "edge"}
	if len(got) != len(wantIDs) {
		t.Fatalf("List returned %v, want ids %v", got, wantIDs)
	}
	for i, id := range wantIDs {
		if got[i].TraceID != id {
			t.Fatalf("List[%d] = %s, want %s (newest first)", i, got[i].TraceID, id)
		}
	}
	if got[0].MinStart != 5000 || got[0].MaxEnd != 6000 || got[0].Spans != 1 {
		t.Fatalf("bounds for new = %+v", got[0])
	}
}

func TestHeadListBoundsSpanWholeTrace(t *testing.T) {
	// Bounds must cover every span of the trace, not just the root's.
	h := NewHead()
	h.Append(span("t1", "s1", "", "request", 500, 600))
	h.Append(span("t1", "s2", "s1", "late-child", 550, 900))
	h.Append(span("t1", "s0", "s1", "early-child", 400, 450))
	infos := h.List(0, math.MaxInt64)
	if len(infos) != 1 || infos[0].MinStart != 400 || infos[0].MaxEnd != 900 {
		t.Fatalf("infos = %+v, want one trace spanning [400, 900]", infos)
	}
}

func TestHeadTraceReturnsACopy(t *testing.T) {
	h := NewHead()
	h.Append(span("t1", "s1", "", "request", 100, 1000))
	got := h.Trace("t1")
	got[0].Name = "mutated"
	if h.Trace("t1")[0].Name != "request" {
		t.Fatal("Trace result aliases head storage")
	}
}

func TestKeepRatioIsStableAndProportional(t *testing.T) {
	s := KeepRatio(0.5)
	kept := 0
	for i := 0; i < 10_000; i++ {
		id := fmt.Sprintf("trace-%d", i)
		first := s(id)
		if s(id) != first {
			t.Fatalf("sampler changed its mind about %s", id)
		}
		if first {
			kept++
		}
	}
	// Hash-proportional, not exact: allow a generous band around 50%.
	if kept < 4500 || kept > 5500 {
		t.Fatalf("kept %d of 10000 at ratio 0.5", kept)
	}
	if !KeepRatio(1)("anything") || KeepRatio(0)("anything") {
		t.Fatal("edge ratios must keep all / none")
	}
	if s2 := KeepRatio(1.5); !s2("x") {
		t.Fatal("ratio above 1 keeps all")
	}
}

func TestHeadConcurrentAppends(t *testing.T) {
	// The race detector is the real assertion here.
	h := NewHead()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				tid := fmt.Sprintf("t%d", i%10)
				h.Append(span(tid, fmt.Sprintf("g%d-s%d", g, i), "", "request",
					int64(i*100), int64(i*100+50)))
				h.Trace(tid)
				h.List(0, math.MaxInt64)
			}
		}(g)
	}
	wg.Wait()
	if h.NumSpans() != 8*200 {
		t.Fatalf("NumSpans = %d, want %d", h.NumSpans(), 8*200)
	}
}
