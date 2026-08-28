package tracestore

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func flushTo(t *testing.T, h *Head, path string) int {
	t.Helper()
	n, err := h.Flush(path)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	return n
}

func TestSegmentRoundTrip(t *testing.T) {
	h := NewHead()
	h.Append(span("t1", "s2", "s1", "provider", 200, 800))
	h.Append(span("t1", "s1", "", "request", 100, 1000))
	h.Append(span("t2", "x1", "", "request", 5000, 6000))
	attrs := map[string]string{"tenant": "acme", "tokens_total": "42"}
	sp := span("t2", "x2", "x1", "settle", 5500, 5900)
	sp.Attrs = attrs
	h.Append(sp)

	path := filepath.Join(t.TempDir(), "00000001.tseg")
	if n := flushTo(t, h, path); n != 2 {
		t.Fatalf("flush wrote %d traces, want 2", n)
	}
	// The head must be empty after a successful flush.
	if h.NumTraces() != 0 || h.NumSpans() != 0 {
		t.Fatalf("head after flush: %d traces / %d spans", h.NumTraces(), h.NumSpans())
	}

	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	if seg.NumTraces() != 2 || seg.NumSpans() != 4 {
		t.Fatalf("segment holds %d traces / %d spans, want 2/4", seg.NumTraces(), seg.NumSpans())
	}
	if seg.MinTime() != 100 || seg.MaxTime() != 6000 {
		t.Fatalf("segment bounds [%d, %d], want [100, 6000]", seg.MinTime(), seg.MaxTime())
	}

	got, err := seg.Trace("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].SpanID != "s1" || got[1].SpanID != "s2" {
		t.Fatalf("t1 spans = %v, want s1 then s2 (sorted by start)", got)
	}
	got, err = seg.Trace("t2")
	if err != nil {
		t.Fatal(err)
	}
	if got[1].Attrs["tenant"] != "acme" || got[1].Attrs["tokens_total"] != "42" {
		t.Fatalf("attrs lost in the freeze: %v", got[1].Attrs)
	}
	if missing, err := seg.Trace("nope"); err != nil || missing != nil {
		t.Fatalf("unknown trace = %v, %v; want nil, nil", missing, err)
	}
}

func TestSegmentListNewestFirstAndWindowed(t *testing.T) {
	h := NewHead()
	h.Append(span("old", "a", "", "request", 100, 200))
	h.Append(span("new", "b", "", "request", 5000, 6000))
	h.Append(span("edge", "c", "", "request", 900, 1100))
	path := filepath.Join(t.TempDir(), "00000001.tseg")
	flushTo(t, h, path)
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	got := seg.List(1000, math.MaxInt64)
	if len(got) != 2 || got[0].TraceID != "new" || got[1].TraceID != "edge" {
		t.Fatalf("List = %v, want [new, edge]", got)
	}
}

func TestFlushEmptyHeadWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001.tseg")
	if n := flushTo(t, NewHead(), path); n != 0 {
		t.Fatalf("empty flush wrote %d traces", n)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("empty flush left a file: %v", err)
	}
}

func TestOpenSegmentRefusesCorruption(t *testing.T) {
	h := NewHead()
	h.Append(span("t1", "s1", "", "request", 100, 1000))
	dir := t.TempDir()
	path := filepath.Join(dir, "00000001.tseg")
	flushTo(t, h, path)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Segments were fully written before they got their name, so unlike
	// the WAL there is no legal way for one to be half-good: every kind of
	// damage must fail the open, never "recover".
	cases := map[string][]byte{
		"bad magic":         append([]byte("WRONGMAG"), good[8:]...),
		"truncated header":  good[:len(traceMagic)+3],
		"truncated payload": good[:len(good)-2],
		"flipped byte": func() []byte {
			b := append([]byte(nil), good...)
			b[len(b)-1] ^= 0xff
			return b
		}(),
	}
	for name, data := range cases {
		p := filepath.Join(dir, "bad.tseg")
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSegment(p); err == nil {
			t.Fatalf("%s: OpenSegment accepted a damaged file", name)
		}
	}
}

func TestFlushFailureLeavesHeadIntact(t *testing.T) {
	h := NewHead()
	h.Append(span("t1", "s1", "", "request", 100, 1000))
	// A directory that does not exist makes the tmp create fail.
	if _, err := h.Flush(filepath.Join(t.TempDir(), "no-such-dir", "x.tseg")); err == nil {
		t.Fatal("flush into a missing directory should fail")
	}
	if h.NumTraces() != 1 || h.NumSpans() != 1 {
		t.Fatalf("failed flush disturbed the head: %d traces / %d spans", h.NumTraces(), h.NumSpans())
	}
}
