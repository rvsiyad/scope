package tracestore

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mustFlush(t *testing.T, st *Store) {
	t.Helper()
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestStoreReadsAcrossFlushBoundary(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A trace split by a flush: root and provider freeze into a segment,
	// settle lands in the fresh head. The read must reassemble it.
	st.Append(span("t1", "s1", "", "request", 100, 1000))
	st.Append(span("t1", "s2", "s1", "provider", 200, 800))
	mustFlush(t, st)
	if st.NumSegments() != 1 {
		t.Fatalf("segments = %d, want 1", st.NumSegments())
	}
	st.Append(span("t1", "s3", "s1", "settle", 800, 900))

	got, err := st.Trace("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].SpanID != "s1" || got[1].SpanID != "s2" || got[2].SpanID != "s3" {
		t.Fatalf("reassembled trace = %v, want s1,s2,s3", got)
	}

	// The split trace appears once in the listing, with merged bounds.
	infos := st.List(0, math.MaxInt64, 0)
	if len(infos) != 1 || infos[0].MinStart != 100 || infos[0].MaxEnd != 1000 {
		t.Fatalf("List = %+v, want one entry spanning [100, 1000]", infos)
	}
}

func TestStoreDedupesReplayedSpans(t *testing.T) {
	// After a graceful shutdown the WAL still holds everything, so restart
	// replay re-appends spans that already live in a segment. The head
	// takes them (it cannot know), and the read path must drop them.
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Append(span("t1", "s1", "", "request", 100, 1000))
	mustFlush(t, st)

	st2, err := Open(dir) // restart: segment reloaded, head empty
	if err != nil {
		t.Fatal(err)
	}
	st2.Append(span("t1", "s1", "", "request", 100, 1000)) // WAL replay
	got, err := st2.Trace("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("trace holds %d spans after replay, want 1", len(got))
	}
	if infos := st2.List(0, math.MaxInt64, 0); len(infos) != 1 || infos[0].Spans != 1 {
		t.Fatalf("List = %+v, want one trace with 1 span", infos)
	}
}

func TestStoreSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Append(span("t1", "s1", "", "request", 100, 1000))
	mustFlush(t, st)
	st.Append(span("t2", "s2", "", "request", 2000, 3000))
	mustFlush(t, st)

	// A stale tmp from a flush that died before its rename must be swept.
	tmp := filepath.Join(dir, "00000003.tseg.tmp")
	if err := os.WriteFile(tmp, []byte("half-written"), 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st2.NumSegments() != 2 {
		t.Fatalf("reopened with %d segments, want 2", st2.NumSegments())
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("stale tmp survived reopen")
	}
	for _, id := range []string{"t1", "t2"} {
		got, err := st2.Trace(id)
		if err != nil || len(got) != 1 {
			t.Fatalf("trace %s after restart = %v, %v", id, got, err)
		}
	}
	// Sequence numbering continues past existing files, no overwrites.
	st2.Append(span("t3", "s3", "", "request", 4000, 5000))
	mustFlush(t, st2)
	if st2.NumSegments() != 3 {
		t.Fatalf("segments after post-restart flush = %d, want 3", st2.NumSegments())
	}
}

func TestStoreListLimitAndOrder(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		st.Append(span(fmt.Sprintf("t%d", i), fmt.Sprintf("s%d", i), "", "request",
			int64(1000*(i+1)), int64(1000*(i+1)+500)))
		if i == 2 {
			mustFlush(t, st) // entries must merge across segment + head
		}
	}
	got := st.List(0, math.MaxInt64, 2)
	if len(got) != 2 || got[0].TraceID != "t4" || got[1].TraceID != "t3" {
		t.Fatalf("List(limit=2) = %v, want [t4, t3]", got)
	}
}

func TestStoreSamplerDropsWholeTraces(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st.SetSampler(func(id string) bool { return id == "keep" })
	if !st.Append(span("keep", "s1", "", "request", 100, 1000)) {
		t.Fatal("kept trace's span reported dropped")
	}
	// Every span of a dropped trace gets the same verdict.
	if st.Append(span("drop", "s2", "", "request", 100, 1000)) ||
		st.Append(span("drop", "s3", "s2", "provider", 200, 800)) {
		t.Fatal("sampled-out span reported kept")
	}
	if tr, _ := st.Trace("drop"); tr != nil {
		t.Fatal("sampled-out trace is readable")
	}
	if st.Head().NumSpans() != 1 {
		t.Fatalf("head spans = %d, want 1", st.Head().NumSpans())
	}
}

func TestStoreRetentionDropsWholeSegmentsOnly(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Append(span("old", "a", "", "request", 100, 200))
	mustFlush(t, st)
	st.Append(span("straddle", "b", "", "request", 900, 1100))
	mustFlush(t, st)

	dropped, err := st.EnforceRetention(1000)
	if err != nil {
		t.Fatal(err)
	}
	// The old segment goes; the straddling one survives whole — retention
	// is a bound, not a knife edge.
	if dropped != 1 || st.NumSegments() != 1 {
		t.Fatalf("dropped=%d segments=%d, want 1 and 1", dropped, st.NumSegments())
	}
	if tr, _ := st.Trace("old"); tr != nil {
		t.Fatal("dropped trace still readable")
	}
	if tr, _ := st.Trace("straddle"); len(tr) != 1 {
		t.Fatal("straddling trace lost")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.tseg"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files on disk = %v, want exactly the surviving segment", files)
	}
	// Cutoff 0 means retention disabled.
	if n, err := st.EnforceRetention(0); n != 0 || err != nil {
		t.Fatalf("disabled retention dropped %d, err %v", n, err)
	}
}

func TestStoreConcurrentReadsAndFlushes(t *testing.T) {
	// The race detector is the real assertion here.
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				tid := fmt.Sprintf("t%d", i%7)
				st.Append(span(tid, fmt.Sprintf("g%d-s%d", g, i), "", "request",
					int64(i*100), int64(i*100+50)))
				if _, err := st.Trace(tid); err != nil {
					t.Errorf("trace: %v", err)
					return
				}
				st.List(0, math.MaxInt64, 10)
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := st.Flush(); err != nil {
				t.Errorf("flush: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}
