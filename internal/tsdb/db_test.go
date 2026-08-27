package tsdb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// TestSelectAcrossHeadSegmentBoundary is the session's headline contract:
// write, flush, keep writing, and a query cannot tell the boundary exists.
func TestSelectAcrossHeadSegmentBoundary(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("gateway_tokens_total", map[string]string{"tenant": "acme"})
	for i := int64(1); i <= 10; i++ {
		if err := db.Append(ls, i*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := int64(11); i <= 20; i++ {
		if err := db.Append(ls, i*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}

	got, err := db.Select([]Matcher{Eq("tenant", "acme")}, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1 — the boundary must not split a series", len(got))
	}
	if len(got[0].Samples) != 20 {
		t.Fatalf("got %d samples, want 20", len(got[0].Samples))
	}
	for i, s := range got[0].Samples {
		want := Sample{int64(i+1) * 1000, float64(i + 1)}
		if s != want {
			t.Fatalf("sample %d = %v, want %v", i, s, want)
		}
	}

	// A window straddling the boundary trims from both sides of it.
	got, err = db.Select(nil, 8000, 13_000)
	if err != nil {
		t.Fatal(err)
	}
	var ts []int64
	for _, s := range got[0].Samples {
		ts = append(ts, s.T)
	}
	if want := []int64{8000, 9000, 10_000, 11_000, 12_000, 13_000}; !reflect.DeepEqual(ts, want) {
		t.Fatalf("straddling window: %v, want %v", ts, want)
	}
}

func TestMultipleFlushCycles(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("m", nil)
	for cycle := int64(0); cycle < 3; cycle++ {
		for i := int64(0); i < 5; i++ {
			if err := db.Append(ls, cycle*5000+i*1000, 1); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if db.NumSegments() != 3 {
		t.Fatalf("NumSegments = %d, want 3", db.NumSegments())
	}
	// An empty flush must not mint an empty segment.
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if db.NumSegments() != 3 {
		t.Fatalf("empty flush minted a segment: %d", db.NumSegments())
	}
	got, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Samples) != 15 {
		t.Fatalf("3 segments should merge to one series of 15 samples, got %v", got)
	}
}

func TestReopenRecoversSegments(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("m", nil)
	for i := int64(1); i <= 5; i++ {
		if err := db.Append(ls, i*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// Head-only samples: flushed nowhere, on restart they come back via
	// the collector's WAL replay (S8), not from the DB's own files.
	if err := db.Append(ls, 9000, 9); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if db2.NumSegments() != 1 {
		t.Fatalf("reopen found %d segments, want 1", db2.NumSegments())
	}
	got, err := db2.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Samples) != 5 {
		t.Fatalf("reopened DB returned %v, want the 5 flushed samples", got)
	}
	// New segments must continue the sequence, not collide with 00000001.
	if err := db2.Append(ls, 20_000, 1); err != nil {
		t.Fatal(err)
	}
	if err := db2.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000002.seg")); err != nil {
		t.Fatalf("second flush did not produce 00000002.seg: %v", err)
	}
}

func TestOpenCleansStaleTmpAndFailsOnCorruptSegment(t *testing.T) {
	dir := t.TempDir()
	// A stale .tmp is a flush that died before its rename: never part of
	// the query set, deleted on open.
	stale := filepath.Join(dir, "00000007.seg.tmp")
	if err := os.WriteFile(stale, []byte("half a segment"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale .tmp survived Open")
	}
	// A corrupt real segment is different: refusing to start beats serving
	// queries with a silent hole in history.
	if err := os.WriteFile(filepath.Join(dir, "00000001.seg"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open accepted a directory with a corrupt segment")
	}
}

func TestDuplicateTimestampAcrossBoundaryFirstWriteWins(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("m", nil)
	if err := db.Append(ls, 1000, 111); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// The reset head accepts t=1000 again (its watermark is gone); the
	// merge must keep the segment's value, mirroring the head's own
	// first-write-wins duplicate rule.
	if err := db.Append(ls, 1000, 999); err != nil {
		t.Fatal(err)
	}
	if err := db.Append(ls, 2000, 2); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	want := []Sample{{1000, 111}, {2000, 2}}
	if !reflect.DeepEqual(got[0].Samples, want) {
		t.Fatalf("samples = %v, want %v", got[0].Samples, want)
	}
}

func TestLateSampleSortsIntoPlace(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("m", nil)
	for _, tm := range []int64{1000, 2000, 3000} {
		if err := db.Append(ls, tm, float64(tm)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// A late sample older than flushed history lands in the fresh head;
	// the merged result must still come out ascending.
	if err := db.Append(ls, 1500, 1500); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	var ts []int64
	for _, s := range got[0].Samples {
		ts = append(ts, s.T)
	}
	if want := []int64{1000, 1500, 2000, 3000}; !reflect.DeepEqual(ts, want) {
		t.Fatalf("timestamps %v, want %v", ts, want)
	}
}

func TestSelectSkipsSegmentsOutsideWindow(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("m", nil)
	// Two time-disjoint segments.
	for _, base := range []int64{0, 100_000} {
		for i := int64(1); i <= 5; i++ {
			if err := db.Append(ls, base+i*1000, float64(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.Select(nil, 100_000, 200_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Samples) != 5 {
		t.Fatalf("window over segment 2 returned %v", got)
	}
	if got[0].Samples[0].T != 101_000 {
		t.Fatalf("first sample %v, want t=101000", got[0].Samples[0])
	}
}

func TestMatchersApplyAcrossSources(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"acme", "globex"} {
		ls := NewLabels("m", map[string]string{"tenant": tenant})
		if err := db.Append(ls, 1000, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"acme", "hooli"} {
		ls := NewLabels("m", map[string]string{"tenant": tenant})
		if err := db.Append(ls, 2000, 2); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.Select([]Matcher{Eq("tenant", "acme")}, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Samples) != 2 {
		t.Fatalf("acme across sources: %v", got)
	}
	got, err = db.Select([]Matcher{Neq("tenant", "acme")}, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 { // globex (segment only) + hooli (head only)
		t.Fatalf("neq across sources returned %d series, want 2", len(got))
	}
}

func TestConcurrentAppendFlushSelect(t *testing.T) {
	// Appends, flushes, and selects race freely; the race detector
	// referees and the final total must balance.
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const writers, samples = 4, 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ls := NewLabels("m", map[string]string{"w": fmt.Sprint(w)})
			for i := int64(0); i < samples; i++ {
				if err := db.Append(ls, i, float64(i)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err := db.Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := db.Select(nil, 0, samples); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
	got, err := db.Select(nil, 0, samples)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, s := range got {
		total += len(s.Samples)
	}
	if len(got) != writers || total != writers*samples {
		t.Fatalf("final state: %d series / %d samples, want %d / %d",
			len(got), total, writers, writers*samples)
	}
}
