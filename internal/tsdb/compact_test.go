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

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// flushCycles writes `cycles` segments of 5 samples each for two tenants,
// timestamps advancing across cycles.
func flushCycles(t *testing.T, db *DB, cycles int) {
	t.Helper()
	for c := int64(0); c < int64(cycles); c++ {
		for _, tenant := range []string{"acme", "globex"} {
			ls := NewLabels("m", map[string]string{"tenant": tenant})
			for i := int64(0); i < 5; i++ {
				if err := db.Append(ls, c*5000+i*1000, float64(c*5+i)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactMergesToOneSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	flushCycles(t, db, 3)
	before, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Compact(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inputs != 3 || res.Series != 2 || res.Samples != 30 || res.Dropped != 0 {
		t.Fatalf("result = %+v, want 3 inputs, 2 series, 30 samples, 0 dropped", res)
	}
	if db.NumSegments() != 1 {
		t.Fatalf("NumSegments = %d, want 1", db.NumSegments())
	}
	// On disk too: one file, and it kept the oldest input's name so the
	// chronological naming survives compaction.
	if files := segFiles(t, dir); !reflect.DeepEqual(files, []string{"00000001.seg"}) {
		t.Fatalf("files = %v, want [00000001.seg]", files)
	}

	// Queries must be indistinguishable before and after.
	after, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("compaction changed query results:\nbefore %v\nafter  %v", before, after)
	}

	// The compacted DB must survive a reopen.
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := db2.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, reopened) {
		t.Fatal("reopened compacted DB differs from pre-compaction results")
	}
}

func TestCompactEnforcesRetention(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	flushCycles(t, db, 3) // samples at 0..14000
	res, err := db.Compact(5000)
	if err != nil {
		t.Fatal(err)
	}
	// Cycle 0 (t=0..4000, 10 samples across both tenants) expires.
	if res.Dropped != 10 || res.Samples != 20 {
		t.Fatalf("result = %+v, want 10 dropped / 20 kept", res)
	}
	got, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range got {
		if len(s.Samples) != 10 || s.Samples[0].T != 5000 {
			t.Fatalf("%s: %d samples starting at %d, want 10 starting at 5000",
				s.Labels.Key(), len(s.Samples), s.Samples[0].T)
		}
	}
}

func TestCompactDropsFullyExpiredSeries(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old := NewLabels("m", map[string]string{"age": "old"})
	fresh := NewLabels("m", map[string]string{"age": "fresh"})
	if err := db.Append(old, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Append(fresh, 9000, 9); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(5000); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	// The expired series is gone entirely — not present-but-empty; the
	// index in the compacted segment never learns its labels.
	if len(got) != 1 || got[0].Labels.Key() != `m{age="fresh"}` {
		t.Fatalf("got %v, want only the fresh series", got)
	}
}

func TestCompactAllExpiredDeletesEverything(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	flushCycles(t, db, 2)
	res, err := db.Compact(1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Samples != 0 || res.Dropped != 20 {
		t.Fatalf("result = %+v, want everything dropped", res)
	}
	if db.NumSegments() != 0 {
		t.Fatalf("NumSegments = %d, want 0", db.NumSegments())
	}
	if files := segFiles(t, dir); len(files) != 0 {
		t.Fatalf("files left on disk: %v", files)
	}
	// The DB keeps working after total expiry.
	if err := db.Append(NewLabels("m", nil), 2_000_000, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if db.NumSegments() != 1 {
		t.Fatal("flush after total expiry failed to mint a segment")
	}
}

func TestCompactNoOpCases(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// No segments at all.
	if res, err := db.Compact(0); err != nil || res.Inputs != 0 {
		t.Fatalf("empty compact: %+v, %v", res, err)
	}
	flushCycles(t, db, 1)
	// One segment, nothing expired: rewriting it would be pure write
	// amplification, so the call must not touch the file.
	before, err := os.Stat(filepath.Join(dir, "00000001.seg"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Compact(0)
	if err != nil || res.Inputs != 0 {
		t.Fatalf("single-segment compact: %+v, %v", res, err)
	}
	after, err := os.Stat(filepath.Join(dir, "00000001.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if !before.ModTime().Equal(after.ModTime()) || before.Size() != after.Size() {
		t.Fatal("no-op compact rewrote the segment file")
	}
}

func TestCompactReclaimsCrossSegmentDuplicates(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ls := NewLabels("m", nil)
	// t=1000 lands in segment 1 with value 111, and again in segment 2
	// with value 999 (the fresh post-flush head accepts it).
	if err := db.Append(ls, 1000, 111); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, s := range []Sample{{1000, 999}, {2000, 2}} {
		if err := db.Append(ls, s.T, s.V); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	res, err := db.Compact(0)
	if err != nil {
		t.Fatal(err)
	}
	// Until now every query deduped t=1000 on the fly; compaction removes
	// it physically, keeping the oldest write — the same rule Select uses.
	if res.Samples != 2 || res.Dropped != 1 {
		t.Fatalf("result = %+v, want 2 kept / 1 duplicate dropped", res)
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

func TestReopenAfterCrashMidCompaction(t *testing.T) {
	// Simulate dying between the merged rename and the stale-input
	// deletes: the merged segment holds everything, a stale newer segment
	// still holds a subset. Open must serve correct results, and the next
	// Compact must clean up.
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	flushCycles(t, db, 2)
	before, err := db.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve segment 2, compact (which deletes it), then restore it as
	// the crash leftover.
	stale, err := os.ReadFile(filepath.Join(dir, "00000002.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000002.seg"), stale, 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("open with crash leftovers: %v", err)
	}
	got, err := db2.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, got) {
		t.Fatalf("crash leftovers changed results:\nwant %v\ngot  %v", before, got)
	}
	if _, err := db2.Compact(0); err != nil {
		t.Fatal(err)
	}
	if files := segFiles(t, dir); !reflect.DeepEqual(files, []string{"00000001.seg"}) {
		t.Fatalf("straggler not cleaned: %v", files)
	}
}

func TestCompactRacesIngest(t *testing.T) {
	// Appends, flushes, compactions, and selects all race; the race
	// detector referees and the final sample total must balance.
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
			if _, err := db.Compact(0); err != nil {
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
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(0); err != nil {
		t.Fatal(err)
	}
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
	if db.NumSegments() != 1 {
		t.Fatalf("NumSegments = %d, want 1 after final compact", db.NumSegments())
	}
}
