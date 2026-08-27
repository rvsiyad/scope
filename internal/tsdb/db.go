package tsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// DB ties the pieces into one store: appends land in the head block, Flush
// freezes the head into the next numbered segment file, and Select answers
// over head + segments as if they were one dataset. Callers never learn
// where a sample physically lives — that boundary moving over time (every
// flush, and S8's compaction) is exactly what the abstraction hides.

// DB is the storage engine's front door. Safe for concurrent use.
type DB struct {
	dir  string
	head *Head

	// mu guards the segment list and flush sequencing. Queries copy the
	// slice header under the lock and then read the segments lock-free —
	// segments are immutable, so a query racing a flush simply sees the
	// list from just before or just after, both of them consistent.
	mu       sync.Mutex
	segments []*Segment // oldest → newest by sequence number
	nextSeq  uint64
}

// Open loads a DB from dir, creating it if needed. Existing segments are
// opened oldest-first; a segment that fails to open fails the whole Open —
// serving queries while silently missing a chunk of history would be the
// storage engine lying, and refusing to start is the same call the
// collector makes on an unopenable WAL. Stale .tmp files from a flush that
// died before its rename are deleted: the data they hold was never
// acknowledged as flushed and still lives in the WAL upstream.
//
// The head starts empty: samples that were in the head at crash time come
// back via the collector's WAL replay (wired in S8), not from disk here.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	db := &DB{dir: dir, head: NewHead(), nextSeq: 1}
	var names []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".seg.tmp"):
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
		case strings.HasSuffix(name, ".seg"):
			names = append(names, name)
		}
	}
	// Zero-padded sequence names make lexical order chronological order.
	sort.Strings(names)
	for _, name := range names {
		seg, err := OpenSegment(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var seq uint64
		if _, err := fmt.Sscanf(name, "%d.seg", &seq); err != nil {
			return nil, fmt.Errorf("segment %s: unparseable name: %w", name, err)
		}
		db.segments = append(db.segments, seg)
		if seq >= db.nextSeq {
			db.nextSeq = seq + 1
		}
	}
	return db, nil
}

// Append adds one sample (see Head.Append for the ordering contract).
func (db *DB) Append(ls Labels, t int64, v float64) error {
	return db.head.Append(ls, t, v)
}

// Flush freezes the current head into the next segment file. The new
// segment is opened back from disk before it joins the query set — queries
// serve what the file actually says, so a write bug surfaces here as an
// error, never later as wrong query results. A flush of an empty head is a
// no-op.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	path := filepath.Join(db.dir, fmt.Sprintf("%08d.seg", db.nextSeq))
	n, err := db.head.Flush(path)
	if err != nil || n == 0 {
		return err
	}
	seg, err := OpenSegment(path)
	if err != nil {
		return fmt.Errorf("reopening just-flushed segment: %w", err)
	}
	db.segments = append(db.segments, seg)
	db.nextSeq++
	return nil
}

// Select answers a query over everything the DB holds: segments oldest
// first, then the head, merged per series identity into one ascending
// sample list. Whole segments outside the window are skipped by their time
// bounds without touching their series. Results follow the same contract
// as the sources: sorted by canonical key, bounds inclusive, no empty
// series.
//
// If the same timestamp exists in more than one place (a late append after
// a flush re-minted the series in a fresh head), the oldest source wins —
// the same first-write-wins rule the head applies to duplicates within one
// stream.
func (db *DB) Select(matchers []Matcher, mint, maxt int64) ([]Series, error) {
	db.mu.Lock()
	segments := db.segments
	db.mu.Unlock()

	order := make([]string, 0, 8)  // first-seen key order, re-sorted at the end
	merged := map[string]*Series{} // key → accumulating result
	add := func(results []Series) {
		for _, s := range results {
			key := s.Labels.Key()
			acc, ok := merged[key]
			if !ok {
				acc = &Series{Labels: s.Labels}
				merged[key] = acc
				order = append(order, key)
			}
			acc.Samples = append(acc.Samples, s.Samples...)
		}
	}
	for _, seg := range segments {
		if seg.MaxTime() < mint || seg.MinTime() > maxt {
			continue
		}
		results, err := seg.Select(matchers, mint, maxt)
		if err != nil {
			return nil, err
		}
		add(results)
	}
	add(db.head.Select(matchers, mint, maxt))

	out := make([]Series, 0, len(order))
	for _, key := range order {
		s := merged[key]
		s.Samples = sortDedup(s.Samples)
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.Key() < out[j].Labels.Key() })
	return out, nil
}

// sortDedup puts concatenated per-source runs into one ascending list,
// keeping the first occurrence of a duplicated timestamp. Sources arrive
// oldest-first and each run is already ascending, so the stable sort is
// usually a single verification pass — and preserves source order between
// equal timestamps, which is what makes "first occurrence" mean "oldest
// source".
func sortDedup(samples []Sample) []Sample {
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].T < samples[j].T })
	out := samples[:0]
	for i, s := range samples {
		if i > 0 && s.T == out[len(out)-1].T {
			continue
		}
		out = append(out, s)
	}
	return out
}

// NumSegments reports how many segment files back the DB.
func (db *DB) NumSegments() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.segments)
}

// Head exposes the live head block (its scoreboard feeds /healthz and the
// S8 cardinality guard).
func (db *DB) Head() *Head { return db.head }
