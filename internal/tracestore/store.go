package tracestore

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/rvsiyad/scope/internal/telemetry"
)

// Store ties the pieces into one trace store: appends land in the head
// (through the sampler), Flush freezes the head into the next numbered
// segment file, and reads answer over head + segments as if they were one
// dataset. Callers never learn where a trace physically lives — including
// a trace split by a flush, whose halves reassemble on every read.

// Store is the trace store's front door. Safe for concurrent use.
type Store struct {
	dir     string
	head    *Head
	sampler Sampler // nil keeps everything

	// mu guards the segment list and flush sequencing, with the tsdb's
	// discipline: reads copy the slice header under the lock and then read
	// the immutable segments lock-free.
	mu       sync.Mutex
	segments []*Segment // oldest → newest by sequence number
	nextSeq  uint64
}

// Open loads a Store from dir, creating it if needed. Existing segments
// open oldest-first; one that fails to open fails the whole Open — serving
// waterfalls while silently missing a chunk of history would be the store
// lying. Stale .tmp files from a flush that died before its rename are
// deleted: their data was never acknowledged as flushed and still lives in
// the WAL upstream.
//
// The head starts empty: spans that were in the head at crash time come
// back via the collector's WAL replay, exactly as the tsdb's do.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	st := &Store{dir: dir, head: NewHead(), nextSeq: 1}
	var names []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tseg.tmp"):
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
		case strings.HasSuffix(name, ".tseg"):
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
		if _, err := fmt.Sscanf(name, "%d.tseg", &seq); err != nil {
			return nil, fmt.Errorf("trace segment %s: unparseable name: %w", name, err)
		}
		st.segments = append(st.segments, seg)
		if seq >= st.nextSeq {
			st.nextSeq = seq + 1
		}
	}
	return st, nil
}

// SetSampler installs the keep/drop policy applied at Append (nil keeps
// everything). Set once at startup, before ingest: changing the policy
// mid-flight would split verdicts within a trace.
func (st *Store) SetSampler(s Sampler) { st.sampler = s }

// Append adds one span, unless its trace is sampled out. Returns whether
// the span was kept — the caller's scoreboard wants to count drops, and a
// sampled-out span is a policy outcome, not an error.
func (st *Store) Append(sp telemetry.Span) bool {
	if st.sampler != nil && !st.sampler(sp.TraceID) {
		return false
	}
	st.head.Append(sp)
	return true
}

// Flush freezes the current head into the next segment file. As in the
// tsdb, the new segment is opened back from disk before it joins the read
// set — reads serve what the file actually says, so a write bug surfaces
// here as an error, never later as a broken waterfall. A flush of an empty
// head is a no-op.
func (st *Store) Flush() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	path := filepath.Join(st.dir, fmt.Sprintf("%08d.tseg", st.nextSeq))
	n, err := st.head.Flush(path)
	if err != nil || n == 0 {
		return err
	}
	seg, err := OpenSegment(path)
	if err != nil {
		return fmt.Errorf("reopening just-flushed trace segment: %w", err)
	}
	st.segments = append(st.segments, seg)
	st.nextSeq++
	return nil
}

// Trace returns the id's full span list — every source merged, duplicates
// dropped by span id (oldest source wins, the same rule everywhere in this
// project), sorted by start time. Returns nil if no source has ever seen
// the id.
func (st *Store) Trace(id string) ([]telemetry.Span, error) {
	st.mu.Lock()
	segments := st.segments
	st.mu.Unlock()

	var out []telemetry.Span
	seen := map[string]struct{}{}
	add := func(spans []telemetry.Span) {
		for _, sp := range spans {
			if _, dup := seen[sp.SpanID]; dup {
				continue
			}
			seen[sp.SpanID] = struct{}{}
			out = append(out, sp)
		}
	}
	for _, seg := range segments {
		spans, err := seg.Trace(id)
		if err != nil {
			return nil, err
		}
		add(spans)
	}
	add(st.head.Trace(id))
	if out == nil {
		return nil, nil
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out, nil
}

// List returns directory entries for traces overlapping [mint, maxt],
// newest first, at most limit (0 = unlimited). A trace living in several
// sources (split by a flush, or re-delivered by WAL replay) appears once,
// with merged bounds; its span count is the maximum any single source
// holds — exact counts would mean decoding span bodies, which a directory
// listing must not do. The waterfall read has the exact truth.
func (st *Store) List(mint, maxt int64, limit int) []TraceInfo {
	st.mu.Lock()
	segments := st.segments
	st.mu.Unlock()

	merged := map[string]*TraceInfo{}
	add := func(infos []TraceInfo) {
		for _, info := range infos {
			acc, ok := merged[info.TraceID]
			if !ok {
				c := info
				merged[info.TraceID] = &c
				continue
			}
			if info.MinStart < acc.MinStart {
				acc.MinStart = info.MinStart
			}
			if info.MaxEnd > acc.MaxEnd {
				acc.MaxEnd = info.MaxEnd
			}
			if info.Spans > acc.Spans {
				acc.Spans = info.Spans
			}
		}
	}
	for _, seg := range segments {
		if seg.MaxTime() < mint || seg.MinTime() > maxt {
			continue
		}
		add(seg.List(mint, maxt))
	}
	add(st.head.List(mint, maxt))

	out := make([]TraceInfo, 0, len(merged))
	for _, info := range merged {
		out = append(out, *info)
	}
	sortInfos(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// EnforceRetention drops whole segments whose newest span ends before
// cutoff, deleting their files, and returns how many were dropped. Whole
// segments only: traces are never rewritten to trim a file, so a segment
// straddling the cutoff survives until all of it is old — retention is a
// bound, not a knife edge, the same call the tsdb's compactor makes.
//
// There is deliberately no merge compaction here (the tsdb needed it; this
// store does not yet): a range scan degrades linearly with segment count,
// but a read-by-id is one map probe per segment, so many small segments
// cost microseconds until segment counts get far past this project's
// scale. The moment List over long windows earns it, the tsdb's compactor
// is the pattern to copy — that asymmetry is ADR 0006 material.
func (st *Store) EnforceRetention(cutoff int64) (int, error) {
	if cutoff <= 0 {
		return 0, nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	kept := st.segments[:0:0]
	dropped := 0
	for i, seg := range st.segments {
		if seg.MaxTime() < cutoff {
			if err := os.Remove(seg.Path()); err != nil {
				// The file still exists, so the segment stays in the read
				// set: losing track of a live file would be worse than
				// serving old data one interval longer.
				st.segments = append(kept, st.segments[i:]...)
				return dropped, err
			}
			dropped++
			continue
		}
		kept = append(kept, seg)
	}
	st.segments = kept
	if dropped > 0 {
		return dropped, syncDir(st.dir)
	}
	return dropped, nil
}

// NumSegments reports how many segment files back the store.
func (st *Store) NumSegments() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.segments)
}

// Head exposes the live head (its scoreboard feeds /healthz).
func (st *Store) Head() *Head { return st.head }
