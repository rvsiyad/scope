package tsdb

import (
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/rvsiyad/scope/internal/gorilla"
)

// Compaction is where the LSM shape pays rent. Every flush minted a small
// segment; left alone, a query would eventually merge hundreds of them
// (read amplification). Compaction rewrites many small segments into one
// (paying write amplification: retained samples are re-encoded even though
// they didn't change) to keep reads cheap — the classic
// read/write/space-amplification triangle, and this engine deliberately
// buys cheap reads with the full rewrite. Tiered and leveled policies are
// how production LSMs (RocksDB, Prometheus's time-window compaction)
// spend the triangle differently; at this engine's scale, full compaction
// is the honest simple corner.
//
// Retention rides along for free: the merge is already decoding and
// re-encoding every sample, so dropping the ones past the cutoff costs
// nothing extra — deletion in an immutable world is just "don't copy it
// forward".

// CompactResult reports what one Compact call did.
type CompactResult struct {
	// Inputs is how many segments were merged (0 = the call was a no-op).
	Inputs int
	// Series and Samples describe the merged output segment.
	Series  int
	Samples int
	// Dropped counts samples discarded by retention or as cross-segment
	// duplicates.
	Dropped int
}

// Compact merges every current segment into one, dropping samples older
// than retainAfter (0 keeps everything). The head is untouched: its size
// is already bounded by the flush cadence, and its samples will meet
// retention at their own first compaction after flushing.
//
// Ingest continues while the merge runs: only the snapshot and the final
// swap take the DB lock, and the merge works on immutable segments in
// between. Flush only ever appends to the segment list, so the snapshot
// stays a stable prefix of it.
//
// Crash-safety is ordering plus idempotence, no journal needed:
//
//  1. The merged output is written (tmp → fsync → rename) over the OLDEST
//     input's path, keeping segment names in chronological order.
//  2. Only then are the newer inputs deleted.
//
// A crash between 1 and 2 leaves the merged segment plus stale inputs
// whose samples are a subset of it — Open loads both and Select's
// first-write-wins dedupe returns correct results; the next Compact
// deletes the stragglers. A crash before 1 leaves the inputs untouched.
func (db *DB) Compact(retainAfter int64) (CompactResult, error) {
	// One compaction at a time; ingest holds db.mu, not this.
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.Lock()
	inputs := db.segments
	db.mu.Unlock()

	if len(inputs) == 0 {
		return CompactResult{}, nil
	}
	if len(inputs) == 1 && inputs[0].MinTime() >= retainAfter {
		// Nothing to merge, nothing to expire: rewriting one segment into
		// itself would be pure write amplification.
		return CompactResult{}, nil
	}

	series, ids, inTotal := mergeSegments(inputs, retainAfter)

	res := CompactResult{Inputs: len(inputs), Series: len(ids)}
	for _, id := range ids {
		res.Samples += series[id].enc.Len()
	}
	res.Dropped = inTotal - res.Samples

	var merged *Segment
	if len(ids) > 0 {
		path := inputs[0].Path()
		if err := writeSegmentFile(path, series, ids); err != nil {
			return CompactResult{}, err
		}
		var err error
		if merged, err = OpenSegment(path); err != nil {
			return CompactResult{}, fmt.Errorf("reopening compacted segment: %w", err)
		}
	} else if err := os.Remove(inputs[0].Path()); err != nil {
		// Everything expired: the oldest input is deleted rather than
		// replaced — an empty segment file is not a thing this format has.
		return CompactResult{}, err
	}
	for _, seg := range inputs[1:] {
		if err := os.Remove(seg.Path()); err != nil {
			return CompactResult{}, err
		}
	}

	db.mu.Lock()
	rest := db.segments[len(inputs):] // segments flushed while we merged
	if merged != nil {
		db.segments = append([]*Segment{merged}, rest...)
	} else {
		db.segments = append([]*Segment{}, rest...)
	}
	db.mu.Unlock()
	return res, nil
}

// mergeSegments decodes every retained sample from the inputs (oldest
// first) and re-encodes one memSeries per surviving series identity,
// returning them keyed for writeSegmentFile along with the total input
// sample count. Cross-segment duplicate timestamps collapse here —
// first-write-wins, same as Select — so compaction physically reclaims
// what the read path was deduping on every query.
func mergeSegments(inputs []*Segment, retainAfter int64) (map[uint64]*memSeries, []uint64, int) {
	type acc struct {
		labels  Labels
		samples []Sample
	}
	byKey := map[string]*acc{}
	inTotal := 0
	for _, seg := range inputs {
		inTotal += seg.NumSamples()
		for _, ss := range seg.series {
			// mint = retainAfter: retention enforcement is literally a
			// decode window.
			samples, err := decodeRange(ss.block, ss.meta.Samples, retainAfter, math.MaxInt64)
			if err != nil {
				// A block that decoded at Open time cannot fail here; see
				// Head.Select for the same reasoning.
				panic("tsdb: compaction failed to decode an open segment: " + err.Error())
			}
			if len(samples) == 0 {
				continue
			}
			key := ss.meta.Labels.Key()
			a, ok := byKey[key]
			if !ok {
				a = &acc{labels: ss.meta.Labels}
				byKey[key] = a
			}
			a.samples = append(a.samples, samples...)
		}
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	series := make(map[uint64]*memSeries, len(keys))
	ids := make([]uint64, 0, len(keys))
	for i, key := range keys {
		a := byKey[key]
		a.samples = sortDedup(a.samples)
		enc := gorilla.NewEncoder()
		for _, s := range a.samples {
			enc.Append(s.T, s.V)
		}
		id := uint64(i + 1)
		series[id] = &memSeries{
			labels: a.labels,
			enc:    enc,
			minT:   a.samples[0].T,
			maxT:   a.samples[len(a.samples)-1].T,
		}
		ids = append(ids, id)
	}
	return series, ids, inTotal
}
