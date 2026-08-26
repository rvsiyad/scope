package tsdb

import "sort"

// The inverted label index: label pair → sorted posting list of series
// IDs, exactly a search engine's term → document list. A multi-matcher
// query intersects posting lists instead of scanning series, which is the
// entire reason label lookups stay fast as series count grows — and why
// unbounded label values (cardinality explosions) are what kill real
// TSDBs: every new value mints a new posting list and a new series.

// memIndex is the in-memory series registry + inverted index used by the
// head block. Not safe for concurrent use; the head wraps it in its lock.
type memIndex struct {
	nextID uint64
	// byKey resolves a canonical label set to its series ID; ids holds the
	// reverse mapping.
	byKey map[string]uint64
	ids   map[uint64]Labels
	// postings maps each label pair to the ascending list of series IDs
	// carrying it.
	postings map[Label][]uint64
}

func newMemIndex() *memIndex {
	return &memIndex{
		byKey:    map[string]uint64{},
		ids:      map[uint64]Labels{},
		postings: map[Label][]uint64{},
	}
}

// getOrCreate returns the series ID for a label set, registering it (and
// its postings) on first sight. created tells the caller whether to
// allocate series storage.
func (ix *memIndex) getOrCreate(ls Labels) (id uint64, created bool) {
	key := ls.Key()
	if id, ok := ix.byKey[key]; ok {
		return id, false
	}
	ix.nextID++
	id = ix.nextID
	ix.byKey[key] = id
	ix.ids[id] = ls
	for _, l := range ls {
		// IDs are allocated in ascending order and each appears once per
		// pair, so appending keeps every posting list sorted for free.
		ix.postings[l] = append(ix.postings[l], id)
	}
	return id, true
}

func (ix *memIndex) numSeries() int { return len(ix.byKey) }

// selectIDs returns the ascending IDs of series matching all matchers.
// Equality matchers drive posting-list intersection (start from the
// shortest list — intersection can only shrink); the remaining matchers
// filter the survivors against their full label sets. With no equality
// matcher there is nothing to intersect and the candidate set is every
// series — the query layer's cue that such queries are inherently scans.
func (ix *memIndex) selectIDs(matchers []Matcher) []uint64 {
	var eqLists [][]uint64
	for _, m := range matchers {
		if m.Type == MatchEq {
			list, ok := ix.postings[Label{m.Name, m.Value}]
			if !ok {
				return nil // some required pair exists nowhere: empty result
			}
			eqLists = append(eqLists, list)
		}
	}

	var candidates []uint64
	if len(eqLists) > 0 {
		sort.Slice(eqLists, func(i, j int) bool { return len(eqLists[i]) < len(eqLists[j]) })
		candidates = eqLists[0]
		for _, list := range eqLists[1:] {
			candidates = intersect(candidates, list)
			if len(candidates) == 0 {
				return nil
			}
		}
	} else {
		candidates = make([]uint64, 0, len(ix.ids))
		for id := uint64(1); id <= ix.nextID; id++ {
			if _, ok := ix.ids[id]; ok {
				candidates = append(candidates, id)
			}
		}
	}

	out := candidates[:0:0]
	for _, id := range candidates {
		ls := ix.ids[id]
		ok := true
		for _, m := range matchers {
			if !m.Matches(ls) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, id)
		}
	}
	return out
}

// intersect merges two ascending lists, the classic two-pointer walk.
func intersect(a, b []uint64) []uint64 {
	out := make([]uint64, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}
