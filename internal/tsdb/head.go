package tsdb

import (
	"errors"
	"sort"
	"sync"

	"github.com/rvsiyad/scope/internal/gorilla"
)

// The head block: the newest samples of every series, held in memory as one
// live Gorilla stream per series. Appends go straight into the compressed
// stream — there is no uncompressed buffer to flush, which is the Gorilla
// paper's actual trick for fitting a fleet's metrics in RAM. Durability is
// not the head's job: the collector's WAL sits in front of it, so a crash
// loses nothing acknowledged (S8 wires the replay).

// Sample is one timestamp/value pair. Timestamps are unix milliseconds
// everywhere in this engine (telemetry.MetricPoint sets the unit).
type Sample struct {
	T int64
	V float64
}

// Series is one query result: a series identity and its samples in
// ascending time order.
type Series struct {
	Labels  Labels
	Samples []Sample
}

// ErrOutOfOrder rejects a sample at or before its series' newest sample.
// The Gorilla stream is append-only and delta-encoded, so it physically
// cannot hold out-of-order samples — the error is the storage layout
// showing through, the same reason Prometheus rejects them. The rejected
// sample leaves no trace: the stream is untouched and later in-order
// appends succeed.
var ErrOutOfOrder = errors.New("tsdb: sample out of order for series")

// memSeries is one series' live state in the head.
type memSeries struct {
	labels Labels
	enc    *gorilla.Encoder
	minT   int64
	maxT   int64 // also the ordering watermark for appends
}

// Head is the in-memory head block. Safe for concurrent use; one RWMutex
// guards everything, which is deliberate at this scale — Prometheus stripes
// the series map across locks, and that is the upgrade path if appends ever
// contend, not a place to spend complexity now.
type Head struct {
	mu     sync.RWMutex
	ix     *memIndex
	series map[uint64]*memSeries
}

func NewHead() *Head {
	return &Head{ix: newMemIndex(), series: map[uint64]*memSeries{}}
}

// Append adds one sample to the series identified by ls, registering the
// series on first sight. Samples per series must be strictly ascending in
// time (see ErrOutOfOrder).
func (h *Head) Append(ls Labels, t int64, v float64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	id, created := h.ix.getOrCreate(ls)
	s := h.series[id]
	if created {
		s = &memSeries{labels: ls, enc: gorilla.NewEncoder(), minT: t}
		h.series[id] = s
	} else if t <= s.maxT {
		return ErrOutOfOrder
	}
	s.enc.Append(t, v)
	s.maxT = t
	return nil
}

// Select returns the series matching all matchers, restricted to samples
// with mint <= t <= maxt, sorted by canonical label key. Series with no
// samples in the window are omitted — a query result never contains empty
// series.
func (h *Head) Select(matchers []Matcher, mint, maxt int64) []Series {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []Series
	for _, id := range h.ix.selectIDs(matchers) {
		s := h.series[id]
		if s.enc.Len() == 0 || s.maxT < mint || s.minT > maxt {
			continue
		}
		samples, err := decodeRange(s.enc.Bytes(), s.enc.Len(), mint, maxt)
		if err != nil {
			// The head decoding its own live stream cannot fail unless the
			// codec is broken; surfacing a corrupt query result would be
			// worse than surfacing none.
			panic("tsdb: head block failed to decode its own stream: " + err.Error())
		}
		if len(samples) > 0 {
			out = append(out, Series{Labels: s.labels, Samples: samples})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.Key() < out[j].Labels.Key() })
	return out
}

// decodeRange decodes a Gorilla block and keeps the samples inside
// [mint, maxt]. Gorilla blocks have no internal seek structure — decoding
// is always a scan from the front, which is fine because blocks are small
// and per-series; the time filtering happens here, once, for head and
// segment reads alike.
func decodeRange(block []byte, count int, mint, maxt int64) ([]Sample, error) {
	var out []Sample
	it := gorilla.NewIterator(block, count)
	for it.Next() {
		t, v := it.At()
		if t > maxt {
			break // samples are ascending: nothing later can be in range
		}
		if t >= mint {
			out = append(out, Sample{t, v})
		}
	}
	return out, it.Err()
}

// NumSeries and NumSamples are the head's scoreboard (and the future
// cardinality guard's input).
func (h *Head) NumSeries() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ix.numSeries()
}

func (h *Head) NumSamples() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, s := range h.series {
		n += s.enc.Len()
	}
	return n
}
