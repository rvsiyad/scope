// Package bench is the measurement harness behind docs/benchmarks.md: a
// latency histogram and an open-loop load pacer, both built here because
// both are places where the obvious implementation quietly lies. A slice
// of samples sorted at the end is exact but unbounded; a fixed-bucket
// histogram is bounded but wrecks the tail resolution percentiles live
// in. And a closed loop that waits for each response before sending the
// next measures only the requests the system felt like serving — the
// coordinated-omission mistake the pacer exists to avoid.
package bench

import (
	"fmt"
	"math/bits"
	"sync/atomic"
	"time"
)

// The histogram is HdrHistogram-shaped: values up to 127 count exactly,
// and every power-of-two octave above that is split into 64 linear
// sub-buckets, bounding relative error at 1/64 (~1.6%) at any magnitude —
// a microsecond stays a microsecond and a second stays a second, in a
// few KB of counters, recordable from any number of goroutines.
const subBits = 6

const exactMax = 1 << (subBits + 1) // 0..127 recorded exactly

// numBuckets covers every int64: 56 octaves above the exact range.
const numBuckets = exactMax + (63-subBits-1)<<subBits

// Hist records durations and answers quantile questions about them.
// Record is safe for concurrent use; readers should wait until recording
// is done (counts are monotone, so a racing read is stale, not wrong).
type Hist struct {
	counts [numBuckets]int64
	total  atomic.Int64
	max    atomic.Int64
}

func indexOf(v int64) int {
	if v < exactMax {
		return int(v)
	}
	exp := bits.Len64(uint64(v)) - (subBits + 1)
	sub := int(v>>exp) & (1<<subBits - 1)
	return exactMax + (exp-1)<<subBits + sub
}

// valueOf is indexOf's inverse, up to bucket width: the midpoint of the
// bucket, so quantile answers err toward neither side systematically.
func valueOf(idx int) int64 {
	if idx < exactMax {
		return int64(idx)
	}
	exp := (idx - exactMax) >> subBits
	sub := int64((idx-exactMax)&(1<<subBits-1)) | 1<<subBits
	return sub<<(exp+1) | 1<<exp // bucket floor + half a bucket width
}

// Record adds one duration. Negative durations (clock steps) clamp to 0.
func (h *Hist) Record(d time.Duration) {
	v := int64(d)
	if v < 0 {
		v = 0
	}
	atomic.AddInt64(&h.counts[indexOf(v)], 1)
	h.total.Add(1)
	for {
		old := h.max.Load()
		if v <= old || h.max.CompareAndSwap(old, v) {
			break
		}
	}
}

// Count is the number of recorded values.
func (h *Hist) Count() int64 { return h.total.Load() }

// Max is the largest recorded value, exact (not bucketed).
func (h *Hist) Max() time.Duration { return time.Duration(h.max.Load()) }

// Quantile returns the smallest recorded bucket below which at least
// q of the values fall, as a duration. q outside (0,1] is clamped.
func (h *Hist) Quantile(q float64) time.Duration {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	if q <= 0 {
		q = 1e-9
	}
	if q > 1 {
		q = 1
	}
	rank := int64(q*float64(total) + 0.5)
	if rank < 1 {
		rank = 1
	}
	var seen int64
	for i := range h.counts {
		seen += atomic.LoadInt64(&h.counts[i])
		if seen >= rank {
			return time.Duration(valueOf(i))
		}
	}
	return h.Max()
}

// Mean is the average of the bucket-resolution values.
func (h *Hist) Mean() time.Duration {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	var sum int64
	for i := range h.counts {
		if c := atomic.LoadInt64(&h.counts[i]); c > 0 {
			sum += c * valueOf(i)
		}
	}
	return time.Duration(sum / total)
}

// Summary is the one-line report every benchmark prints: percentiles a
// reader can compare across runs.
func (h *Hist) Summary() string {
	return fmt.Sprintf("n=%d p50=%v p90=%v p99=%v p99.9=%v max=%v",
		h.Count(), round(h.Quantile(0.50)), round(h.Quantile(0.90)),
		round(h.Quantile(0.99)), round(h.Quantile(0.999)), round(h.Max()))
}

// round trims durations to a legible precision for reports.
func round(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(100 * time.Nanosecond)
	}
}
