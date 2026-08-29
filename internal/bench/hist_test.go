package bench

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

// The bucketing must be monotone (a bigger value never lands in an
// earlier bucket) and the inverse must land inside the bucket it names —
// the two properties every quantile answer rests on.
func TestIndexMonotoneAndInvertible(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	prev := int64(-1)
	for v := int64(0); v < 4096; v++ {
		checkValue(t, v, &prev)
	}
	prev = -1
	last := int64(0)
	for i := 0; i < 100_000; i++ {
		last += rng.Int63n(1 << 40)
		checkValue(t, last, &prev)
	}
}

func checkValue(t *testing.T, v int64, prevIdx *int64) {
	t.Helper()
	idx := indexOf(v)
	if int64(idx) < *prevIdx {
		t.Fatalf("indexOf not monotone at %d: idx %d after %d", v, idx, *prevIdx)
	}
	*prevIdx = int64(idx)
	if got := indexOf(valueOf(idx)); got != idx {
		t.Fatalf("valueOf(%d)=%d maps back to bucket %d", idx, valueOf(idx), got)
	}
}

// Quantiles must agree with the exact answer from a sorted copy within
// the bucketing's promised ~1.6% relative error.
func TestQuantilesMatchExactWithinBucketError(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	h := &Hist{}
	samples := make([]int64, 200_000)
	for i := range samples {
		// Log-uniform-ish spread: latencies live at every magnitude.
		v := int64(1) << rng.Intn(34)
		v += rng.Int63n(v)
		samples[i] = v
		h.Record(time.Duration(v))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := samples[int(q*float64(len(samples)))-1]
		got := int64(h.Quantile(q))
		if diff := got - exact; diff < -exact/32 || diff > exact/32 {
			t.Errorf("q=%v: got %d, exact %d (off by %.2f%%)",
				q, got, exact, 100*float64(got-exact)/float64(exact))
		}
	}
	if h.Count() != int64(len(samples)) {
		t.Errorf("count %d, want %d", h.Count(), len(samples))
	}
	if int64(h.Max()) != samples[len(samples)-1] {
		t.Errorf("max %d, want exact %d", h.Max(), samples[len(samples)-1])
	}
}

func TestEmptyHist(t *testing.T) {
	h := &Hist{}
	if h.Quantile(0.99) != 0 || h.Mean() != 0 || h.Count() != 0 {
		t.Error("empty histogram should answer zero")
	}
}
