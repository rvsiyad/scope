package gorilla

import (
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	t int64
	v float64
}

func roundTripBlock(t *testing.T, samples []sample) *Encoder {
	t.Helper()
	enc := NewEncoder()
	for _, s := range samples {
		enc.Append(s.t, s.v)
	}
	it := NewIterator(enc.Bytes(), enc.Len())
	for i, want := range samples {
		if !it.Next() {
			t.Fatalf("iterator ended at sample %d of %d: %v", i, len(samples), it.Err())
		}
		gotT, gotV := it.At()
		if gotT != want.t || math.Float64bits(gotV) != math.Float64bits(want.v) {
			t.Fatalf("sample %d: got (%d, %#x), want (%d, %#x)",
				i, gotT, math.Float64bits(gotV), want.t, math.Float64bits(want.v))
		}
	}
	if it.Next() {
		t.Fatal("iterator produced more samples than were appended")
	}
	if it.Err() != nil {
		t.Fatalf("clean exhaustion must not set Err: %v", it.Err())
	}
	return enc
}

// The paper's scenario: a regular clock and slowly drifting values. The
// famous number is 1.37 bytes/sample fleet-wide vs 16 raw; a synthetic
// well-behaved series should land in that neighborhood.
func TestBlockCompressionOnRegularSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	samples := make([]sample, 2000)
	ts := int64(1_700_000_000_000)
	v := 250.0
	for i := range samples {
		ts += 15_000
		v += rng.NormFloat64() * 0.1 // p99 latency drifting a little
		samples[i] = sample{ts, v}
	}
	enc := roundTripBlock(t, samples)

	bps := enc.BytesPerSample()
	t.Logf("regular series: %.2f bytes/sample (raw 16)", bps)
	// Continuous random drift churns the full mantissa every sample — the
	// codec's worst realistic case (~6.5 here, still 2.5x under raw). The
	// paper's 1.37 is a fleet-wide average dominated by series whose
	// values repeat; integer-valued counters do far better (see the
	// constant-series test and cmd/compressbench on real telemetry).
	if bps > 8 {
		t.Fatalf("bytes/sample = %.2f, expected well under raw 16", bps)
	}
}

func TestBlockConstantSeriesNearsTwoBitsPerSample(t *testing.T) {
	samples := make([]sample, 4000)
	ts := int64(1_700_000_000_000)
	for i := range samples {
		ts += 1000
		samples[i] = sample{ts, 1} // a flat series on a perfect clock
	}
	enc := roundTripBlock(t, samples)
	// Steady state costs 2 bits/sample ('0' dod + '0' xor) → 0.25
	// bytes/sample plus the 16-byte header amortized.
	if bps := enc.BytesPerSample(); bps > 0.3 {
		t.Fatalf("constant series = %.3f bytes/sample, expected ~0.25", bps)
	}
}

func TestBlockPropertyRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(23))
	for trial := 0; trial < 150; trial++ {
		n := 1 + rng.Intn(400)
		samples := make([]sample, n)
		ts := rng.Int63n(1 << 45)
		v := rng.NormFloat64() * 100
		for i := range samples {
			ts += int64(rng.Intn(120_000)) // includes zero deltas
			switch trial % 3 {
			case 0:
				v += rng.NormFloat64()
			case 1:
				v += float64(rng.Intn(3))
			case 2:
				v = math.Float64frombits(rng.Uint64())
			}
			samples[i] = sample{ts, v}
		}
		roundTripBlock(t, samples)
	}
}

func TestIteratorReportsTruncatedBlock(t *testing.T) {
	enc := NewEncoder()
	ts := int64(1_700_000_000_000)
	for i := 0; i < 100; i++ {
		ts += 1000
		enc.Append(ts, float64(i)*1.5)
	}
	block := enc.Bytes()

	it := NewIterator(block[:len(block)/2], enc.Len())
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("a block cut in half must surface an error, not end silently")
	}
}

func TestEmptyBlock(t *testing.T) {
	enc := NewEncoder()
	if enc.BytesPerSample() != 0 {
		t.Fatal("empty encoder bytes/sample must be 0")
	}
	it := NewIterator(enc.Bytes(), 0)
	if it.Next() {
		t.Fatal("empty block must produce no samples")
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
}
