package gorilla

import (
	"math"
	"math/rand"
	"testing"
)

func roundTripTimestamps(t *testing.T, ts []int64) []byte {
	t.Helper()
	w := &bitWriter{}
	enc := tsEncoder{}
	for _, v := range ts {
		enc.append(w, v)
	}
	r := newBitReader(w.bytes())
	dec := tsDecoder{}
	for i, want := range ts {
		got, err := dec.next(r)
		if err != nil {
			t.Fatalf("timestamp %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("timestamp %d: got %d, want %d", i, got, want)
		}
	}
	return w.bytes()
}

func TestTimestampsRegularClock(t *testing.T) {
	// The steady-state case the encoding exists for: a perfectly regular
	// clock costs one bit per sample after the first two.
	ts := make([]int64, 1000)
	base := int64(1_700_000_000_000)
	for i := range ts {
		ts[i] = base + int64(i)*15_000 // 15s scrape interval, in ms
	}
	b := roundTripTimestamps(t, ts)

	// 64 bits header + ~1 sample paying for the first delta + 998 single
	// bits ≈ 135 bytes. Anything near raw (8000 bytes) means the dod path
	// is broken even though decode still round-trips.
	if len(b) > 200 {
		t.Fatalf("regular clock compressed to %d bytes, expected ~135", len(b))
	}
}

func TestTimestampsJitteredClock(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	ts := make([]int64, 2000)
	tt := int64(1_700_000_000_000)
	for i := range ts {
		tt += 1000 + int64(rng.Intn(40)) - 20 // 1s ± 20ms jitter
		ts[i] = tt
	}
	roundTripTimestamps(t, ts)
}

func TestTimestampsAdversarial(t *testing.T) {
	cases := map[string][]int64{
		"single":            {12345},
		"two":               {12345, 12346},
		"identical":         {5, 5, 5, 5},
		"backwards":         {1000, 500, 2000, 1},
		"bucket boundaries": {0, 64, 64 + 64 + 65, 300, 556, 2900, 5000},
		"extremes":          {math.MinInt64 + 10, 0, math.MaxInt64 - 10, 0},
		"negative":          {-1_000_000, -999_000, -998_000, -500},
	}
	for name, ts := range cases {
		t.Run(name, func(t *testing.T) { roundTripTimestamps(t, ts) })
	}
}

// Property test: any sequence of int64 timestamps round-trips exactly —
// the codec's compression may degrade on hostile input, its correctness
// must not.
func TestTimestampsPropertyRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 200; trial++ {
		n := 1 + rng.Intn(300)
		ts := make([]int64, n)
		mode := trial % 4
		v := rng.Int63() - rng.Int63()
		for i := range ts {
			switch mode {
			case 0: // small forward steps
				v += int64(rng.Intn(5000))
			case 1: // dod hovers around bucket edges
				v += int64([]int{63, 64, 65, 256, 257, 2048, 2049}[rng.Intn(7)])
			case 2: // random walk, both directions
				v += int64(rng.Intn(100000)) - 50000
			case 3: // full-range chaos
				v = rng.Int63() - rng.Int63()
			}
			ts[i] = v
		}
		roundTripTimestamps(t, ts)
	}
}

func TestBitstreamRoundTrip(t *testing.T) {
	w := &bitWriter{}
	w.writeBit(1)
	w.writeBits(0b101, 3)
	w.writeBits(0xDEADBEEFCAFE, 48)
	w.writeBit(0)
	w.writeBits(0x3FF, 10)

	r := newBitReader(w.bytes())
	for _, step := range []struct {
		n    uint
		want uint64
	}{{1, 1}, {3, 0b101}, {48, 0xDEADBEEFCAFE}, {1, 0}, {10, 0x3FF}} {
		got, err := r.readBits(step.n)
		if err != nil {
			t.Fatal(err)
		}
		if got != step.want {
			t.Fatalf("readBits(%d) = %#x, want %#x", step.n, got, step.want)
		}
	}
}

func TestBitReaderExhaustion(t *testing.T) {
	r := newBitReader([]byte{0xFF})
	if _, err := r.readBits(9); err == nil {
		t.Fatal("reading past the stream must error, not fabricate bits")
	}
}
