package gorilla

import (
	"math"
	"math/rand"
	"testing"
)

func roundTripValues(t *testing.T, vals []float64) []byte {
	t.Helper()
	w := &bitWriter{}
	enc := valEncoder{}
	for _, v := range vals {
		enc.append(w, math.Float64bits(v))
	}
	r := newBitReader(w.bytes())
	dec := valDecoder{}
	for i, want := range vals {
		got, err := dec.next(r)
		if err != nil {
			t.Fatalf("value %d: %v", i, err)
		}
		// Bit-pattern equality, not ==: NaN != NaN and -0 == 0 would both
		// lie about whether the codec is exact.
		if got != math.Float64bits(want) {
			t.Fatalf("value %d: got %#x, want %#x", i, got, math.Float64bits(want))
		}
	}
	return w.bytes()
}

func TestValuesConstantSeries(t *testing.T) {
	// A flat gauge is the '0'-bit case: 1000 samples ≈ 8 bytes header +
	// 999 bits ≈ 133 bytes.
	vals := make([]float64, 1000)
	for i := range vals {
		vals[i] = 98.6
	}
	if b := roundTripValues(t, vals); len(b) > 150 {
		t.Fatalf("constant series compressed to %d bytes, expected ~133", len(b))
	}
}

func TestValuesSlowCounter(t *testing.T) {
	// A counter incrementing by small integers: close values share leading
	// bits, so the sticky window should keep this well under raw.
	vals := make([]float64, 1000)
	total := 0.0
	rng := rand.New(rand.NewSource(2))
	for i := range vals {
		total += float64(rng.Intn(5))
		vals[i] = total
	}
	if b := roundTripValues(t, vals); len(b) > 4000 { // raw = 8000
		t.Fatalf("slow counter compressed to %d bytes, expected well under raw 8000", len(b))
	}
}

func TestValuesSpecialFloats(t *testing.T) {
	cases := map[string][]float64{
		"single":        {3.14},
		"zeros":         {0, math.Copysign(0, -1), 0, math.Copysign(0, -1)},
		"nan":           {math.NaN(), 1.5, math.NaN(), math.NaN()},
		"infinities":    {math.Inf(1), math.Inf(-1), math.Inf(1)},
		"denormals":     {5e-324, 1e-310, 5e-324},
		"max min":       {math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64},
		"sign flips":    {1.0, -1.0, 1.0, -1.0},
		"full mantissa": {1.0000000000000002, 1.0000000000000004, 1.0},
	}
	for name, vals := range cases {
		t.Run(name, func(t *testing.T) { roundTripValues(t, vals) })
	}
}

// Property test: every float64 bit pattern round-trips exactly, whatever
// the sequence shape.
func TestValuesPropertyRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for trial := 0; trial < 200; trial++ {
		n := 1 + rng.Intn(300)
		vals := make([]float64, n)
		mode := trial % 4
		v := rng.NormFloat64() * 1000
		for i := range vals {
			switch mode {
			case 0: // gauge drifting slowly
				v += rng.NormFloat64()
			case 1: // counter
				v += math.Abs(rng.NormFloat64() * 10)
			case 2: // repeated runs (exercises the '0' bit mid-stream)
				if rng.Intn(3) != 0 {
					v = math.Float64frombits(rng.Uint64())
				}
			case 3: // raw bit chaos, including NaNs/infs/denormals
				v = math.Float64frombits(rng.Uint64())
			}
			vals[i] = v
		}
		roundTripValues(t, vals)
	}
}

// The sticky window must shrink when a value stops fitting it — the '11'
// re-describe path — and reads must agree after every transition.
func TestValuesWindowTransitions(t *testing.T) {
	vals := []float64{
		1.5,     // header
		1.5,     // identical → '0'
		1.75,    // small xor → new window
		1.875,   // fits window → '10'
		-8000.5, // sign+exponent change → wider window
		-8000.5,
		3e300, // extreme exponent
		1.5,
	}
	roundTripValues(t, vals)
}
