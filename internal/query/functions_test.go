package query

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/rvsiyad/scope/internal/tsdb"
)

func call(fn string, name string, r time.Duration, matchers ...tsdb.Matcher) Call {
	return Call{Func: fn, Arg: MatrixSelector{Sel: sel(name, matchers...), Range: r}}
}

func TestRateHandComputed(t *testing.T) {
	// A counter climbing 10 tokens/s: samples every 2s, +20 each.
	e := New(fixture(t, serie("gateway_tokens_total", map[string]string{"tenant": "acme"},
		Sample{T: 0, V: 0}, Sample{T: 2000, V: 20}, Sample{T: 4000, V: 40},
		Sample{T: 6000, V: 60}, Sample{T: 8000, V: 80})))
	got, err := e.Instant(call("rate", "gateway_tokens_total", 10*time.Second), 8000)
	if err != nil {
		t.Fatal(err)
	}
	// Window (t-10s, t] holds all five samples: growth 80 over 8s = 10/s.
	if len(got) != 1 || got[0].Samples[0].V != 10 {
		t.Fatalf("got %+v, want exactly 10 tokens/s", got)
	}
	// rate() output must not be mistakable for the raw metric: __name__ is
	// dropped, the identifying labels stay.
	if got[0].Labels.Get(tsdb.MetricName) != "" || got[0].Labels.Get("tenant") != "acme" {
		t.Fatalf("labels = %v, want tenant kept and __name__ dropped", got[0].Labels)
	}
}

func TestRateHealsCounterResets(t *testing.T) {
	// The process restarts at t=5s: 10, 20, then the counter is reborn at
	// 5, then 8. Growth = (20-10) + 5 + (8-5) = 18 over 6s = 3/s.
	e := New(fixture(t, serie("c", nil,
		Sample{T: 0, V: 10}, Sample{T: 2000, V: 20},
		Sample{T: 4000, V: 5}, Sample{T: 6000, V: 8})))
	got, err := e.Instant(call("increase", "c", time.Minute), 6000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Samples[0].V != 18 {
		t.Fatalf("increase = %+v, want 18 (resets healed)", got)
	}
	got, err = e.Instant(call("rate", "c", time.Minute), 6000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Samples[0].V != 3 {
		t.Fatalf("rate = %+v, want 3/s", got)
	}
}

func TestRateNeedsTwoSamples(t *testing.T) {
	e := New(fixture(t, serie("c", nil, Sample{T: 1000, V: 50})))
	got, err := e.Instant(call("rate", "c", time.Minute), 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %+v, want no point from a single sample", got)
	}
}

func TestWindowIsLeftOpen(t *testing.T) {
	// Window (t-r, t]: at t=10s with r=5s, the sample AT 5s is excluded,
	// so only 6s..10s (three samples, +10 each) contribute: growth 20.
	e := New(fixture(t, serie("c", nil,
		Sample{T: 5000, V: 0}, Sample{T: 6000, V: 10},
		Sample{T: 8000, V: 20}, Sample{T: 10000, V: 30})))
	got, err := e.Instant(call("increase", "c", 5*time.Second), 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Samples[0].V != 20 {
		t.Fatalf("got %+v, want 20 (boundary sample excluded)", got)
	}
}

func TestOverTimeFamilyHandComputed(t *testing.T) {
	e := New(fixture(t, serie("m", nil,
		Sample{T: 1000, V: 4}, Sample{T: 2000, V: 1},
		Sample{T: 3000, V: 7}, Sample{T: 4000, V: 2})))
	cases := []struct {
		fn   string
		want float64
	}{
		{"avg_over_time", 3.5},
		{"sum_over_time", 14},
		{"min_over_time", 1},
		{"max_over_time", 7},
		{"count_over_time", 4},
	}
	for _, c := range cases {
		got, err := e.Instant(call(c.fn, "m", time.Minute), 5000)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Samples[0].V != c.want {
			t.Fatalf("%s = %+v, want %v", c.fn, got, c.want)
		}
	}
}

func TestQuantileOverTimeInterpolates(t *testing.T) {
	// Values 10, 20, 30, 40 (arrival order scrambled to prove sorting).
	e := New(fixture(t, serie("gateway_ttft_ms", nil,
		Sample{T: 1000, V: 30}, Sample{T: 2000, V: 10},
		Sample{T: 3000, V: 40}, Sample{T: 4000, V: 20})))
	q := func(phi float64) float64 {
		c := call("quantile_over_time", "gateway_ttft_ms", time.Minute)
		c.Param = phi
		got, err := e.Instant(c, 5000)
		if err != nil || len(got) != 1 {
			t.Fatalf("phi=%v: got %+v, %v", phi, got, err)
		}
		return got[0].Samples[0].V
	}
	// Hand-computed linear interpolation over sorted [10 20 30 40]:
	// rank = phi * 3.
	if got := q(0); got != 10 {
		t.Fatalf("p0 = %v, want 10", got)
	}
	if got := q(1); got != 40 {
		t.Fatalf("p100 = %v, want 40", got)
	}
	if got := q(0.5); got != 25 {
		t.Fatalf("p50 = %v, want 25", got)
	}
	if got := q(0.99); math.Abs(got-39.7) > 1e-9 {
		t.Fatalf("p99 = %v, want 39.7", got)
	}
}

func TestQuantileValidation(t *testing.T) {
	e := New(fixture(t, serie("m", nil, Sample{T: 1000, V: 1})))
	for _, phi := range []float64{-0.1, 1.1, math.NaN()} {
		c := call("quantile_over_time", "m", time.Minute)
		c.Param = phi
		if _, err := e.Instant(c, 2000); err == nil {
			t.Fatalf("quantile %v must be rejected", phi)
		}
	}
}

func TestCallValidation(t *testing.T) {
	e := New(fixture(t))
	if _, err := e.Instant(call("no_such_fn", "m", time.Minute), 0); err == nil {
		t.Fatal("unknown function must be rejected")
	}
	if _, err := e.Instant(call("rate", "m", 0), 0); err == nil {
		t.Fatal("zero range must be rejected")
	}
}

func TestRangeQueryWithRateSlidesTheWindow(t *testing.T) {
	// Counter +10/s sampled every second from 0 to 10s. rate over a 4s
	// window evaluated at 4s/8s steps must be 10 everywhere — and per
	// series the evaluation is one forward pass, which this also smokes.
	samples := make([]Sample, 0, 11)
	for i := int64(0); i <= 10; i++ {
		samples = append(samples, Sample{T: i * 1000, V: float64(i * 10)})
	}
	e := New(fixture(t, serie("c", nil, samples...)))
	got, err := e.Range(call("rate", "c", 4*time.Second), 4000, 8000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	want := []Sample{{T: 4000, V: 10}, {T: 8000, V: 10}}
	if len(got) != 1 || !reflect.DeepEqual(got[0].Samples, want) {
		t.Fatalf("got %+v\nwant one series with %+v", got, want)
	}
}
