package tsdb

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestHeadAppendSelectRoundTrip(t *testing.T) {
	h := NewHead()
	ls := NewLabels("gateway_ttft_ms", map[string]string{"tenant": "acme"})
	// Deliberately hostile values: the head must return bit-exact floats,
	// not merely close ones — the codec guarantees it, the head must not
	// lose it.
	want := []Sample{
		{1000, 0},
		{2000, math.Copysign(0, -1)},
		{3000, math.Inf(1)},
		{4000, math.NaN()},
		{5000, 123.456},
	}
	for _, s := range want {
		if err := h.Append(ls, s.T, s.V); err != nil {
			t.Fatalf("append %v: %v", s, err)
		}
	}
	got := h.Select([]Matcher{Eq(MetricName, "gateway_ttft_ms")}, 0, 10_000)
	if len(got) != 1 || len(got[0].Samples) != len(want) {
		t.Fatalf("got %d series / %v, want 1 series with %d samples", len(got), got, len(want))
	}
	for i, s := range got[0].Samples {
		if s.T != want[i].T ||
			math.Float64bits(s.V) != math.Float64bits(want[i].V) {
			t.Fatalf("sample %d = %v (bits %x), want %v (bits %x)",
				i, s, math.Float64bits(s.V), want[i], math.Float64bits(want[i].V))
		}
	}
}

func TestHeadRejectsOutOfOrder(t *testing.T) {
	h := NewHead()
	ls := NewLabels("m", nil)
	for _, tm := range []int64{100, 200} {
		if err := h.Append(ls, tm, 1); err != nil {
			t.Fatal(err)
		}
	}
	// Both strictly-older and duplicate timestamps must be refused.
	for _, tm := range []int64{150, 200} {
		if err := h.Append(ls, tm, 9); err != ErrOutOfOrder {
			t.Fatalf("append t=%d: err = %v, want ErrOutOfOrder", tm, err)
		}
	}
	// The rejection must leave no trace: the stream still works and the
	// refused samples are not in it.
	if err := h.Append(ls, 300, 3); err != nil {
		t.Fatalf("in-order append after rejection: %v", err)
	}
	got := h.Select(nil, 0, 1000)
	want := []Sample{{100, 1}, {200, 1}, {300, 3}}
	if !reflect.DeepEqual(got[0].Samples, want) {
		t.Fatalf("samples = %v, want %v", got[0].Samples, want)
	}
	if h.NumSamples() != 3 {
		t.Fatalf("NumSamples = %d, want 3", h.NumSamples())
	}
}

func TestHeadOutOfOrderPerSeriesNotGlobal(t *testing.T) {
	// Ordering is a per-series contract: interleaved series each have
	// their own clock, and an old timestamp on a NEW series is fine.
	h := NewHead()
	a := NewLabels("m", map[string]string{"s": "a"})
	b := NewLabels("m", map[string]string{"s": "b"})
	if err := h.Append(a, 5000, 1); err != nil {
		t.Fatal(err)
	}
	if err := h.Append(b, 100, 2); err != nil {
		t.Fatalf("older timestamp on a different series must be accepted: %v", err)
	}
	if err := h.Append(a, 5001, 3); err != nil {
		t.Fatal(err)
	}
}

func TestHeadSelectByMatchers(t *testing.T) {
	h := NewHead()
	for _, tenant := range []string{"acme", "globex"} {
		for _, outcome := range []string{"ok", "rejected"} {
			ls := NewLabels("gateway_requests_total",
				map[string]string{"tenant": tenant, "outcome": outcome})
			for i := int64(0); i < 5; i++ {
				if err := h.Append(ls, i*1000, float64(i)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	cases := []struct {
		name     string
		matchers []Matcher
		want     int
	}{
		{"all of metric", []Matcher{Eq(MetricName, "gateway_requests_total")}, 4},
		{"one tenant", []Matcher{Eq("tenant", "acme")}, 2},
		{"tenant+outcome", []Matcher{Eq("tenant", "acme"), Eq("outcome", "ok")}, 1},
		{"neq", []Matcher{Neq("outcome", "rejected")}, 2},
		{"unknown", []Matcher{Eq("tenant", "hooli")}, 0},
	}
	for _, tc := range cases {
		got := h.Select(tc.matchers, 0, 10_000)
		if len(got) != tc.want {
			t.Errorf("%s: %d series, want %d", tc.name, len(got), tc.want)
		}
		// Results are sorted by canonical key — the contract merges and
		// tests downstream rely on.
		for i := 1; i < len(got); i++ {
			if got[i-1].Labels.Key() >= got[i].Labels.Key() {
				t.Errorf("%s: results not sorted by key", tc.name)
			}
		}
	}
}

func TestHeadSelectTimeWindow(t *testing.T) {
	h := NewHead()
	ls := NewLabels("m", nil)
	for i := int64(1); i <= 10; i++ {
		if err := h.Append(ls, i*100, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name       string
		mint, maxt int64
		want       []int64
	}{
		{"interior, bounds inclusive", 300, 500, []int64{300, 400, 500}},
		{"clipped start", 0, 150, []int64{100}},
		{"clipped end", 950, 5000, []int64{1000}},
		{"single point", 400, 400, []int64{400}},
		{"between samples", 410, 490, nil},
		{"before all", 0, 50, nil},
		{"after all", 2000, 3000, nil},
	}
	for _, tc := range cases {
		got := h.Select(nil, tc.mint, tc.maxt)
		if tc.want == nil {
			// No samples in window ⇒ the series itself must be absent,
			// not present-but-empty.
			if len(got) != 0 {
				t.Errorf("%s: got %v, want no series", tc.name, got)
			}
			continue
		}
		if len(got) != 1 {
			t.Fatalf("%s: got %d series, want 1", tc.name, len(got))
		}
		var ts []int64
		for _, s := range got[0].Samples {
			ts = append(ts, s.T)
		}
		if !reflect.DeepEqual(ts, tc.want) {
			t.Errorf("%s: timestamps %v, want %v", tc.name, ts, tc.want)
		}
	}
}

func TestHeadConcurrentAppendAndSelect(t *testing.T) {
	// 8 writers on distinct series while readers query throughout; the
	// race detector referees, and the final state must hold every sample.
	h := NewHead()
	const writers, samples = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ls := NewLabels("m", map[string]string{"w": fmt.Sprint(w)})
			for i := int64(0); i < samples; i++ {
				if err := h.Append(ls, i, float64(i)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			h.Select([]Matcher{Eq(MetricName, "m")}, 0, samples)
		}
	}()
	wg.Wait()
	if got := h.NumSeries(); got != writers {
		t.Fatalf("NumSeries = %d, want %d", got, writers)
	}
	if got := h.NumSamples(); got != writers*samples {
		t.Fatalf("NumSamples = %d, want %d", got, writers*samples)
	}
	for _, s := range h.Select(nil, 0, samples) {
		if len(s.Samples) != samples {
			t.Fatalf("%s has %d samples, want %d", s.Labels.Key(), len(s.Samples), samples)
		}
	}
}

func TestHeadEmpty(t *testing.T) {
	h := NewHead()
	if got := h.Select(nil, 0, 1000); len(got) != 0 {
		t.Fatalf("empty head returned %v", got)
	}
	if h.NumSeries() != 0 || h.NumSamples() != 0 {
		t.Fatal("empty head reports nonzero counts")
	}
}
