package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/rvsiyad/scope/internal/tsdb"
)

// fixture is a Queryable serving hand-built series through the real tsdb
// matcher semantics — a head block is the simplest correct implementation.
func fixture(t *testing.T, series ...tsdb.Series) Queryable {
	t.Helper()
	h := tsdb.NewHead()
	for _, s := range series {
		for _, smp := range s.Samples {
			if err := h.Append(s.Labels, smp.T, smp.V); err != nil {
				t.Fatalf("fixture append: %v", err)
			}
		}
	}
	return headQueryable{h}
}

type headQueryable struct{ h *tsdb.Head }

func (q headQueryable) Select(ms []tsdb.Matcher, mint, maxt int64) ([]tsdb.Series, error) {
	return q.h.Select(ms, mint, maxt), nil
}

func serie(name string, labels map[string]string, samples ...Sample) tsdb.Series {
	return tsdb.Series{Labels: tsdb.NewLabels(name, labels), Samples: samples}
}

func sel(name string, extra ...tsdb.Matcher) VectorSelector {
	return VectorSelector{Matchers: append([]tsdb.Matcher{tsdb.Eq(tsdb.MetricName, name)}, extra...)}
}

func TestInstantTakesNewestSampleWithinLookback(t *testing.T) {
	e := New(fixture(t,
		serie("m", map[string]string{"tenant": "acme"}, Sample{T: 1000, V: 1}, Sample{T: 5000, V: 5}),
		serie("m", map[string]string{"tenant": "globex"}, Sample{T: 2000, V: 2}),
	))
	got, err := e.Instant(sel("m"), 6000)
	if err != nil {
		t.Fatal(err)
	}
	// Both series answer with their newest sample, restamped to the
	// evaluation time.
	want := Matrix{
		{Labels: tsdb.NewLabels("m", map[string]string{"tenant": "acme"}), Samples: []Sample{{T: 6000, V: 5}}},
		{Labels: tsdb.NewLabels("m", map[string]string{"tenant": "globex"}), Samples: []Sample{{T: 6000, V: 2}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestInstantRespectsMatchers(t *testing.T) {
	e := New(fixture(t,
		serie("m", map[string]string{"tenant": "acme"}, Sample{T: 1000, V: 1}),
		serie("m", map[string]string{"tenant": "globex"}, Sample{T: 1000, V: 2}),
		serie("other", map[string]string{"tenant": "acme"}, Sample{T: 1000, V: 3}),
	))
	got, err := e.Instant(sel("m", tsdb.Eq("tenant", "acme")), 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Labels.Get("tenant") != "acme" || got[0].Samples[0].V != 1 {
		t.Fatalf("got %+v, want just acme's m", got)
	}
}

func TestInstantStalenessBeyondLookback(t *testing.T) {
	e := New(fixture(t, serie("m", nil, Sample{T: 1000, V: 1})))
	e.SetLookback(5 * time.Second)
	// The lookback window is [t-lookback, t], closed on the left like
	// Prometheus's: at t=6000 the sample sits exactly on the edge and is
	// kept; one ms later it has aged out.
	got, err := e.Instant(sel("m"), 6000)
	if err != nil || len(got) != 1 {
		t.Fatalf("at the edge: got %+v, %v; want the point", got, err)
	}
	got, err = e.Instant(sel("m"), 6001)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %+v, want staleness (no point)", got)
	}
}

func TestInstantIgnoresFutureSamples(t *testing.T) {
	e := New(fixture(t, serie("m", nil, Sample{T: 1000, V: 1}, Sample{T: 9000, V: 9})))
	got, err := e.Instant(sel("m"), 5000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Samples[0].V != 1 {
		t.Fatalf("got %+v, want only the past sample (v=1)", got)
	}
}

func TestRangeStepAlignmentAndStaleness(t *testing.T) {
	// Samples at 1s, 2s, 12s. Steps every 5s from 0 to 20s, lookback 5s:
	//   t=0     nothing yet                        -> no point
	//   t=5000  newest <=5000 is 2000, in reach    -> 2
	//   t=10000 newest is 2000 < 10000-5000        -> stale
	//   t=15000 newest is 12000, in reach          -> 12
	//   t=20000 12000 < 20000-5000                 -> stale again
	e := New(fixture(t, serie("m", nil,
		Sample{T: 1000, V: 1}, Sample{T: 2000, V: 2}, Sample{T: 12000, V: 12})))
	e.SetLookback(5 * time.Second)
	got, err := e.Range(sel("m"), 0, 20000, 5000)
	if err != nil {
		t.Fatal(err)
	}
	want := []Sample{{T: 5000, V: 2}, {T: 15000, V: 12}}
	if len(got) != 1 || !reflect.DeepEqual(got[0].Samples, want) {
		t.Fatalf("got %+v\nwant one series with %+v", got, want)
	}
}

func TestRangeValidation(t *testing.T) {
	e := New(fixture(t))
	if _, err := e.Range(sel("m"), 0, 1000, 0); err == nil {
		t.Fatal("step 0 must be rejected")
	}
	if _, err := e.Range(sel("m"), 2000, 1000, 100); err == nil {
		t.Fatal("end before start must be rejected")
	}
}

func TestRangeEmptySelectionIsEmptyNotError(t *testing.T) {
	e := New(fixture(t, serie("m", nil, Sample{T: 1000, V: 1})))
	got, err := e.Range(sel("nope"), 0, 10000, 1000)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %+v, %v; want empty result, nil error", got, err)
	}
}

func TestAlignStart(t *testing.T) {
	cases := []struct{ start, step, want int64 }{
		{1234, 1000, 1000},
		{1000, 1000, 1000},
		{999, 1000, 0},
		{1234, 0, 1234}, // degenerate step: passthrough
	}
	for _, c := range cases {
		if got := AlignStart(c.start, c.step); got != c.want {
			t.Fatalf("AlignStart(%d, %d) = %d, want %d", c.start, c.step, got, c.want)
		}
	}
}
