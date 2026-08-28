package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/rvsiyad/scope/internal/tsdb"
)

func TestSumByGroupsSeries(t *testing.T) {
	// acme runs two models, globex one. sum by (tenant) folds acme's two
	// series and leaves globex's alone; model must vanish from the output.
	e := New(fixture(t,
		serie("m", map[string]string{"tenant": "acme", "model": "a"}, Sample{T: 1000, V: 1}),
		serie("m", map[string]string{"tenant": "acme", "model": "b"}, Sample{T: 1000, V: 2}),
		serie("m", map[string]string{"tenant": "globex", "model": "a"}, Sample{T: 1000, V: 5}),
	))
	got, err := e.Instant(Aggregate{Op: "sum", By: []string{"tenant"}, Expr: sel("m")}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	want := Matrix{
		{Labels: tsdb.Labels{{Name: "tenant", Value: "acme"}}, Samples: []Sample{{T: 2000, V: 3}}},
		{Labels: tsdb.Labels{{Name: "tenant", Value: "globex"}}, Samples: []Sample{{T: 2000, V: 5}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestBareSumCollapsesToOneSeries(t *testing.T) {
	e := New(fixture(t,
		serie("m", map[string]string{"tenant": "acme"}, Sample{T: 1000, V: 1}),
		serie("m", map[string]string{"tenant": "globex"}, Sample{T: 1000, V: 5}),
	))
	got, err := e.Instant(Aggregate{Op: "sum", Expr: sel("m")}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Labels) != 0 || got[0].Samples[0].V != 6 {
		t.Fatalf("got %+v, want one label-less series summing to 6", got)
	}
}

func TestAggregationOps(t *testing.T) {
	e := New(fixture(t,
		serie("m", map[string]string{"s": "a"}, Sample{T: 1000, V: 4}),
		serie("m", map[string]string{"s": "b"}, Sample{T: 1000, V: 1}),
		serie("m", map[string]string{"s": "c"}, Sample{T: 1000, V: 7}),
	))
	cases := []struct {
		op   string
		want float64
	}{
		{"sum", 12}, {"avg", 4}, {"min", 1}, {"max", 7}, {"count", 3},
	}
	for _, c := range cases {
		got, err := e.Instant(Aggregate{Op: c.op, Expr: sel("m")}, 2000)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Samples[0].V != c.want {
			t.Fatalf("%s = %+v, want %v", c.op, got, c.want)
		}
	}
	if _, err := e.Instant(Aggregate{Op: "median", Expr: sel("m")}, 2000); err == nil {
		t.Fatal("unknown aggregation must be rejected")
	}
}

func TestAggregateStalenessPerStep(t *testing.T) {
	// Series a reports at 1s and 11s; series b only at 1s and goes stale.
	// With lookback 5s and steps at 2s and 12s: the avg at 2s sees both
	// (avg 3), at 12s sees only a (avg 10) — no phantom zero for b.
	e := New(fixture(t,
		serie("m", map[string]string{"s": "a"}, Sample{T: 1000, V: 2}, Sample{T: 11000, V: 10}),
		serie("m", map[string]string{"s": "b"}, Sample{T: 1000, V: 4}),
	))
	e.SetLookback(5 * time.Second)
	got, err := e.Range(Aggregate{Op: "avg", Expr: sel("m")}, 2000, 12000, 10000)
	if err != nil {
		t.Fatal(err)
	}
	want := []Sample{{T: 2000, V: 3}, {T: 12000, V: 10}}
	if len(got) != 1 || !reflect.DeepEqual(got[0].Samples, want) {
		t.Fatalf("got %+v\nwant one series with %+v", got, want)
	}
}

func TestAggregateMissingByLabelStaysAbsent(t *testing.T) {
	// One series has no tenant label at all: it groups under an identity
	// WITHOUT the label, separate from any real tenant value.
	e := New(fixture(t,
		serie("m", map[string]string{"tenant": "acme"}, Sample{T: 1000, V: 1}),
		serie("m", nil, Sample{T: 1000, V: 9}),
	))
	got, err := e.Instant(Aggregate{Op: "sum", By: []string{"tenant"}, Expr: sel("m")}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %+v, want the labelless series in its own group", got)
	}
	byKey := map[string]Series{}
	for _, s := range got {
		byKey[s.Labels.Key()] = s
	}
	if s, ok := byKey["{}"]; !ok || len(s.Labels) != 0 || s.Samples[0].V != 9 {
		t.Fatalf("labelless group = %+v, want v=9 with no labels", byKey)
	}
	if s, ok := byKey[`{tenant="acme"}`]; !ok || s.Samples[0].V != 1 {
		t.Fatalf("acme group = %+v", byKey)
	}
}

func TestSumByOverRateComposes(t *testing.T) {
	// The query the README promises: sum by (tenant) (rate(tokens[10s])).
	// acme: two models at +10/s and +20/s; globex: one at +5/s.
	mk := func(perSec float64) []Sample {
		out := make([]Sample, 0, 11)
		for i := int64(0); i <= 10; i++ {
			out = append(out, Sample{T: i * 1000, V: perSec * float64(i)})
		}
		return out
	}
	e := New(fixture(t,
		serie("gateway_tokens_total", map[string]string{"tenant": "acme", "model": "a"}, mk(10)...),
		serie("gateway_tokens_total", map[string]string{"tenant": "acme", "model": "b"}, mk(20)...),
		serie("gateway_tokens_total", map[string]string{"tenant": "globex", "model": "a"}, mk(5)...),
	))
	got, err := e.Instant(Aggregate{
		Op: "sum", By: []string{"tenant"},
		Expr: call("rate", "gateway_tokens_total", 10*time.Second),
	}, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 ||
		got[0].Labels.Get("tenant") != "acme" || got[0].Samples[0].V != 30 ||
		got[1].Labels.Get("tenant") != "globex" || got[1].Samples[0].V != 5 {
		t.Fatalf("got %+v, want acme=30/s and globex=5/s", got)
	}
}
