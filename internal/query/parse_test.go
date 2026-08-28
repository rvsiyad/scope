package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/rvsiyad/scope/internal/tsdb"
)

func TestParseSelector(t *testing.T) {
	cases := []struct {
		in   string
		want Expr
	}{
		{`gateway_tokens_total`, sel("gateway_tokens_total")},
		{`m{}`, sel("m")},
		{`m{tenant="acme"}`, sel("m", tsdb.Eq("tenant", "acme"))},
		{`m{tenant="acme", outcome!="ok"}`, sel("m", tsdb.Eq("tenant", "acme"), tsdb.Neq("outcome", "ok"))},
		// Escapes in label values, and a metric named like an aggregation.
		{`m{path="say \"hi\""}`, sel("m", tsdb.Eq("path", `say "hi"`))},
		{`sum{tenant="acme"}`, sel("sum", tsdb.Eq("tenant", "acme"))},
		{`count`, sel("count")},
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.in, err)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Fatalf("Parse(%q) = %#v\nwant %#v", c.in, got, c.want)
		}
	}
}

func TestParseCalls(t *testing.T) {
	got := MustParse(`rate(gateway_tokens_total{tenant="acme"}[5m])`)
	want := Call{Func: "rate", Arg: MatrixSelector{
		Sel:   sel("gateway_tokens_total", tsdb.Eq("tenant", "acme")),
		Range: 5 * time.Minute,
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}

	got = MustParse(`quantile_over_time(0.99, gateway_ttft_ms[90s])`)
	want = Call{Func: "quantile_over_time", Param: 0.99, Arg: MatrixSelector{
		Sel:   sel("gateway_ttft_ms"),
		Range: 90 * time.Second,
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}

	// Compound durations reassemble across lexer tokens.
	c := MustParse(`increase(m[1h30m])`).(Call)
	if c.Arg.Range != 90*time.Minute {
		t.Fatalf("range = %v, want 1h30m", c.Arg.Range)
	}
}

func TestParseAggregates(t *testing.T) {
	got := MustParse(`sum by (tenant) (rate(gateway_tokens_total[5m]))`)
	want := Aggregate{Op: "sum", By: []string{"tenant"},
		Expr: Call{Func: "rate", Arg: MatrixSelector{
			Sel: sel("gateway_tokens_total"), Range: 5 * time.Minute}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}

	got = MustParse(`avg by (tenant, model) (m)`)
	if agg := got.(Aggregate); !reflect.DeepEqual(agg.By, []string{"tenant", "model"}) {
		t.Fatalf("by = %v", agg.By)
	}

	// Bare aggregation, and nesting aggregate-in-aggregate.
	got = MustParse(`max(sum by (tenant) (m))`)
	outer := got.(Aggregate)
	if outer.Op != "max" || outer.By != nil {
		t.Fatalf("outer = %#v", outer)
	}
	if inner := outer.Expr.(Aggregate); inner.Op != "sum" || inner.By[0] != "tenant" {
		t.Fatalf("inner = %#v", inner)
	}
}

func TestParseEvaluatesEndToEnd(t *testing.T) {
	// The README's promised query, from string to numbers.
	samples := make([]Sample, 0, 11)
	for i := int64(0); i <= 10; i++ {
		samples = append(samples, Sample{T: i * 1000, V: float64(i * 10)})
	}
	e := New(fixture(t,
		tsdb.Series{Labels: tsdb.NewLabels("gateway_tokens_total", map[string]string{"tenant": "acme"}), Samples: samples}))
	expr := MustParse(`rate(gateway_tokens_total{tenant="acme"}[10s])`)
	got, err := e.Instant(expr, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Samples[0].V != 10 {
		t.Fatalf("got %+v, want 10 tokens/s", got)
	}
}

func TestParseErrors(t *testing.T) {
	cases := []string{
		``,
		`{tenant="acme"}`,             // matchers need a metric name here
		`m{tenant=acme}`,              // unquoted value
		`m{tenant="acme"`,             // unclosed brace
		`m{tenant="acme}`,             // unclosed string
		`rate(m)`,                     // window function needs a range
		`rate(m[5m]`,                  // unclosed call
		`rate(m[abc])`,                // not a duration
		`rate(m[0s])`,                 // non-positive range
		`rate(0.5, m[5m])`,            // rate takes no parameter
		`quantile_over_time(m[5m])`,   // quantile needs its parameter
		`sum by tenant (m)`,           // by-list needs parens
		`sum by (tenant) m`,           // aggregated expr needs parens
		`median by (tenant) (m)`,      // unknown aggregation op reads as a selector...
		`m{tenant="acme"} extra`,      // trailing garbage
		`rate(m[5m]) and rate(n[5m])`, // no binary operators in the grammar
	}
	for _, in := range cases {
		if got, err := Parse(in); err == nil {
			t.Fatalf("Parse(%q) accepted as %#v, want error", in, got)
		}
	}
}

func TestParsedUnknownFunctionStillFailsSomewhere(t *testing.T) {
	// `median by (...) (...)` lexes as a selector named median followed by
	// garbage — a parse error. A bare `median(m)` parses nothing sensible
	// either. Belt and braces: nothing unknown slips through to eval.
	if _, err := Parse(`median(m)`); err == nil {
		t.Fatal("median(m) should not parse")
	}
}
