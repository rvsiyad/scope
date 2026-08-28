package query

import (
	"fmt"
	"math"
	"sort"

	"github.com/rvsiyad/scope/internal/tsdb"
)

// Aggregations: the "by tenant" in "tokens per second by tenant". Where a
// window function reduces one series across time, an aggregation reduces
// across series at each step — the two compose (sum by (tenant)
// (rate(...))) because both speak the same Matrix shape.

// Aggregate groups its input's series by the By labels at each step and
// combines each group's values with Op (sum, avg, min, max, count). An
// empty By collapses everything into one series with no labels — PromQL's
// bare sum(...).
type Aggregate struct {
	Op   string
	By   []string
	Expr Expr
}

func (Aggregate) exprNode() {}

// aggFunc folds one group's values at one step.
type aggFunc func(vals []float64) float64

var aggFuncs = map[string]aggFunc{
	"sum": func(vals []float64) float64 {
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s
	},
	"avg": func(vals []float64) float64 {
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	},
	"min": func(vals []float64) float64 {
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Min(m, v)
		}
		return m
	},
	"max": func(vals []float64) float64 {
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Max(m, v)
		}
		return m
	},
	"count": func(vals []float64) float64 {
		return float64(len(vals))
	},
}

// evalAggregate evaluates the child once, then regroups: each input
// series maps to an output identity holding only the By labels, and at
// each step every input value present contributes to its group's fold.
// A series missing a step (staleness) is simply absent from that step's
// fold — an avg over three series where one went stale is the avg of two
// values, not two values and a phantom zero.
func (e *Engine) evalAggregate(agg Aggregate, ts []int64) (Matrix, error) {
	fn, ok := aggFuncs[agg.Op]
	if !ok {
		return nil, fmt.Errorf("query: unknown aggregation %q", agg.Op)
	}
	input, err := e.eval(agg.Expr, ts)
	if err != nil {
		return nil, err
	}

	type group struct {
		labels tsdb.Labels
		// vals[i] collects the group's input values at step ts[i].
		vals [][]float64
	}
	groups := map[string]*group{}
	order := []string{}
	for _, s := range input {
		ls := projectLabels(s.Labels, agg.By)
		key := ls.Key()
		g, ok := groups[key]
		if !ok {
			g = &group{labels: ls, vals: make([][]float64, len(ts))}
			groups[key] = g
			order = append(order, key)
		}
		// Two-pointer walk: input samples are stamped with step timestamps
		// by construction, so each matches its slot directly.
		i := 0
		for _, smp := range s.Samples {
			for i < len(ts) && ts[i] < smp.T {
				i++
			}
			if i < len(ts) && ts[i] == smp.T {
				g.vals[i] = append(g.vals[i], smp.V)
			}
		}
	}

	sort.Strings(order)
	out := make(Matrix, 0, len(order))
	for _, key := range order {
		g := groups[key]
		points := make([]Sample, 0, len(ts))
		for i, t := range ts {
			if len(g.vals[i]) == 0 {
				continue
			}
			points = append(points, Sample{T: t, V: fn(g.vals[i])})
		}
		if len(points) > 0 {
			out = append(out, Series{Labels: g.labels, Samples: points})
		}
	}
	return out, nil
}

// projectLabels keeps only the By labels (absent ones stay absent — a
// missing label is not an empty-valued one in a group identity, matching
// the tsdb's own convention). __name__ never survives an aggregation,
// even if named in By: an aggregate is a derived value, not the metric.
func projectLabels(ls tsdb.Labels, by []string) tsdb.Labels {
	out := tsdb.Labels{}
	for _, name := range by {
		if name == tsdb.MetricName {
			continue
		}
		for _, l := range ls {
			if l.Name == name {
				out = append(out, l)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
