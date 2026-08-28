package query

import (
	"fmt"
	"math"
	"sort"

	"github.com/rvsiyad/scope/internal/tsdb"
)

// Window functions: the calls that take a range selector — m{...}[5m] —
// and reduce each series' window of raw samples to one point per step.
// This is where the two calculations every observability engineer must
// actually understand live: rate() over counters (with reset handling)
// and quantiles over raw samples.
//
// Window semantics: at step t with range r, the window is (t-r, t] —
// left-open, exactly Prometheus's matrix selector, so a sample sitting
// on a step boundary belongs to one window, not two adjacent ones.

// Call applies a window function to a range selector, e.g.
// rate(m[5m]) or quantile_over_time(0.99, m[5m]). Param carries the
// quantile φ for quantile_over_time and is ignored by everything else.
type Call struct {
	Func  string
	Param float64
	Arg   MatrixSelector
}

func (Call) exprNode() {}

// windowFunc reduces one window of samples to one value; ok=false means
// the window produces no point (empty, or too few samples for the
// function to mean anything).
type windowFunc func(samples []Sample, param float64) (v float64, ok bool)

var windowFuncs = map[string]windowFunc{
	"rate":               funcRate,
	"increase":           funcIncrease,
	"avg_over_time":      funcAvgOverTime,
	"sum_over_time":      funcSumOverTime,
	"min_over_time":      funcMinOverTime,
	"max_over_time":      funcMaxOverTime,
	"count_over_time":    funcCountOverTime,
	"quantile_over_time": funcQuantileOverTime,
}

// evalCall: one Select spanning every step's window, then per series a
// sliding window over its samples — two indexes that only ever move
// forward, so the whole range evaluation is one pass per series.
func (e *Engine) evalCall(c Call, ts []int64) (Matrix, error) {
	fn, ok := windowFuncs[c.Func]
	if !ok {
		return nil, fmt.Errorf("query: unknown function %q", c.Func)
	}
	if c.Func == "quantile_over_time" && (c.Param < 0 || c.Param > 1 || math.IsNaN(c.Param)) {
		return nil, fmt.Errorf("query: quantile must be in [0, 1], got %v", c.Param)
	}
	r := c.Arg.Range.Milliseconds()
	if r <= 0 {
		return nil, fmt.Errorf("query: range must be positive, got %s", c.Arg.Range)
	}
	first, last := ts[0], ts[len(ts)-1]
	series, err := e.q.Select(c.Arg.Sel.Matchers, first-r+1, last)
	if err != nil {
		return nil, err
	}
	var out Matrix
	for _, s := range series {
		points := make([]Sample, 0, len(ts))
		lo, hi := 0, 0 // s.Samples[lo:hi] is the current window
		for _, t := range ts {
			for hi < len(s.Samples) && s.Samples[hi].T <= t {
				hi++
			}
			for lo < hi && s.Samples[lo].T <= t-r {
				lo++
			}
			if v, ok := fn(s.Samples[lo:hi], c.Param); ok {
				points = append(points, Sample{T: t, V: v})
			}
		}
		if len(points) > 0 {
			out = append(out, Series{Labels: dropName(s.Labels), Samples: points})
		}
	}
	return out, nil
}

// increase computes how much a counter grew across the window, healing
// resets: a sample below its predecessor means the process restarted and
// the counter restarted from ~0, so the predecessor's value is added back
// — the standard Prometheus reset rule. Needs at least two samples; a
// window with one sample has no visible growth.
//
// Divergence, on purpose: Prometheus extrapolates the observed growth out
// to the full window (its rate() famously returns non-integer increases
// for integer counters). Here the observed growth over the observed span
// is returned as-is — hand-computable from the fixtures, and honest about
// only what was seen.
func increase(samples []Sample) (growth, spanMS float64, ok bool) {
	if len(samples) < 2 {
		return 0, 0, false
	}
	total := 0.0
	prev := samples[0].V
	for _, s := range samples[1:] {
		if s.V < prev {
			// Reset: the counter restarted from ~0, so this sample's whole
			// value is growth since the restart.
			total += s.V
		} else {
			total += s.V - prev
		}
		prev = s.V
	}
	return total, float64(samples[len(samples)-1].T - samples[0].T), true
}

func funcIncrease(samples []Sample, _ float64) (float64, bool) {
	growth, _, ok := increase(samples)
	return growth, ok
}

// funcRate is increase divided by the observed span (in seconds): the
// per-second growth of a counter. Dividing by the span between the first
// and last sample — not the nominal window — pairs with the
// no-extrapolation choice above.
func funcRate(samples []Sample, _ float64) (float64, bool) {
	growth, spanMS, ok := increase(samples)
	if !ok || spanMS <= 0 {
		return 0, false
	}
	return growth / (spanMS / 1000), true
}

func funcAvgOverTime(samples []Sample, _ float64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	sum := 0.0
	for _, s := range samples {
		sum += s.V
	}
	return sum / float64(len(samples)), true
}

func funcSumOverTime(samples []Sample, _ float64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	sum := 0.0
	for _, s := range samples {
		sum += s.V
	}
	return sum, true
}

func funcMinOverTime(samples []Sample, _ float64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	m := samples[0].V
	for _, s := range samples[1:] {
		m = math.Min(m, s.V)
	}
	return m, true
}

func funcMaxOverTime(samples []Sample, _ float64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	m := samples[0].V
	for _, s := range samples[1:] {
		m = math.Max(m, s.V)
	}
	return m, true
}

func funcCountOverTime(samples []Sample, _ float64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	return float64(len(samples)), true
}

// funcQuantileOverTime is the percentile calculation the dashboards use
// (TTFT p99 is quantile_over_time(0.99, gateway_ttft_ms[5m])): the
// φ-quantile of the raw samples in the window, with linear interpolation
// between order statistics — Prometheus's method exactly. The gateway
// emits raw duration samples rather than pre-bucketed histograms, so
// quantiles here are exact for what was kept, not bucket-boundary
// estimates; histogram series (and their aggregatability across
// instances) are the documented upgrade when sample volume demands
// pre-aggregation.
func funcQuantileOverTime(samples []Sample, phi float64) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	vals := make([]float64, len(samples))
	for i, s := range samples {
		vals[i] = s.V
	}
	sort.Float64s(vals)
	rank := phi * float64(len(vals)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return vals[lower], true
	}
	weight := rank - float64(lower)
	return vals[lower]*(1-weight) + vals[upper]*weight, true
}

// dropName removes __name__ from a result's labels: the output of
// rate(m[5m]) is a rate, not the metric m, and keeping the name would let
// it be mistaken for raw data — the same reason Prometheus drops it.
func dropName(ls tsdb.Labels) tsdb.Labels {
	out := make(tsdb.Labels, 0, len(ls))
	for _, l := range ls {
		if l.Name != tsdb.MetricName {
			out = append(out, l)
		}
	}
	return out
}
