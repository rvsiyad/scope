// Package query is the PromQL-lite layer: the thing that turns the
// storage engine into a database. The tsdb can already answer "give me
// series matching these labels in this window" — this package answers the
// questions dashboards actually ask ("tokens per second by tenant", "TTFT
// p99 over the last 5 minutes") by evaluating a small expression language
// over that Select surface: selectors with a lookback, range windows
// aligned to steps, window functions, and label aggregations.
//
// The semantics follow Prometheus wherever a dashboard would notice
// (lookback, left-open range windows, counter-reset handling) and
// deliberately simplify where it wouldn't; every divergence is documented
// where it lives. Correctness here is fixture-tested by hand-computed
// numbers, not by resemblance to Prometheus output.
package query

import (
	"fmt"
	"time"

	"github.com/rvsiyad/scope/internal/tsdb"
)

// DefaultLookback is how far back an instant evaluation reaches for the
// newest sample of each series — Prometheus's 5 minutes. Without a
// lookback an instant query would only ever hit samples stamped exactly
// at the evaluation time, which for scraped-and-batched telemetry means
// never.
const DefaultLookback = 5 * time.Minute

// A Queryable answers raw series selections; *tsdb.DB is the production
// implementation, and tests substitute fixtures.
type Queryable interface {
	Select(matchers []tsdb.Matcher, mint, maxt int64) ([]tsdb.Series, error)
}

// Engine evaluates expressions over one Queryable. Safe for concurrent
// use — it holds no per-query state.
type Engine struct {
	q Queryable
	// lookback is the instant-evaluation reach, in milliseconds (the
	// engine's time unit, inherited from the tsdb).
	lookback int64
}

// New builds an engine with the default lookback.
func New(q Queryable) *Engine {
	return &Engine{q: q, lookback: DefaultLookback.Milliseconds()}
}

// SetLookback overrides the instant-evaluation reach (tests mostly).
func (e *Engine) SetLookback(d time.Duration) { e.lookback = d.Milliseconds() }

// An Expr is one evaluable expression tree node. The set is closed on
// purpose: selectors, range selectors (only as function arguments),
// window-function calls, and label aggregations — the vocabulary the
// dashboards in phase C actually speak.
type Expr interface {
	// exprNode is a marker; evaluation dispatches on concrete type.
	exprNode()
}

// VectorSelector selects series by matchers and evaluates, at each step,
// to each series' newest sample within the lookback window [t-lookback,
// t] (closed on both ends, as Prometheus's is). A series with no sample in
// reach contributes no point at that step — absence, not zero, exactly as
// Prometheus treats staleness.
type VectorSelector struct {
	Matchers []tsdb.Matcher
}

// MatrixSelector is a range selector, e.g. m{...}[5m]: a VectorSelector
// plus a window. It never evaluates on its own — it exists as the argument
// of a window function, which is also the only place PromQL allows one.
type MatrixSelector struct {
	Sel   VectorSelector
	Range time.Duration
}

func (VectorSelector) exprNode() {}

// Sample is one evaluated point of one series at one step.
type Sample = tsdb.Sample

// Series is one series of an evaluation result: its identity and one
// point per step that produced a value (steps may be skipped — staleness).
type Series = tsdb.Series

// Matrix is a full evaluation result: series sorted by canonical key.
type Matrix = []Series

// Instant evaluates the expression at one timestamp (unix ms) and returns
// at most one point per series, stamped t.
func (e *Engine) Instant(expr Expr, t int64) (Matrix, error) {
	return e.eval(expr, []int64{t})
}

// Range evaluates the expression at every step timestamp start, start+
// step, ..., up to and including end (unix ms; step > 0). Callers wanting
// grid-stable dashboards align start first — see AlignStart.
func (e *Engine) Range(expr Expr, start, end, step int64) (Matrix, error) {
	if step <= 0 {
		return nil, fmt.Errorf("query: step must be positive, got %d", step)
	}
	if end < start {
		return nil, fmt.Errorf("query: range end %d precedes start %d", end, start)
	}
	ts := make([]int64, 0, (end-start)/step+1)
	for t := start; t <= end; t += step {
		ts = append(ts, t)
	}
	return e.eval(expr, ts)
}

// AlignStart floors start onto the step grid (multiples of step since the
// epoch). Two dashboard refreshes a second apart then evaluate at the
// same timestamps and see the same buckets, instead of every refresh
// re-bucketing history — the reason Prometheus dashboards align too.
func AlignStart(start, step int64) int64 {
	if step <= 0 {
		return start
	}
	return start - start%step
}

// eval computes the expression at each timestamp in ts (ascending). One
// storage Select happens per selector for the whole evaluation — the
// per-step work is a walk over the selected samples, never another trip
// to the store.
func (e *Engine) eval(expr Expr, ts []int64) (Matrix, error) {
	if len(ts) == 0 {
		return nil, nil
	}
	switch n := expr.(type) {
	case VectorSelector:
		return e.evalVectorSelector(n, ts)
	case Call:
		return e.evalCall(n, ts)
	default:
		return nil, fmt.Errorf("query: unknown expression node %T", expr)
	}
}

// evalVectorSelector: one Select spanning every step's lookback reach,
// then per series a two-pointer walk — for each step, advance to the
// newest sample at or before it, and take it if it is inside the lookback
// window.
func (e *Engine) evalVectorSelector(sel VectorSelector, ts []int64) (Matrix, error) {
	first, last := ts[0], ts[len(ts)-1]
	series, err := e.q.Select(sel.Matchers, first-e.lookback, last)
	if err != nil {
		return nil, err
	}
	var out Matrix
	for _, s := range series {
		points := make([]Sample, 0, len(ts))
		i := -1 // index of newest sample <= current step
		for _, t := range ts {
			for i+1 < len(s.Samples) && s.Samples[i+1].T <= t {
				i++
			}
			if i >= 0 && s.Samples[i].T >= t-e.lookback {
				points = append(points, Sample{T: t, V: s.Samples[i].V})
			}
		}
		if len(points) > 0 {
			out = append(out, Series{Labels: s.Labels, Samples: points})
		}
	}
	return out, nil
}
