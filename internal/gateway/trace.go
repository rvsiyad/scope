package gateway

import (
	"strconv"
	"time"

	"github.com/rvsiyad/scope/internal/telemetry"
)

// Instrumentation of the chat path. One request produces one reqTrace: a
// root "request" span, a child span per phase (auth, cache lookup, reserve,
// provider, settle), and the request-level metric samples. Spans and
// metrics accumulate locally and are handed to the emitter once, at
// finish() — a request's telemetry should arrive whole, not interleaved
// with other requests' phases.
//
// A reqTrace lives on one request's goroutine start to finish, so there are
// no locks; and every method is nil-receiver safe, so a gateway with no
// collector configured (nil emitter → nil trace) pays a pointer check and
// nothing else.

type reqTrace struct {
	em       *telemetry.Emitter
	counters *counterSet
	traceID  string
	rootID   string
	started  time.Time
	attrs    map[string]string
	spans    []telemetry.Span
	metrics  []telemetry.MetricPoint
}

func (s *Server) startTrace() *reqTrace {
	if s.emitter == nil {
		return nil
	}
	return &reqTrace{
		em:       s.emitter,
		counters: s.counters,
		traceID:  telemetry.NewTraceID(),
		rootID:   telemetry.NewSpanID(),
		started:  time.Now(),
		attrs:    map[string]string{},
	}
}

// span starts timing one phase; the returned func ends it. Phase spans are
// all direct children of the root — the chat path is a straight pipeline,
// not a nested one.
func (t *reqTrace) span(name string) func(attrs map[string]string) {
	if t == nil {
		return func(map[string]string) {}
	}
	start := time.Now()
	return func(attrs map[string]string) {
		t.spans = append(t.spans, telemetry.Span{
			TraceID:  t.traceID,
			SpanID:   telemetry.NewSpanID(),
			ParentID: t.rootID,
			Name:     name,
			Start:    start.UnixNano(),
			End:      time.Now().UnixNano(),
			Attrs:    attrs,
		})
	}
}

// setAttr annotates the root span. Attributes accumulate as the request
// reveals them (tenant after auth, outcome at the end).
func (t *reqTrace) setAttr(k, v string) {
	if t == nil {
		return
	}
	t.attrs[k] = v
}

// metric queues one gauge-shaped sample — the measured value itself
// (a duration, a TTFT), stamped now — carrying the trace's tenant/model
// labels plus any extras. Labels stay low-cardinality: never ids.
func (t *reqTrace) metric(name string, value float64, extra map[string]string) {
	if t == nil {
		return
	}
	t.metrics = append(t.metrics, telemetry.MetricPoint{
		Name:      name,
		Labels:    t.metricLabels(extra),
		Timestamp: time.Now().UnixMilli(),
		Value:     value,
	})
}

// counter queues one counter-shaped sample: the series' new CUMULATIVE
// total after this request's contribution, not the contribution itself —
// the shape rate() and increase() expect from a _total series (see
// counters.go for why, and for the restart-resets-to-zero contract).
func (t *reqTrace) counter(name string, delta float64, extra map[string]string) {
	if t == nil {
		return
	}
	labels := t.metricLabels(extra)
	total, ts := t.counters.add(name, labels, delta)
	t.metrics = append(t.metrics, telemetry.MetricPoint{
		Name:      name,
		Labels:    labels,
		Timestamp: ts,
		Value:     total,
	})
}

func (t *reqTrace) metricLabels(extra map[string]string) map[string]string {
	labels := map[string]string{}
	for _, k := range []string{"tenant", "model"} {
		if v, ok := t.attrs[k]; ok {
			labels[k] = v
		}
	}
	for k, v := range extra {
		labels[k] = v
	}
	return labels
}

// charge records what the request actually cost once it is known: the
// settled token count, and dollars at the configured per-million price
// ($0 by default — Ollama is free; a paid provider or the demo sets it).
func (t *reqTrace) charge(tokens int, pricePerMTokens float64) {
	if t == nil {
		return
	}
	cost := float64(tokens) * pricePerMTokens / 1e6
	t.setAttr("tokens_total", strconv.Itoa(tokens))
	t.setAttr("cost_usd", strconv.FormatFloat(cost, 'f', -1, 64))
	t.counter("gateway_tokens_total", float64(tokens), nil)
	t.counter("gateway_cost_usd", cost, nil)
}

// finish closes the root span and hands the whole trace to the emitter,
// along with the two metrics every request produces: a count (labelled by
// outcome and cache disposition, so rates slice by both) and a duration.
// Deferred at the top of the handler, so every exit path emits.
func (t *reqTrace) finish() {
	if t == nil {
		return
	}
	end := time.Now()
	t.em.RecordSpan(telemetry.Span{
		TraceID: t.traceID,
		SpanID:  t.rootID,
		Name:    "request",
		Start:   t.started.UnixNano(),
		End:     end.UnixNano(),
		Attrs:   t.attrs,
	})
	for _, sp := range t.spans {
		t.em.RecordSpan(sp)
	}

	outcome := map[string]string{}
	for _, k := range []string{"outcome", "cache"} {
		if v, ok := t.attrs[k]; ok {
			outcome[k] = v
		}
	}
	t.counter("gateway_requests_total", 1, outcome)
	t.metric("gateway_request_duration_ms", float64(end.Sub(t.started))/float64(time.Millisecond), nil)
	for _, m := range t.metrics {
		t.em.RecordMetric(m)
	}
}

func msString(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 1, 64)
}
