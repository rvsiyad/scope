// Package telemetry is the client side of the observability stack: the
// data model spans and metrics travel in, and the emitter the gateway
// calls. It is deliberately provider-shaped, not OpenTelemetry — phase B
// builds the backend that stores exactly these types, and owning the schema
// end to end is the point of the project.
package telemetry

import (
	"crypto/rand"
	"encoding/hex"
)

// Span is one timed phase of one request. A request produces a small tree
// of these (request → auth → reserve → cache lookup → provider → settle),
// stitched by TraceID/ParentID; the trace store renders the tree as a
// waterfall. Everything measured about a phase that isn't a timestamp goes
// in Attrs — including the provider span's TTFT marker and token counts —
// keeping the schema stable while attributes evolve.
type Span struct {
	TraceID  string `json:"trace_id"`
	SpanID   string `json:"span_id"`
	ParentID string `json:"parent_id,omitempty"`
	Name     string `json:"name"`
	// Unix nanoseconds. Nanos because phases like a cache lookup are far
	// shorter than a millisecond, and the waterfall should still show them.
	Start int64             `json:"start"`
	End   int64             `json:"end"`
	Attrs map[string]string `json:"attrs,omitempty"`
}

// MetricPoint is one sample of one series. The series identity is the name
// plus the full label set — the same model Prometheus uses, and what the
// phase-B inverted label index will key on. Labels stay low-cardinality by
// discipline: tenant, model, provider, outcome — never request or trace ids
// (a label per request would mint a series per request and melt the index).
type MetricPoint struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	// Timestamp is unix milliseconds — metric samples don't need nanos, and
	// Gorilla's delta-of-delta encoding (phase B) likes small regular ints.
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// Batch is the wire format of one ingest call: JSON for now, so every step
// is curl-able and debuggable while the system grows around it. The upgrade
// path when JSON becomes the bottleneck is a length-prefixed binary framing
// of the same fields — the schema, not the encoding, is the contract.
type Batch struct {
	Spans   []Span        `json:"spans,omitempty"`
	Metrics []MetricPoint `json:"metrics,omitempty"`
}

// NewTraceID returns a 16-byte hex trace id; NewSpanID an 8-byte one —
// the same widths OpenTelemetry settled on, big enough that collisions are
// not a design concern.
func NewTraceID() string { return randomHex(16) }

func NewSpanID() string { return randomHex(8) }

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
