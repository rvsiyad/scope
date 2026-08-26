// Package collector is the ingest side of the observability backend. Stub
// edition: it enforces the wire contract (POST /v1/ingest carrying a
// telemetry.Batch), validates, counts, and stores nothing. The contract is
// what matters now — the gateway instruments against it today, and phase B
// grows the guts behind it starting with the WAL (S5) without the gateway
// ever noticing.
package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/rvsiyad/scope/internal/telemetry"
)

type Server struct {
	mux *http.ServeMux

	batches atomic.Uint64
	spans   atomic.Uint64
	metrics atomic.Uint64
	invalid atomic.Uint64
}

func New() *Server {
	s := &Server{mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/ingest", s.handleIngest)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleIngest accepts one batch. Validation happens here, at the door,
// because everything behind this point (WAL, stores) trusts its input —
// garbage must be rejected while the sender can still be told 400, not
// discovered later inside a segment file.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var b telemetry.Batch
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		s.invalid.Add(1)
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := validate(b); err != nil {
		s.invalid.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.batches.Add(1)
	s.spans.Add(uint64(len(b.Spans)))
	s.metrics.Add(uint64(len(b.Metrics)))
	w.WriteHeader(http.StatusNoContent)
}

func validate(b telemetry.Batch) error {
	for i, sp := range b.Spans {
		if sp.TraceID == "" || sp.SpanID == "" || sp.Name == "" {
			return fmt.Errorf("span %d: trace_id, span_id and name are required", i)
		}
		if sp.End < sp.Start {
			return fmt.Errorf("span %d (%s): end precedes start", i, sp.Name)
		}
	}
	for i, m := range b.Metrics {
		if m.Name == "" {
			return fmt.Errorf("metric %d: name is required", i)
		}
	}
	return nil
}

// Status is the collector's intake scoreboard, served at /healthz. Until
// the stores exist, this is how a demo proves the gateway's telemetry
// actually arrived.
type Status struct {
	Status  string `json:"status"`
	Batches uint64 `json:"batches_received"`
	Spans   uint64 `json:"spans_received"`
	Metrics uint64 `json:"metrics_received"`
	Invalid uint64 `json:"invalid_rejected"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Status{
		Status:  "ok",
		Batches: s.batches.Load(),
		Spans:   s.spans.Load(),
		Metrics: s.metrics.Load(),
		Invalid: s.invalid.Load(),
	})
}
