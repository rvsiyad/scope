// Package collector is the ingest side of the observability backend: it
// enforces the wire contract (POST /v1/ingest carrying a telemetry.Batch),
// validates at the door, makes each accepted batch durable in a write-ahead
// log, and hands batches off to the stores. The ack contract is the point:
// a 204 means the batch is in the WAL — under the "always" sync policy,
// fsync'd — so acknowledged telemetry survives a crash and is replayed
// into the stores on restart. The stores themselves (tsdb, traces) arrive
// later in phase B; the Consumer interface is their socket.
package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/wal"
)

// maxBodyBytes bounds one ingest call, comfortably under the WAL's record
// cap so a validated body always fits a record.
const maxBodyBytes = 8 << 20

// A Consumer receives every accepted batch: replayed ones during startup
// recovery first, then live ones in arrival order. This is the handoff
// interface the phase-B stores implement — the WAL in front of them is
// what lets consumers keep state only in memory and still lose nothing.
type Consumer func(b telemetry.Batch)

type Config struct {
	// WALPath is the log file backing ingest. Empty disables durability:
	// batches are validated, counted, and handed to consumers, but a 204
	// then only means "received" — the mode unit tests and quick demos use.
	WALPath string
	// SyncPolicy is the WAL's ack dial (see the wal package): "always"
	// means a 204 survives power loss, and is the default in cmd/collector.
	SyncPolicy wal.SyncPolicy
}

type Server struct {
	mux       *http.ServeMux
	log       *wal.WAL // nil when durability is disabled
	consumers []Consumer

	batches atomic.Uint64
	spans   atomic.Uint64
	metrics atomic.Uint64
	invalid atomic.Uint64
	// walErrors counts appends that failed — batches refused with a 503
	// because the durability contract could not be honored.
	walErrors atomic.Uint64
}

// New opens the WAL (if configured), replays every recovered batch into
// the consumers and the counters, and returns a server ready to ingest.
// Replay happens before the first request can arrive, so consumers see
// history strictly before the present.
func New(cfg Config, consumers ...Consumer) (*Server, error) {
	s := &Server{mux: http.NewServeMux(), consumers: consumers}
	if cfg.WALPath != "" {
		w, err := wal.Open(wal.Options{Path: cfg.WALPath, Policy: cfg.SyncPolicy})
		if err != nil {
			return nil, fmt.Errorf("collector: %w", err)
		}
		s.log = w
		if err := w.Replay(func(payload []byte) error {
			var b telemetry.Batch
			if err := json.Unmarshal(payload, &b); err != nil {
				// Every record was validated before it was appended, so
				// this is a bug or foreign file, not a recoverable state.
				return fmt.Errorf("collector: undecodable WAL record: %w", err)
			}
			s.accept(b)
			return nil
		}); err != nil {
			w.Close()
			return nil, err
		}
	}
	s.mux.HandleFunc("POST /v1/ingest", s.handleIngest)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close flushes and closes the WAL.
func (s *Server) Close() error {
	if s.log == nil {
		return nil
	}
	return s.log.Close()
}

// handleIngest accepts one batch: validate → WAL append → ack → consumers.
// Validation happens at the door because everything behind it (the log,
// the stores) trusts its input — garbage gets a 400 while the sender can
// still hear it, not discovered later inside a segment file. The append
// happens before the 204 because the ack IS the durability promise; a
// failed append is a refused batch (503), never a silent maybe.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		s.invalid.Add(1)
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	if len(body) > maxBodyBytes {
		s.invalid.Add(1)
		http.Error(w, fmt.Sprintf("batch exceeds %d bytes", maxBodyBytes), http.StatusRequestEntityTooLarge)
		return
	}
	var b telemetry.Batch
	if err := json.Unmarshal(body, &b); err != nil {
		s.invalid.Add(1)
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := validate(b); err != nil {
		s.invalid.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// The raw wire bytes go into the log, not a re-marshal: what was
	// validated is exactly what recovery will replay.
	if s.log != nil {
		if err := s.log.Append(body); err != nil {
			s.walErrors.Add(1)
			http.Error(w, "durability unavailable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
	s.accept(b)
}

// accept applies one batch to the counters and consumers — the single path
// shared by live ingest and startup replay, which is what makes the
// counters after a crash equal the counters before it.
func (s *Server) accept(b telemetry.Batch) {
	s.batches.Add(1)
	s.spans.Add(uint64(len(b.Spans)))
	s.metrics.Add(uint64(len(b.Metrics)))
	for _, c := range s.consumers {
		c(b)
	}
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

// Status is the collector's intake scoreboard, served at /healthz.
type Status struct {
	Status  string `json:"status"`
	Batches uint64 `json:"batches_received"`
	Spans   uint64 `json:"spans_received"`
	Metrics uint64 `json:"metrics_received"`
	Invalid uint64 `json:"invalid_rejected"`

	WAL       *wal.Status `json:"wal,omitempty"`
	WALErrors uint64      `json:"wal_errors,omitempty"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	st := Status{
		Status:    "ok",
		Batches:   s.batches.Load(),
		Spans:     s.spans.Load(),
		Metrics:   s.metrics.Load(),
		Invalid:   s.invalid.Load(),
		WALErrors: s.walErrors.Load(),
	}
	if s.log != nil {
		ws := s.log.Status()
		st.WAL = &ws
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}
