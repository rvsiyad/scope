// Package collector is the ingest side of the observability backend: it
// enforces the wire contract (POST /v1/ingest carrying a telemetry.Batch),
// validates at the door, makes each accepted batch durable in a write-ahead
// log, and hands batches off to the stores. The ack contract is the point:
// a 204 means the batch is in the WAL — under the "always" sync policy,
// fsync'd — so acknowledged telemetry survives a crash and is replayed
// into the stores on restart. Metrics land in the tsdb engine (tsdb.go);
// spans land in the trace store (traces.go) through the same Consumer
// socket.
package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/tracestore"
	"github.com/rvsiyad/scope/internal/tsdb"
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

	// TSDBDir enables the metrics store: accepted MetricPoints flow into a
	// tsdb.DB rooted here (see tsdb.go in this package). Empty disables it.
	TSDBDir string
	// TSDBFlushEvery is the maintenance cadence: each tick flushes the
	// head into a segment and compacts. Zero means no background
	// maintenance (unit tests drive flushes explicitly).
	TSDBFlushEvery time.Duration
	// TSDBRetention drops samples older than this at compaction time.
	// Zero keeps everything.
	TSDBRetention time.Duration
	// TSDBMaxSeries is the cardinality guard's cap (0 = unlimited).
	TSDBMaxSeries int

	// TraceDir enables the trace store: accepted Spans flow into a
	// tracestore.Store rooted here (see traces.go in this package). Empty
	// disables it.
	TraceDir string
	// TraceFlushEvery is the trace store's maintenance cadence: each tick
	// flushes the trace head into a segment and enforces retention. Zero
	// means no background maintenance.
	TraceFlushEvery time.Duration
	// TraceRetention drops whole trace segments older than this at
	// maintenance time. Zero keeps everything.
	TraceRetention time.Duration
	// TraceKeepRatio is the sampling policy: the fraction of traces kept,
	// decided per trace id so traces stay whole. Zero means unset — keep
	// everything (as does any ratio >= 1).
	TraceKeepRatio float64
}

type Server struct {
	mux       *http.ServeMux
	log       *wal.WAL // nil when durability is disabled
	consumers []Consumer
	tsdb      *tsdbStore  // nil when the metrics store is disabled
	traces    *traceStore // nil when the trace store is disabled
	stop      chan struct{}
	done      chan struct{}

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
	s := &Server{mux: http.NewServeMux(), consumers: consumers,
		stop: make(chan struct{}), done: make(chan struct{})}
	// The tsdb store must register BEFORE the WAL replay below runs: the
	// replay feeds the consumer list, and the head block repopulating from
	// the log is exactly the crash-recovery story.
	if cfg.TSDBDir != "" {
		db, err := tsdb.Open(cfg.TSDBDir)
		if err != nil {
			return nil, fmt.Errorf("collector: tsdb: %w", err)
		}
		db.SetMaxSeries(cfg.TSDBMaxSeries)
		s.tsdb = &tsdbStore{db: db}
		s.consumers = append(s.consumers, s.tsdb.consume)
	}
	// Same rule for the trace store: registered before replay so the trace
	// head repopulates from the log.
	if cfg.TraceDir != "" {
		store, err := tracestore.Open(cfg.TraceDir)
		if err != nil {
			return nil, fmt.Errorf("collector: tracestore: %w", err)
		}
		if cfg.TraceKeepRatio > 0 {
			store.SetSampler(tracestore.KeepRatio(cfg.TraceKeepRatio))
		}
		s.traces = &traceStore{store: store}
		s.consumers = append(s.consumers, s.traces.consume)
	}
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
	if s.tsdb != nil {
		s.mux.HandleFunc("GET /debug/tsdb/select", s.tsdb.handleSelect)
	}
	if s.traces != nil {
		s.mux.HandleFunc("GET /v1/traces", s.traces.handleList)
		s.mux.HandleFunc("GET /v1/traces/{id}", s.traces.handleTrace)
	}
	// Each store gets its own maintenance loop; done closes when every loop
	// has stopped (immediately, when none run).
	var maint sync.WaitGroup
	if s.tsdb != nil && cfg.TSDBFlushEvery > 0 {
		maint.Add(1)
		go func() {
			defer maint.Done()
			s.tsdb.maintain(cfg.TSDBFlushEvery, cfg.TSDBRetention, s.stop)
		}()
	}
	if s.traces != nil && cfg.TraceFlushEvery > 0 {
		maint.Add(1)
		go func() {
			defer maint.Done()
			s.traces.maintain(cfg.TraceFlushEvery, cfg.TraceRetention, s.stop)
		}()
	}
	go func() {
		maint.Wait()
		close(s.done)
	}()
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close stops maintenance, flushes the head into a final segment (a
// graceful shutdown should not leave its samples to the next replay), and
// closes the WAL. Crash shutdowns skip all of this by definition — that
// path is covered by the WAL replay in New.
func (s *Server) Close() error {
	close(s.stop)
	<-s.done
	if s.tsdb != nil {
		if err := s.tsdb.db.Flush(); err != nil {
			return err
		}
	}
	if s.traces != nil {
		if err := s.traces.store.Flush(); err != nil {
			return err
		}
	}
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

	WAL       *wal.Status  `json:"wal,omitempty"`
	WALErrors uint64       `json:"wal_errors,omitempty"`
	TSDB      *TSDBStatus  `json:"tsdb,omitempty"`
	Traces    *TraceStatus `json:"traces,omitempty"`
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
	if s.tsdb != nil {
		st.TSDB = s.tsdb.status()
	}
	if s.traces != nil {
		st.Traces = s.traces.status()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}
