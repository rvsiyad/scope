package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rvsiyad/scope/internal/collector"
	"github.com/rvsiyad/scope/internal/wal"
)

func main() {
	addr := envOr("SCOPE_COLLECTOR_ADDR", ":9091")
	// 9091, not the gateway-adjacent 8091: the exchange stack co-tenant
	// on this dev box owns 8091/8092/8095, and the collector is a metrics
	// backend anyway — the 909x band is its natural home.

	walPath := envOr("SCOPE_WAL_PATH", "data/collector.wal")
	if err := os.MkdirAll(filepath.Dir(walPath), 0o755); err != nil {
		log.Fatalf("SCOPE_WAL_PATH: %v", err)
	}
	// "always" is the default because it is the only ack that survives
	// power loss; the demo and benchmarks can relax it deliberately.
	policy, ok := map[string]wal.SyncPolicy{
		"always":   wal.SyncAlways,
		"interval": wal.SyncInterval,
		"never":    wal.SyncNever,
	}[envOr("SCOPE_WAL_SYNC", "always")]
	if !ok {
		log.Fatalf("SCOPE_WAL_SYNC: want always|interval|never, got %q", os.Getenv("SCOPE_WAL_SYNC"))
	}

	tsdbDir := envOr("SCOPE_TSDB_DIR", "data/tsdb")
	flushEvery, err := time.ParseDuration(envOr("SCOPE_TSDB_FLUSH", "60s"))
	if err != nil {
		log.Fatalf("SCOPE_TSDB_FLUSH: %v", err)
	}
	retention, err := time.ParseDuration(envOr("SCOPE_TSDB_RETENTION", "0s"))
	if err != nil {
		log.Fatalf("SCOPE_TSDB_RETENTION: %v", err)
	}
	maxSeries, err := strconv.Atoi(envOr("SCOPE_TSDB_MAX_SERIES", "10000"))
	if err != nil {
		log.Fatalf("SCOPE_TSDB_MAX_SERIES: %v", err)
	}

	traceDir := envOr("SCOPE_TRACE_DIR", "data/traces")
	traceFlush, err := time.ParseDuration(envOr("SCOPE_TRACE_FLUSH", "60s"))
	if err != nil {
		log.Fatalf("SCOPE_TRACE_FLUSH: %v", err)
	}
	traceRetention, err := time.ParseDuration(envOr("SCOPE_TRACE_RETENTION", "0s"))
	if err != nil {
		log.Fatalf("SCOPE_TRACE_RETENTION: %v", err)
	}
	// 1 = keep every trace; the knob exists so a loaded deployment can dial
	// down without redeploying (the decision hashes the trace id, so any
	// ratio keeps traces whole).
	traceKeep, err := strconv.ParseFloat(envOr("SCOPE_TRACE_KEEP", "1"), 64)
	if err != nil {
		log.Fatalf("SCOPE_TRACE_KEEP: %v", err)
	}

	srv, err := collector.New(collector.Config{
		WALPath:         walPath,
		SyncPolicy:      policy,
		TSDBDir:         tsdbDir,
		TSDBFlushEvery:  flushEvery,
		TSDBRetention:   retention,
		TSDBMaxSeries:   maxSeries,
		TraceDir:        traceDir,
		TraceFlushEvery: traceFlush,
		TraceRetention:  traceRetention,
		TraceKeepRatio:  traceKeep,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()
	log.Printf("collector listening on %s (wal=%s sync=%s tsdb=%s flush=%s traces=%s)",
		addr, walPath, envOr("SCOPE_WAL_SYNC", "always"), tsdbDir, flushEvery, traceDir)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
