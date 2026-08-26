package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

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

	srv, err := collector.New(collector.Config{WALPath: walPath, SyncPolicy: policy})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()
	log.Printf("collector listening on %s (wal=%s sync=%s)", addr, walPath, envOr("SCOPE_WAL_SYNC", "always"))
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
