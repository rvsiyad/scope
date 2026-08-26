package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/rvsiyad/scope/internal/gateway"
)

func main() {
	// name:api-key:tokens-per-minute, comma-separated; empty = open mode.
	tenants, err := gateway.ParseTenants(os.Getenv("SCOPE_TENANTS"))
	if err != nil {
		log.Fatalf("SCOPE_TENANTS: %v", err)
	}

	// USD per million tokens for telemetry cost attribution; 0 (the
	// default) is honest for local Ollama.
	price, err := strconv.ParseFloat(envOr("SCOPE_PRICE_PER_M_TOKENS", "0"), 64)
	if err != nil || price < 0 {
		log.Fatalf("SCOPE_PRICE_PER_M_TOKENS: %q is not a non-negative number", os.Getenv("SCOPE_PRICE_PER_M_TOKENS"))
	}

	cfg := gateway.Config{
		Addr: envOr("SCOPE_ADDR", ":8090"),
		// Comma-separated failover chain; first entry is the primary.
		OllamaURLs:   strings.Split(envOr("SCOPE_OLLAMA_URLS", "http://localhost:11434"), ","),
		PostgresAddr: envOr("SCOPE_POSTGRES_ADDR", "localhost:5433"),
		Tenants:      tenants,
		// Empty means no telemetry; the collector's default addr is :8091.
		CollectorURL:    os.Getenv("SCOPE_COLLECTOR_URL"),
		PricePerMTokens: price,
	}

	srv := gateway.New(cfg)
	srv.Start(context.Background())
	log.Printf("gateway listening on %s (ollama=%v postgres=%s tenants=%d)", cfg.Addr, cfg.OllamaURLs, cfg.PostgresAddr, len(tenants))
	if err := http.ListenAndServe(cfg.Addr, srv); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
