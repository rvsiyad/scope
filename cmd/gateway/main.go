package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/rvsiyad/scope/internal/gateway"
)

func main() {
	cfg := gateway.Config{
		Addr:         envOr("SCOPE_ADDR", ":8090"),
		OllamaURL:    envOr("SCOPE_OLLAMA_URL", "http://localhost:11434"),
		PostgresAddr: envOr("SCOPE_POSTGRES_ADDR", "localhost:5433"),
	}

	srv := gateway.New(cfg)
	srv.Start(context.Background())
	log.Printf("gateway listening on %s (ollama=%s postgres=%s)", cfg.Addr, cfg.OllamaURL, cfg.PostgresAddr)
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
