package main

import (
	"log"
	"net/http"
	"os"

	"github.com/rvsiyad/scope/internal/collector"
)

func main() {
	addr := os.Getenv("SCOPE_COLLECTOR_ADDR")
	if addr == "" {
		// 9091, not the gateway-adjacent 8091: the exchange stack co-tenant
		// on this dev box owns 8091/8092/8095, and the collector is a
		// metrics backend anyway — the 909x band is its natural home.
		addr = ":9091"
	}
	srv := collector.New()
	log.Printf("collector listening on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatal(err)
	}
}
