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
		addr = ":8091"
	}
	srv := collector.New()
	log.Printf("collector listening on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatal(err)
	}
}
