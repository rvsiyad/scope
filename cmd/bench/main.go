// bench produces the numbers in docs/benchmarks.md: ingest throughput
// and ack latency, query latency, and the gateway's added overhead per
// request. Every benchmark uses the internal/bench harness — open-loop
// arrivals, latency measured from each request's scheduled start — so
// the percentiles include queueing delay instead of hiding it (see the
// pacer's doc comment for the coordinated-omission argument).
//
//	go run ./cmd/bench                 # everything, default settings
//	go run ./cmd/bench -bench ingest   # one suite
//
// Numbers are hardware-bound: the report header states the machine, and
// scripts/bench.sh reproduces the whole document in one command.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

func main() {
	which := flag.String("bench", "all", "ingest | query | overhead | all")
	duration := flag.Duration("duration", 10*time.Second, "measured window per benchmark run (after warm-up)")
	ingestRate := flag.Float64("ingest-rate", 200, "ingest batches/second (100 points per batch)")
	queryRate := flag.Float64("query-rate", 100, "queries/second per query shape")
	overheadRate := flag.Float64("overhead-rate", 150, "chat requests/second")
	flag.Parse()

	fmt.Printf("scope bench — %s/%s, %d CPUs, %s\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Printf("open-loop arrivals; latencies measured from scheduled start; warm-up excluded\n\n")

	run := func(name string, fn func() error) {
		if *which != "all" && *which != name {
			return
		}
		if err := fn(); err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		fmt.Println()
	}
	run("ingest", func() error { return benchIngest(*ingestRate, *duration) })
	run("query", func() error { return benchQuery(*queryRate, *duration) })
	run("overhead", func() error { return benchOverhead(*overheadRate, *duration) })
}

// tempDir hands each benchmark a scratch directory that dies with the run.
func tempDir(pattern string) string {
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		log.Fatal(err)
	}
	return dir
}
