package main

// Ingest: how fast does telemetry become durable? The full production
// path — HTTP POST → validate → WAL append (fsync per policy) → head
// block — over a real loopback socket, once per sync policy, because the
// policy IS the benchmark: "always" prices the crash-proof ack, "interval"
// prices the relaxed one, and the gap between their numbers is the
// durability/throughput tradeoff the collector's ADR talks about.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rvsiyad/scope/internal/bench"
	"github.com/rvsiyad/scope/internal/collector"
	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/wal"
)

const pointsPerBatch = 100

func benchIngest(rate float64, d time.Duration) error {
	fmt.Printf("== ingest: %.0f batches/s x %d points, %v measured ==\n", rate, pointsPerBatch, d)
	for _, policy := range []struct {
		name string
		p    wal.SyncPolicy
	}{{"always", wal.SyncAlways}, {"interval", wal.SyncInterval}} {
		if err := ingestOnce(policy.name, policy.p, rate, d); err != nil {
			return err
		}
	}
	return nil
}

func ingestOnce(name string, policy wal.SyncPolicy, rate float64, d time.Duration) error {
	dir := tempDir("scope-bench-ingest-")
	defer os.RemoveAll(dir)
	srv, err := collector.New(collector.Config{
		WALPath:    filepath.Join(dir, "collector.wal"),
		SyncPolicy: policy,
		TSDBDir:    filepath.Join(dir, "tsdb"),
	})
	if err != nil {
		return err
	}
	defer srv.Close()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Bodies are marshalled up front so the measured window prices the
	// system under test, not the generator. 100 points spread over 100
	// series (10 tenants x 10 models) at realistic cardinality; the
	// counter advances so Gorilla sees counter-shaped values, not noise.
	total := int(rate*d.Seconds()) + int(rate) // schedule + one second of warm-up
	bodies := make([][]byte, total)
	base := time.Now().UnixMilli()
	var counter float64
	for i := range bodies {
		b := telemetry.Batch{Metrics: make([]telemetry.MetricPoint, pointsPerBatch)}
		for j := range b.Metrics {
			counter++
			b.Metrics[j] = telemetry.MetricPoint{
				Name: "bench_ingest_total",
				Labels: map[string]string{
					"tenant": fmt.Sprintf("t%02d", j/10),
					"model":  fmt.Sprintf("m%d", j%10),
				},
				Timestamp: base + int64(i),
				Value:     counter,
			}
		}
		bodies[i], _ = json.Marshal(b)
	}

	client := ts.Client()
	client.Transport = &http.Transport{MaxIdleConnsPerHost: 64}
	var next atomic.Int64
	var failed atomic.Int64
	send := func() {
		i := next.Add(1) - 1
		resp, err := client.Post(ts.URL+"/v1/ingest", "application/json",
			bytes.NewReader(bodies[i%int64(len(bodies))]))
		if err != nil {
			failed.Add(1)
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			failed.Add(1)
		}
	}

	// One second of warm-up outside the measured window: connection pool,
	// series registry, and file growth all pay their setup cost here.
	bench.Run(rate, time.Second, 32, send)

	start := time.Now()
	h := bench.Run(rate, d, 32, send)
	elapsed := time.Since(start)

	if n := failed.Load(); n > 0 {
		return fmt.Errorf("%d ingest requests failed", n)
	}
	fmt.Printf("%-9s %8.0f points/s sustained | ack %s\n",
		name, float64(h.Count())*pointsPerBatch/elapsed.Seconds(), h.Summary())
	return nil
}
