package main

// Query: what does a dashboard panel cost? The store is populated the
// way the gateway would populate it (counter and gauge series at
// realistic cardinality), half the data is flushed into segment files so
// every query pays the head/segment merge real queries pay, and then the
// dashboard's own three question shapes run through the engine at a
// fixed rate.

import (
	"fmt"
	"os"
	"time"

	"github.com/rvsiyad/scope/internal/bench"
	"github.com/rvsiyad/scope/internal/query"
	"github.com/rvsiyad/scope/internal/tsdb"
)

const (
	queryTenants = 20
	queryModels  = 5 // 100 counter + 100 gauge series
	sampleEvery  = 5 * time.Second
	historyLen   = 30 * time.Minute
)

func benchQuery(rate float64, d time.Duration) error {
	fmt.Printf("== query: %.0f queries/s per shape, %v measured ==\n", rate, d)
	dir := tempDir("scope-bench-query-")
	defer os.RemoveAll(dir)
	db, err := tsdb.Open(dir)
	if err != nil {
		return err
	}

	// 30 minutes of history at 5s resolution, 200 series, with a Flush
	// halfway: the first 15 minutes live in an immutable segment, the
	// rest in the head — the boundary the engine's reads must merge.
	end := time.Now().UnixMilli()
	start := end - historyLen.Milliseconds()
	steps := int(historyLen / sampleEvery)
	var counter float64
	for i := 0; i < steps; i++ {
		t := start + int64(i)*sampleEvery.Milliseconds()
		for tn := 0; tn < queryTenants; tn++ {
			for m := 0; m < queryModels; m++ {
				labels := map[string]string{
					"tenant": fmt.Sprintf("t%02d", tn),
					"model":  fmt.Sprintf("m%d", m),
				}
				counter += float64(tn + m + 1)
				if err := db.Append(tsdb.NewLabels("bench_tokens_total", labels), t, counter); err != nil {
					return err
				}
				if err := db.Append(tsdb.NewLabels("bench_ttft_ms", labels), t,
					float64(50+(i*7+tn*13+m*29)%200)); err != nil {
					return err
				}
			}
		}
		if i == steps/2 {
			if err := db.Flush(); err != nil {
				return err
			}
		}
	}

	engine := query.New(db)
	shapes := []struct {
		name string
		expr string
		run  func(e query.Expr) error
	}{
		{
			// The dashboard's range question: 15 minutes at 15s steps.
			name: "range: sum by(tenant) rate 15m/15s",
			expr: `sum by (tenant) (rate(bench_tokens_total[1m]))`,
			run: func(e query.Expr) error {
				_, err := engine.Range(e, query.AlignStart(end-15*60*1000, 15000), end, 15000)
				return err
			},
		},
		{
			name: "range: p99 quantile_over_time 15m/15s",
			expr: `quantile_over_time(0.99, bench_ttft_ms[1m])`,
			run: func(e query.Expr) error {
				_, err := engine.Range(e, query.AlignStart(end-15*60*1000, 15000), end, 15000)
				return err
			},
		},
		{
			name: "instant: increase 5m, one tenant",
			expr: `increase(bench_tokens_total{tenant="t07"}[5m])`,
			run: func(e query.Expr) error {
				_, err := engine.Instant(e, end)
				return err
			},
		},
	}

	for _, s := range shapes {
		expr, err := query.Parse(s.expr)
		if err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		var failed error
		fn := func() {
			if err := s.run(expr); err != nil && failed == nil {
				failed = err
			}
		}
		bench.Run(rate, time.Second, 16, fn) // warm-up
		h := bench.Run(rate, d, 16, fn)
		if failed != nil {
			return fmt.Errorf("%s: %w", s.name, failed)
		}
		fmt.Printf("%-38s %s\n", s.name, h.Summary())
	}
	return nil
}
