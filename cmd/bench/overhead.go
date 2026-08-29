package main

// Overhead: what does the gateway itself cost? The honest version of a
// gateway benchmark measures YOUR code, not the provider's GPU, so the
// provider here is a stub that answers instantly and identically for
// both arms: clients hit the stub directly, then hit the gateway (full
// production path — auth, token-budget reservation, cache lookup, router,
// breaker, provider call, settle, telemetry into a real collector), and
// the overhead is the difference between the two distributions.

import (
	"bytes"
	"context"
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
	"github.com/rvsiyad/scope/internal/gateway"
	"github.com/rvsiyad/scope/internal/wal"
)

func benchOverhead(rate float64, d time.Duration) error {
	fmt.Printf("== gateway overhead: %.0f requests/s, %v measured ==\n", rate, d)

	// The stub provider: Ollama's wire shape, zero thinking time. Both
	// arms talk to this same handler over the same loopback stack.
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/version":
			fmt.Fprint(w, `{"version":"bench"}`)
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"model":"bench","message":{"role":"assistant","content":"ok"},`+
				`"done":true,"done_reason":"stop","prompt_eval_count":20,"eval_count":10}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	// A real collector takes the gateway's telemetry: emission is
	// fire-and-forget by design, but "fire" still costs the hot path
	// something, and this benchmark should charge for it.
	dir := tempDir("scope-bench-overhead-")
	defer os.RemoveAll(dir)
	col, err := collector.New(collector.Config{
		WALPath:    filepath.Join(dir, "collector.wal"),
		SyncPolicy: wal.SyncAlways,
		TSDBDir:    filepath.Join(dir, "tsdb"),
		TraceDir:   filepath.Join(dir, "traces"),
	})
	if err != nil {
		return err
	}
	defer col.Close()
	colSrv := httptest.NewServer(col)
	defer colSrv.Close()

	gw := gateway.New(gateway.Config{
		OllamaURLs:   []string{stub.URL},
		CollectorURL: colSrv.URL,
		Tenants: []gateway.TenantConfig{
			{Name: "bench", APIKey: "sk-bench", TokensPerMinute: 100_000_000},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.Start(ctx)
	gwSrv := httptest.NewServer(gw)
	defer gwSrv.Close()

	body, _ := json.Marshal(map[string]any{
		"model":      "bench",
		"messages":   []map[string]string{{"role": "user", "content": "benchmark request"}},
		"max_tokens": 32,
	})
	// The direct arm speaks Ollama's own dialect so the stub does equal
	// JSON work for both arms.
	direct, _ := json.Marshal(map[string]any{
		"model":    "bench",
		"messages": []map[string]string{{"role": "user", "content": "benchmark request"}},
	})

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	var failed atomic.Int64
	post := func(url, auth string, payload []byte) func() {
		return func() {
			req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			resp, err := client.Do(req)
			if err != nil {
				failed.Add(1)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				failed.Add(1)
			}
		}
	}

	arms := []struct {
		name string
		fn   func()
	}{
		{"direct to provider", post(stub.URL+"/api/chat", "", direct)},
		{"through gateway", post(gwSrv.URL+"/v1/chat/completions", "Bearer sk-bench", body)},
	}
	hists := make([]*bench.Hist, len(arms))
	for i, arm := range arms {
		bench.Run(rate, time.Second, 32, arm.fn) // warm-up
		hists[i] = bench.Run(rate, d, 32, arm.fn)
		if n := failed.Load(); n > 0 {
			return fmt.Errorf("%s: %d requests failed", arm.name, n)
		}
		fmt.Printf("%-20s %s\n", arm.name, hists[i].Summary())
	}
	fmt.Printf("added overhead       p50=%v p99=%v\n",
		hists[1].Quantile(0.50)-hists[0].Quantile(0.50),
		hists[1].Quantile(0.99)-hists[0].Quantile(0.99))
	return nil
}
