package wal

import (
	"path/filepath"
	"testing"
	"time"
)

// The durability/throughput tradeoff, measured: how many appends per second
// each sync policy sustains with a realistic ~1 KiB payload (about the size
// of one gateway batch of a few spans). Run with:
//
//	go test -bench=Append -benchtime=2s ./internal/wal/
//
// Expect orders of magnitude between them: SyncAlways pays a device flush
// per record, SyncNever pays only a memcpy into the page cache, and
// SyncInterval buys nearly SyncNever's throughput for a bounded (one-tick)
// power-loss window. That gap is the whole reason ack policies exist.
func BenchmarkAppend(b *testing.B) {
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	policies := []struct {
		name   string
		policy SyncPolicy
	}{
		{"sync-always", SyncAlways},
		{"sync-interval", SyncInterval},
		{"sync-never", SyncNever},
	}
	for _, p := range policies {
		b.Run(p.name, func(b *testing.B) {
			w, err := Open(Options{
				Path:     filepath.Join(b.TempDir(), "bench.wal"),
				Policy:   p.policy,
				Interval: 100 * time.Millisecond,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			b.SetBytes(int64(len(payload) + headerSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Append(payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "appends/s")
		})
	}
}
