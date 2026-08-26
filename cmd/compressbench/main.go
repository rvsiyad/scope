// compressbench answers the paper's question about our own data: how many
// bytes per sample does Gorilla compression spend on real gateway
// telemetry? It reads a collector WAL, groups the metric samples into
// series (name + sorted labels — the same identity the TSDB will use),
// runs each series through the codec, verifies the round trip
// bit-exactly, and reports bytes/sample against the 16-byte raw cost.
//
//	go run ./cmd/compressbench -wal data/collector.wal
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/rvsiyad/scope/internal/gorilla"
	"github.com/rvsiyad/scope/internal/telemetry"
	"github.com/rvsiyad/scope/internal/wal"
)

type point struct {
	t int64
	v float64
}

func main() {
	walPath := flag.String("wal", "data/collector.wal", "collector WAL to read")
	flag.Parse()

	w, err := wal.Open(wal.Options{Path: *walPath, Policy: wal.SyncNever})
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	series := map[string][]point{}
	if err := w.Replay(func(payload []byte) error {
		var b telemetry.Batch
		if err := json.Unmarshal(payload, &b); err != nil {
			return err
		}
		for _, m := range b.Metrics {
			id := seriesID(m)
			series[id] = append(series[id], point{m.Timestamp, m.Value})
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if len(series) == 0 {
		log.Fatalf("no metric samples in %s — run some traffic through the gateway first", *walPath)
	}

	names := make([]string, 0, len(series))
	for id := range series {
		names = append(names, id)
	}
	sort.Strings(names)

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "series\tsamples\traw B\tgorilla B\tB/sample\tratio")
	var totalSamples, totalBytes int
	for _, id := range names {
		pts := series[id]
		// The store will append in arrival order; sort by time like the
		// head block would before flushing.
		sort.Slice(pts, func(i, j int) bool { return pts[i].t < pts[j].t })

		enc := gorilla.NewEncoder()
		for _, p := range pts {
			enc.Append(p.t, p.v)
		}
		verify(id, enc, pts)

		raw := 16 * len(pts)
		got := len(enc.Bytes())
		totalSamples += len(pts)
		totalBytes += got
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%.2f\t%.1fx\n",
			id, len(pts), raw, got, enc.BytesPerSample(), float64(raw)/float64(got))
	}
	tw.Flush()
	fmt.Printf("\ntotal: %d samples, %d bytes — %.2f bytes/sample vs 16 raw (%.1fx); paper's fleet-wide figure: 1.37\n",
		totalSamples, totalBytes, float64(totalBytes)/float64(totalSamples),
		16*float64(totalSamples)/float64(totalBytes))
}

// verify decodes what was just encoded — a compression number from a codec
// that can't round-trip its input would be worse than no number.
func verify(id string, enc *gorilla.Encoder, pts []point) {
	it := gorilla.NewIterator(enc.Bytes(), enc.Len())
	for i, want := range pts {
		if !it.Next() {
			log.Fatalf("%s: decode ended at %d/%d: %v", id, i, len(pts), it.Err())
		}
		t, v := it.At()
		if t != want.t || math.Float64bits(v) != math.Float64bits(want.v) {
			log.Fatalf("%s: sample %d decoded (%d,%v), want (%d,%v)", id, i, t, v, want.t, want.v)
		}
	}
}

func seriesID(m telemetry.MetricPoint) string {
	keys := make([]string, 0, len(m.Labels))
	for k := range m.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(m.Name)
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%q", k, m.Labels[k])
	}
	sb.WriteByte('}')
	return sb.String()
}
