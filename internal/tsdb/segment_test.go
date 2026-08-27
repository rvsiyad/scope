package tsdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func fillHead(t *testing.T) *Head {
	t.Helper()
	h := NewHead()
	for _, tenant := range []string{"acme", "globex"} {
		ls := NewLabels("gateway_requests_total", map[string]string{"tenant": tenant})
		for i := int64(1); i <= 50; i++ {
			if err := h.Append(ls, i*1000, float64(i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	ttft := NewLabels("gateway_ttft_ms", map[string]string{"tenant": "acme"})
	for _, s := range []Sample{{1500, 812.5}, {2500, math.NaN()}, {3500, math.Inf(1)}} {
		if err := h.Append(ttft, s.T, s.V); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

func TestFlushOpenRoundTrip(t *testing.T) {
	h := fillHead(t)
	before := h.Select(nil, 0, math.MaxInt64)
	path := filepath.Join(t.TempDir(), "0001.seg")

	n, err := h.Flush(path)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("flushed %d series, want 3", n)
	}
	// The flush must reset the head...
	if h.NumSeries() != 0 || h.NumSamples() != 0 {
		t.Fatalf("head not reset: %d series / %d samples", h.NumSeries(), h.NumSamples())
	}
	// ...and must not leave its scratch file behind.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind: %v", err)
	}

	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := seg.Select(nil, 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	// The segment must return exactly what the head returned before the
	// flush — same series, same order, bit-exact samples.
	if len(after) != len(before) {
		t.Fatalf("segment has %d series, head had %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Labels.Key() != after[i].Labels.Key() {
			t.Fatalf("series %d: key %q, want %q", i, after[i].Labels.Key(), before[i].Labels.Key())
		}
		if len(before[i].Samples) != len(after[i].Samples) {
			t.Fatalf("%s: %d samples, want %d", after[i].Labels.Key(), len(after[i].Samples), len(before[i].Samples))
		}
		for j := range before[i].Samples {
			b, a := before[i].Samples[j], after[i].Samples[j]
			if b.T != a.T || math.Float64bits(b.V) != math.Float64bits(a.V) {
				t.Fatalf("%s sample %d: %v, want %v", after[i].Labels.Key(), j, a, b)
			}
		}
	}
	if seg.NumSeries() != 3 || seg.NumSamples() != 103 {
		t.Fatalf("segment reports %d series / %d samples, want 3 / 103", seg.NumSeries(), seg.NumSamples())
	}
	if seg.MinTime() != 1000 || seg.MaxTime() != 50_000 {
		t.Fatalf("segment bounds [%d, %d], want [1000, 50000]", seg.MinTime(), seg.MaxTime())
	}
}

func TestFlushEmptyHeadWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.seg")
	n, err := NewHead().Flush(path)
	if err != nil || n != 0 {
		t.Fatalf("empty flush: n=%d err=%v, want 0, nil", n, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("empty flush must not create a file")
	}
}

func TestFlushIsDeterministic(t *testing.T) {
	// Two heads fed the same data must produce byte-identical files —
	// series order comes from canonical keys, not map iteration.
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a.seg"), filepath.Join(dir, "b.seg")}
	for _, p := range paths {
		if _, err := fillHead(t).Flush(p); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := os.ReadFile(paths[0])
	b, _ := os.ReadFile(paths[1])
	if !bytes.Equal(a, b) {
		t.Fatal("same head contents produced different segment bytes")
	}
}

func TestHeadUsableAfterFlush(t *testing.T) {
	h := fillHead(t)
	path := filepath.Join(t.TempDir(), "0001.seg")
	if _, err := h.Flush(path); err != nil {
		t.Fatal(err)
	}
	// The reset head has no memory of flushed watermarks: ordering is
	// enforced within a head's lifetime, and cross-flush ordering belongs
	// to the appender (the collector delivers in arrival order). The DB's
	// merge sorts, so a late sample degrades nothing.
	ls := NewLabels("gateway_requests_total", map[string]string{"tenant": "acme"})
	if err := h.Append(ls, 10, 1); err != nil {
		t.Fatalf("append to reset head: %v", err)
	}
	got := h.Select(nil, 0, math.MaxInt64)
	if len(got) != 1 || len(got[0].Samples) != 1 {
		t.Fatalf("post-flush head state: %v", got)
	}
}

func TestSegmentSelectWindowAndMatchers(t *testing.T) {
	h := fillHead(t)
	path := filepath.Join(t.TempDir(), "0001.seg")
	if _, err := h.Flush(path); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}

	got, err := seg.Select([]Matcher{Eq("tenant", "acme")}, 2000, 3000)
	if err != nil {
		t.Fatal(err)
	}
	// acme has two series; ttft's only in-window sample is the NaN at 2500.
	want := map[string][]int64{
		`gateway_requests_total{tenant="acme"}`: {2000, 3000},
		`gateway_ttft_ms{tenant="acme"}`:        {2500},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series, want %d", len(got), len(want))
	}
	for _, s := range got {
		var ts []int64
		for _, smp := range s.Samples {
			ts = append(ts, smp.T)
		}
		if !reflect.DeepEqual(ts, want[s.Labels.Key()]) {
			t.Errorf("%s: timestamps %v, want %v", s.Labels.Key(), ts, want[s.Labels.Key()])
		}
	}

	// A window past the data selects nothing.
	if out, err := seg.Select(nil, 60_000, 70_000); err != nil || len(out) != 0 {
		t.Fatalf("out-of-window select: %v, %v", out, err)
	}
}

func TestOpenSegmentRejectsCorruption(t *testing.T) {
	h := fillHead(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "0001.seg")
	if _, err := h.Flush(path); err != nil {
		t.Fatal(err)
	}
	pristine, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"bad magic", func(b []byte) []byte { b[0] ^= 0xff; return b }},
		{"flipped payload byte", func(b []byte) []byte { b[len(b)/2] ^= 0x01; return b }},
		{"truncated mid-record", func(b []byte) []byte { return b[:len(b)-5] }},
		{"truncated header", func(b []byte) []byte { return b[:len(segMagic)+3] }},
	}
	for _, tc := range cases {
		mutated := tc.mutate(append([]byte(nil), pristine...))
		p := filepath.Join(dir, tc.name+".seg")
		if err := os.WriteFile(p, mutated, 0o644); err != nil {
			t.Fatal(err)
		}
		// Segments never recover partially — any bad byte fails the open.
		if _, err := OpenSegment(p); err == nil {
			t.Errorf("%s: OpenSegment accepted a corrupt file", tc.name)
		}
	}

	// The pristine bytes still open, proving the harness itself is sound.
	if _, err := OpenSegment(path); err != nil {
		t.Fatalf("pristine segment failed to open: %v", err)
	}
}

func TestSegmentSelectSurfacesUndecodableBlock(t *testing.T) {
	// Hand-build a structurally valid record (good CRC, good meta) whose
	// block cannot supply the promised sample count: Open succeeds, and
	// the lie surfaces as a Select error, never as silent partial data.
	meta, _ := json.Marshal(segMeta{
		Labels:  NewLabels("m", nil),
		Samples: 5,
		MinT:    0,
		MaxT:    1000,
	})
	payload := make([]byte, 4+len(meta)) // meta only, zero block bytes
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(meta)))
	copy(payload[4:], meta)
	var buf bytes.Buffer
	buf.WriteString(segMagic)
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(payload, castagnoli))
	buf.Write(header[:])
	buf.Write(payload)

	path := filepath.Join(t.TempDir(), "lying.seg")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("structurally valid segment must open: %v", err)
	}
	if _, err := seg.Select(nil, 0, math.MaxInt64); err == nil {
		t.Fatal("Select returned no error for an undecodable block")
	} else if !strings.Contains(err.Error(), "m{}") {
		t.Fatalf("error should name the series: %v", err)
	}
}
