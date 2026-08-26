package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func tempWAL(t *testing.T, opts Options) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	opts.Path = path
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

func replayAll(t *testing.T, w *WAL) [][]byte {
	t.Helper()
	var out [][]byte
	if err := w.Replay(func(p []byte) error {
		out = append(out, append([]byte(nil), p...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAppendReplayRoundTrip(t *testing.T) {
	w, path := tempWAL(t, Options{Policy: SyncNever})

	rng := rand.New(rand.NewSource(42))
	var want [][]byte
	for i := 0; i < 200; i++ {
		p := make([]byte, rng.Intn(2048)) // includes zero-length records
		rng.Read(p)
		want = append(want, p)
		if err := w.Append(p); err != nil {
			t.Fatal(err)
		}
	}

	// Records must survive in order both live and across a reopen.
	for name, log := range map[string]*WAL{"live": w} {
		got := replayAll(t, log)
		if len(got) != len(want) {
			t.Fatalf("%s: replayed %d records, want %d", name, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("%s: record %d differs", name, i)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Path: path, Policy: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got := replayAll(t, reopened)
	if len(got) != len(want) {
		t.Fatalf("reopen: replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("reopen: record %d differs", i)
		}
	}
}

// The torn-write test the spec asks for: kill a machine mid-append and the
// file ends with a partial record. Recovery must keep every complete
// record, truncate the tear, and leave the log appendable.
func TestRecoveryTruncatesTornTail(t *testing.T) {
	w, path := tempWAL(t, Options{Policy: SyncNever})
	for i := 0; i < 10; i++ {
		if err := w.Append([]byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	goodSize := w.Status().Bytes
	w.Close()

	// Simulate the tear: a header promising 100 bytes, then the crash —
	// only 10 of them ever hit the disk.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	torn := []byte{100, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef}
	torn = append(torn, bytes.Repeat([]byte{'x'}, 10)...)
	if _, err := f.Write(torn); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(Options{Path: path, Policy: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := replayAll(t, r); len(got) != 10 || string(got[9]) != "record-9" {
		t.Fatalf("recovered %d records, want the 10 complete ones", len(got))
	}
	if st := r.Status(); st.Bytes != goodSize {
		t.Fatalf("file is %d bytes after recovery, want truncated to %d", st.Bytes, goodSize)
	}

	// The log must be cleanly appendable after recovery.
	if err := r.Append([]byte("after-recovery")); err != nil {
		t.Fatal(err)
	}
	if got := replayAll(t, r); len(got) != 11 || string(got[10]) != "after-recovery" {
		t.Fatalf("append after recovery: got %d records", len(got))
	}
}

// A CRC mismatch mid-file means real corruption, not a torn tail. The WAL
// guarantees a valid *prefix*: everything before the corruption survives,
// everything after it is untrustworthy and is dropped.
func TestRecoveryStopsAtCorruptRecord(t *testing.T) {
	w, path := tempWAL(t, Options{Policy: SyncNever})
	for i := 0; i < 10; i++ {
		if err := w.Append(bytes.Repeat([]byte{byte(i)}, 100)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Flip one payload byte in the middle record (record 5's payload
	// starts at 5 full records + one header in).
	offset := int64(5*(headerSize+100) + headerSize + 50)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, offset); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(Options{Path: path, Policy: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := replayAll(t, r); len(got) != 5 {
		t.Fatalf("recovered %d records, want the 5 before the corruption", len(got))
	}
}

func TestRecoveryIgnoresGarbageLength(t *testing.T) {
	w, path := tempWAL(t, Options{Policy: SyncNever})
	w.Append([]byte("good"))
	w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	// A "length" of ~4 GiB: recovery must treat it as corruption, not an
	// allocation instruction.
	f.Write([]byte{0xff, 0xff, 0xff, 0xff, 1, 2, 3, 4})
	f.Close()

	r, err := Open(Options{Path: path, Policy: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := replayAll(t, r); len(got) != 1 || string(got[0]) != "good" {
		t.Fatalf("recovered %v, want just the good record", got)
	}
}

// SyncAlways's contract is the ack contract: Append does not return until
// the record is fsync'd.
func TestSyncAlwaysSyncsEveryAppend(t *testing.T) {
	w, _ := tempWAL(t, Options{Policy: SyncAlways})
	var syncs atomic.Int64
	realSync := w.sync
	w.sync = func() error { syncs.Add(1); return realSync() }

	for i := 0; i < 5; i++ {
		if err := w.Append([]byte("r")); err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load(); got != 5 {
		t.Fatalf("syncs = %d, want one per append", got)
	}
}

func TestSyncNeverOnlySyncsOnClose(t *testing.T) {
	w, _ := tempWAL(t, Options{Policy: SyncNever})
	var syncs atomic.Int64
	realSync := w.sync
	w.sync = func() error { syncs.Add(1); return realSync() }

	for i := 0; i < 5; i++ {
		w.Append([]byte("r"))
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("syncs = %d before close, want 0", got)
	}
	w.Close()
	if got := syncs.Load(); got != 1 {
		t.Fatalf("syncs = %d after close, want 1", got)
	}
}

func TestSyncIntervalSyncsInBackground(t *testing.T) {
	w, _ := tempWAL(t, Options{Policy: SyncInterval, Interval: 10 * time.Millisecond})
	var syncs atomic.Int64
	w.mu.Lock()
	realSync := w.sync
	w.sync = func() error { syncs.Add(1); return realSync() }
	w.mu.Unlock()

	w.Append([]byte("r"))
	deadline := time.After(5 * time.Second)
	for syncs.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("background sync never fired")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestConcurrentAppends(t *testing.T) {
	w, path := tempWAL(t, Options{Policy: SyncNever})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := w.Append([]byte(fmt.Sprintf("g%d-r%d", g, i))); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	w.Close()

	r, err := Open(Options{Path: path, Policy: SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := replayAll(t, r); len(got) != 400 {
		t.Fatalf("recovered %d records, want all 400", len(got))
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	w, _ := tempWAL(t, Options{Policy: SyncNever})
	w.Close()
	if err := w.Append([]byte("r")); err == nil {
		t.Fatal("append after close must fail")
	}
}

func TestOversizeRecordRejected(t *testing.T) {
	w, _ := tempWAL(t, Options{Policy: SyncNever})
	if err := w.Append(make([]byte, maxRecordSize+1)); err == nil {
		t.Fatal("oversize record must be rejected")
	}
}

func TestEmptyAndFreshFiles(t *testing.T) {
	w, path := tempWAL(t, Options{Policy: SyncNever})
	if got := replayAll(t, w); len(got) != 0 {
		t.Fatalf("fresh log replayed %d records", len(got))
	}
	if st := w.Status(); st.Records != 0 || st.Bytes != 0 {
		t.Fatalf("fresh status = %+v", st)
	}
	_ = path
}
