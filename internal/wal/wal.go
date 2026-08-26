// Package wal is a write-ahead log: an append-only file of checksummed
// records that turns "we wrote it" into "it survives a crash". Every
// storage system in the lineage this project studies — Postgres, Kafka,
// Prometheus — starts here, because the WAL is what makes an ack mean
// something: data is acknowledged only once it is somewhere a crash can't
// take it (with SyncAlways) or at least somewhere the process dying can't
// take it (SyncInterval/SyncNever — the OS page cache survives a killed
// process, not a killed machine).
//
// The record format is the classic one:
//
//	record := length u32 | crc32c(payload) u32 | payload bytes
//
// Length-prefixing frames records without escaping; the CRC (Castagnoli,
// the polynomial with hardware support everywhere) makes corruption —
// including a torn write, where the machine died mid-record — detectable
// instead of silently poisoning everything after it.
//
// Recovery scans from the start and keeps the longest valid prefix: a
// short or checksum-failing tail is a torn final write, truncated away so
// the next append lands on a clean boundary. A record was either fully
// written or it never happened — the WAL's version of atomicity.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// SyncPolicy is when Append fsyncs — the durability/throughput dial.
type SyncPolicy int

const (
	// SyncAlways fsyncs before Append returns: an acked record survives
	// power loss. The slowest and only honest-to-the-hardware option.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs on a background timer: an acked record survives
	// a process crash immediately, power loss only after the next tick.
	// Bounded loss window, near-SyncNever throughput.
	SyncInterval
	// SyncNever leaves fsync to the OS: acked records survive a process
	// crash (the page cache is the kernel's), not a machine crash.
	SyncNever
)

const defaultSyncInterval = 100 * time.Millisecond

// headerSize is the fixed record prefix: u32 length + u32 CRC.
const headerSize = 8

// maxRecordSize bounds a single record (16 MiB). A length prefix larger
// than this is treated as corruption during recovery rather than an
// instruction to allocate gigabytes.
const maxRecordSize = 16 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Options struct {
	Path   string
	Policy SyncPolicy
	// Interval is the SyncInterval tick; ignored for other policies.
	Interval time.Duration
}

// WAL is a single append-only log file. Safe for concurrent Append.
type WAL struct {
	opts Options

	mu   sync.Mutex
	f    *os.File
	size int64
	n    int // records in the log
	// sync is f.Sync, injectable so tests count fsyncs without a disk.
	sync func() error

	stop chan struct{}
	done chan struct{}
}

// Open opens (or creates) the log at opts.Path, recovers it — scanning
// every record and truncating a torn or corrupt tail so the file ends on
// a record boundary — and returns a WAL ready to append.
func Open(opts Options) (*WAL, error) {
	if opts.Interval == 0 {
		opts.Interval = defaultSyncInterval
	}
	f, err := os.OpenFile(opts.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	valid, n, err := scan(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal recovery: %w", err)
	}
	// Truncate the invalid tail, if any, and position at the new end.
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	w := &WAL{opts: opts, f: f, size: valid, n: n, sync: f.Sync,
		stop: make(chan struct{}), done: make(chan struct{})}
	if opts.Policy == SyncInterval {
		go w.syncLoop()
	} else {
		close(w.done)
	}
	return w, nil
}

// scan reads records from the start and returns the offset just past the
// last valid one, plus the count. Anything unreadable — short header,
// absurd length, short payload, CRC mismatch — ends the scan there: it is
// either a torn final write (normal after a crash) or corruption, and in
// both cases everything before it is intact and everything from it on is
// untrustworthy.
func scan(f *os.File) (valid int64, n int, err error) {
	var header [headerSize]byte
	r := io.Reader(f)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	buf := []byte{}
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return valid, n, nil // clean EOF or torn header — stop here
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		crc := binary.LittleEndian.Uint32(header[4:8])
		if length > maxRecordSize {
			return valid, n, nil // garbage length: corrupt from here on
		}
		if cap(buf) < int(length) {
			buf = make([]byte, length)
		}
		buf = buf[:length]
		if _, err := io.ReadFull(r, buf); err != nil {
			return valid, n, nil // torn payload
		}
		if crc32.Checksum(buf, castagnoli) != crc {
			return valid, n, nil // corrupt payload
		}
		valid += headerSize + int64(length)
		n++
	}
}

// Append writes one record. When it returns nil the record is framed and
// checksummed in the log — and, under SyncAlways, fsync'd: that return is
// the caller's license to ack upstream. Any error means the record must
// not be acked (the log truncates back to the last good boundary on the
// next recovery anyway).
func (w *WAL) Append(payload []byte) error {
	if len(payload) > maxRecordSize {
		return fmt.Errorf("wal: record of %d bytes exceeds max %d", len(payload), maxRecordSize)
	}
	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(payload, castagnoli))

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("wal: closed")
	}
	// One writev-style call: header and payload land together or the torn
	// half is caught by recovery's CRC check.
	if _, err := w.f.Write(append(header[:], payload...)); err != nil {
		return err
	}
	w.size += headerSize + int64(len(payload))
	w.n++
	if w.opts.Policy == SyncAlways {
		return w.sync()
	}
	return nil
}

func (w *WAL) syncLoop() {
	defer close(w.done)
	ticker := time.NewTicker(w.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.f != nil {
				w.sync()
			}
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}
}

// Replay streams every record to fn in append order. Called on startup to
// rebuild downstream state; fn returning an error aborts the replay.
// Replay must not run concurrently with Append.
func (w *WAL) Replay(fn func(payload []byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	defer w.f.Seek(w.size, io.SeekStart)

	var header [headerSize]byte
	remaining := w.n
	for remaining > 0 {
		if _, err := io.ReadFull(w.f, header[:]); err != nil {
			return fmt.Errorf("wal replay: %w", err)
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			return fmt.Errorf("wal replay: %w", err)
		}
		if err := fn(payload); err != nil {
			return err
		}
		remaining--
	}
	return nil
}

// Close fsyncs whatever is buffered and closes the file — a clean
// shutdown never relies on the sync policy's timing.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.f == nil {
		w.mu.Unlock()
		return nil
	}
	syncErr := w.sync()
	closeErr := w.f.Close()
	w.f = nil
	w.mu.Unlock()

	if w.opts.Policy == SyncInterval {
		close(w.stop)
		<-w.done
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Status reports the log's shape for /healthz.
type Status struct {
	Records int    `json:"records"`
	Bytes   int64  `json:"bytes"`
	Policy  string `json:"sync_policy"`
}

func (w *WAL) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	policy := map[SyncPolicy]string{SyncAlways: "always", SyncInterval: "interval", SyncNever: "never"}[w.opts.Policy]
	return Status{Records: w.n, Bytes: w.size, Policy: policy}
}
