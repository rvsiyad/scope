package tsdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Segments are the head block's afterlife: a flush freezes every live
// Gorilla stream into one immutable, time-partitioned file, and the head
// starts over empty. Immutability is what makes everything downstream
// simple — a segment never changes, so reads need no locks, caches never
// invalidate, and compaction (S8) can replace files wholesale with an
// atomic rename instead of editing anything in place. This is the
// LSM-flavored write path: mutate only in memory, persist only by writing
// new files.
//
// Segment file format v1:
//
//	magic "SCOPESG1" (8 bytes)
//	then one record per series:
//	  length u32 | crc32c(payload) u32 | payload
//	  payload = metaLen u32 | meta JSON | Gorilla block bytes
//
// The framing is the WAL's (length-prefixed, CRC32C), but the recovery
// contract is deliberately the opposite: a WAL ends in a torn write on
// every crash, so recovery keeps the longest valid prefix — a segment was
// fully written, fsynced, and only then renamed into place, so there is no
// legal way for one to be half-good. Any bad byte fails Open outright;
// "repairing" a segment would mean silently returning partial query
// results forever.

const segMagic = "SCOPESG1"

// maxSegRecord caps one series record (meta + compressed block). As in the
// WAL, a garbage length prefix must read as corruption, never as an
// instruction to allocate gigabytes.
const maxSegRecord = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// segMeta is the per-series JSON header inside a record. The sample count
// lives here because a Gorilla block cannot self-terminate (trailing
// padding is indistinguishable from data); min/max let reads skip series
// without decoding.
type segMeta struct {
	Labels  Labels `json:"labels"`
	Samples int    `json:"samples"`
	MinT    int64  `json:"min_t"`
	MaxT    int64  `json:"max_t"`
}

// segSeries is one series loaded from a segment: its frozen block plus the
// meta needed to decode and prune.
type segSeries struct {
	block []byte
	meta  segMeta
}

// Segment is an open, fully-loaded segment file. Blocks stay in memory —
// they are Gorilla-compressed, so a segment holding a million samples is a
// few megabytes; mmap and on-demand reads are the production upgrade, not
// a need at this scale. Immutable after Open, therefore safe for
// concurrent use with no locking.
type Segment struct {
	path       string
	ix         *memIndex
	series     map[uint64]*segSeries
	minT, maxT int64
	samples    int
}

// Flush freezes the head into a new segment file at path and resets the
// head to empty. Returns the number of series written; a head with no
// samples writes nothing and returns 0.
//
// The write is crash-safe by ordering: everything goes to path+".tmp",
// which is fsynced and only then renamed over path (with the directory
// fsynced so the name itself survives) — a crash mid-flush leaves at worst
// a stale .tmp to ignore, never a half-segment under the real name. The
// head is reset only after the rename succeeds; on any error it is
// untouched and the flush can simply be retried.
//
// The head's lock is held for the whole flush, so appends stall while the
// file writes. That is a deliberate simplicity: the WAL in front of the
// head means stalled appends are queued upstream, not at risk, and the
// lock-free answer (Prometheus swaps in a fresh head and flushes the old
// one in the background) is the upgrade path if flush pauses ever matter.
func (h *Head) Flush(path string) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.series) == 0 {
		return 0, nil
	}

	// Deterministic file layout: series ordered by canonical key.
	ids := make([]uint64, 0, len(h.series))
	for id := range h.series {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return h.series[ids[i]].labels.Key() < h.series[ids[j]].labels.Key()
	})

	if err := writeSegmentFile(path, h.series, ids); err != nil {
		return 0, err
	}

	n := len(h.series)
	h.ix = newMemIndex()
	h.series = map[uint64]*memSeries{}
	return n, nil
}

// writeSegmentFile writes series (in the given id order) to path with the
// crash-safe tmp → fsync → rename → fsync-dir dance. Shared by Flush and
// compaction — one implementation of "how a segment becomes durable".
func writeSegmentFile(path string, series map[uint64]*memSeries, ids []uint64) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // no-op after a successful rename

	if err := writeSegmentTo(f, series, ids); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func writeSegmentTo(w io.Writer, series map[uint64]*memSeries, ids []uint64) error {
	if _, err := w.Write([]byte(segMagic)); err != nil {
		return err
	}
	for _, id := range ids {
		s := series[id]
		meta, err := json.Marshal(segMeta{
			Labels:  s.labels,
			Samples: s.enc.Len(),
			MinT:    s.minT,
			MaxT:    s.maxT,
		})
		if err != nil {
			return err
		}
		block := s.enc.Bytes()
		payload := make([]byte, 4+len(meta)+len(block))
		binary.LittleEndian.PutUint32(payload[0:4], uint32(len(meta)))
		copy(payload[4:], meta)
		copy(payload[4+len(meta):], block)

		var header [8]byte
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(payload, castagnoli))
		if _, err := w.Write(header[:]); err != nil {
			return err
		}
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// syncDir fsyncs a directory so a just-renamed file's directory entry is
// durable, not only its bytes.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// OpenSegment loads a segment file. Any structural problem — bad magic,
// short record, CRC mismatch, meta/block inconsistency — fails the open;
// see the format comment for why segments never "recover".
func OpenSegment(path string) (*Segment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < len(segMagic) || string(data[:len(segMagic)]) != segMagic {
		return nil, fmt.Errorf("segment %s: bad magic", path)
	}
	seg := &Segment{path: path, ix: newMemIndex(), series: map[uint64]*segSeries{}}
	rest := data[len(segMagic):]
	for len(rest) > 0 {
		if len(rest) < 8 {
			return nil, fmt.Errorf("segment %s: truncated record header", path)
		}
		length := binary.LittleEndian.Uint32(rest[0:4])
		crc := binary.LittleEndian.Uint32(rest[4:8])
		if length > maxSegRecord {
			return nil, fmt.Errorf("segment %s: record length %d exceeds cap", path, length)
		}
		if uint32(len(rest)-8) < length {
			return nil, fmt.Errorf("segment %s: truncated record payload", path)
		}
		payload := rest[8 : 8+length]
		if crc32.Checksum(payload, castagnoli) != crc {
			return nil, fmt.Errorf("segment %s: record CRC mismatch", path)
		}
		if err := seg.addRecord(payload); err != nil {
			return nil, fmt.Errorf("segment %s: %w", path, err)
		}
		rest = rest[8+length:]
	}
	return seg, nil
}

func (s *Segment) addRecord(payload []byte) error {
	if len(payload) < 4 {
		return errors.New("record too short for meta length")
	}
	metaLen := binary.LittleEndian.Uint32(payload[0:4])
	if uint32(len(payload)-4) < metaLen {
		return errors.New("meta length exceeds record")
	}
	var meta segMeta
	if err := json.Unmarshal(payload[4:4+metaLen], &meta); err != nil {
		return fmt.Errorf("bad series meta: %w", err)
	}
	if meta.Samples <= 0 {
		return fmt.Errorf("series %s: non-positive sample count", meta.Labels.Key())
	}
	id, created := s.ix.getOrCreate(meta.Labels)
	if !created {
		return fmt.Errorf("series %s appears twice", meta.Labels.Key())
	}
	// The payload slice aliases the file's one big read buffer; that is
	// fine because both are immutable from here on.
	s.series[id] = &segSeries{block: payload[4+metaLen:], meta: meta}
	if s.samples == 0 || meta.MinT < s.minT {
		s.minT = meta.MinT
	}
	if meta.MaxT > s.maxT {
		s.maxT = meta.MaxT
	}
	s.samples += meta.Samples
	return nil
}

// Select mirrors Head.Select over the frozen data: matching series
// restricted to mint <= t <= maxt, sorted by canonical key, empty series
// omitted. Per-series min/max from the meta prune without decoding.
func (s *Segment) Select(matchers []Matcher, mint, maxt int64) ([]Series, error) {
	var out []Series
	for _, id := range s.ix.selectIDs(matchers) {
		ss := s.series[id]
		if ss.meta.MaxT < mint || ss.meta.MinT > maxt {
			continue
		}
		samples, err := decodeRange(ss.block, ss.meta.Samples, mint, maxt)
		if err != nil {
			// Unlike the head decoding its own live stream, a segment block
			// that passed its CRC yet fails to decode means the file was
			// written wrong — an error for the caller, not a panic.
			return nil, fmt.Errorf("segment %s: series %s: %w", s.path, ss.meta.Labels.Key(), err)
		}
		if len(samples) > 0 {
			out = append(out, Series{Labels: ss.meta.Labels, Samples: samples})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.Key() < out[j].Labels.Key() })
	return out, nil
}

// MinTime, MaxTime, NumSeries, and NumSamples describe the frozen data;
// the DB uses the time bounds to skip whole segments during queries.
func (s *Segment) MinTime() int64  { return s.minT }
func (s *Segment) MaxTime() int64  { return s.maxT }
func (s *Segment) NumSeries() int  { return s.ix.numSeries() }
func (s *Segment) NumSamples() int { return s.samples }

// Path returns the file the segment was opened from.
func (s *Segment) Path() string { return s.path }
