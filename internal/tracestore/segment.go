package tracestore

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

	"github.com/rvsiyad/scope/internal/telemetry"
)

// Trace segments are the head's afterlife, exactly as in the tsdb: a flush
// freezes every live trace into one immutable file and the head starts
// over empty. The framing is the same battle-tested shape (length-prefixed,
// CRC32C records, tmp → fsync → rename) and the recovery contract is the
// same as the tsdb segment's: a segment was fully written before it got its
// name, so any bad byte fails Open outright — segments never "recover".
//
// What differs is what a record holds. A tsdb record is one series' Gorilla
// block, found by label matchers through an inverted index; a trace record
// is one complete span tree, found by trace id through a plain map. The
// spans are stored as JSON (the project's wire encoding — curl-able and
// debuggable; binary is the upgrade when it earns it) and decoded lazily:
// the meta headers build the id and time indexes at Open, but a trace's
// span bytes are only unmarshalled when someone actually asks for that
// waterfall — the whole point of a read-by-id layout.
//
// Trace segment file format v1:
//
//	magic "SCOPETR1" (8 bytes)
//	then one record per trace:
//	  length u32 | crc32c(payload) u32 | payload
//	  payload = metaLen u32 | meta JSON | spans JSON array
const traceMagic = "SCOPETR1"

// maxTraceRecord caps one trace record. As in the WAL and the tsdb, a
// garbage length prefix must read as corruption, never as an instruction
// to allocate gigabytes.
const maxTraceRecord = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// traceMeta is the per-trace JSON header inside a record: everything List
// and pruning need without touching the span bytes.
type traceMeta struct {
	TraceID  string `json:"trace_id"`
	Spans    int    `json:"spans"`
	MinStart int64  `json:"min_start"`
	MaxEnd   int64  `json:"max_end"`
}

// segTrace is one trace loaded from a segment: raw span bytes plus meta.
type segTrace struct {
	meta traceMeta
	raw  []byte // JSON-encoded []telemetry.Span, decoded on demand
}

// Segment is an open trace segment file. Immutable after Open, therefore
// safe for concurrent use with no locking.
type Segment struct {
	path       string
	byID       map[string]*segTrace
	infos      []TraceInfo // newest first, the frozen time index
	minT, maxT int64
	spans      int
}

// Flush freezes the head into a new segment file at path and resets the
// head to empty. Returns the number of traces written; an empty head
// writes nothing and returns 0.
//
// Crash safety is the tsdb flush's ordering, shared verbatim: write to
// path+".tmp", fsync, rename over path, fsync the directory. The head is
// reset only after the rename succeeds; on error it is untouched and the
// flush can simply be retried.
//
// A trace that is still in flight when the flush fires gets split: its
// flushed spans freeze here and later spans land in the fresh head. That
// is fine by construction — the read path merges every source and dedupes
// by span id, so a split trace reassembles on every read.
func (h *Head) Flush(path string) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.traces) == 0 {
		return 0, nil
	}

	// Deterministic file layout: traces ordered newest first, matching the
	// order List serves.
	infos := make([]TraceInfo, 0, len(h.traces))
	for id, tr := range h.traces {
		infos = append(infos, TraceInfo{TraceID: id, MinStart: tr.minStart, MaxEnd: tr.maxEnd, Spans: len(tr.spans)})
	}
	sortInfos(infos)

	if err := writeSegmentFile(path, h.traces, infos); err != nil {
		return 0, err
	}

	n := len(h.traces)
	h.traces = map[string]*memTrace{}
	h.spans = 0
	return n, nil
}

// writeSegmentFile writes traces (in the given order) to path with the
// crash-safe tmp → fsync → rename → fsync-dir dance.
func writeSegmentFile(path string, traces map[string]*memTrace, infos []TraceInfo) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // no-op after a successful rename

	if err := writeSegmentTo(f, traces, infos); err != nil {
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

func writeSegmentTo(w io.Writer, traces map[string]*memTrace, infos []TraceInfo) error {
	if _, err := w.Write([]byte(traceMagic)); err != nil {
		return err
	}
	for _, info := range infos {
		tr := traces[info.TraceID]
		// Spans stored sorted by start: the order every reader wants, paid
		// once at freeze time.
		spans := make([]telemetry.Span, len(tr.spans))
		copy(spans, tr.spans)
		sort.SliceStable(spans, func(i, j int) bool { return spans[i].Start < spans[j].Start })

		meta, err := json.Marshal(traceMeta{
			TraceID:  info.TraceID,
			Spans:    len(spans),
			MinStart: info.MinStart,
			MaxEnd:   info.MaxEnd,
		})
		if err != nil {
			return err
		}
		body, err := json.Marshal(spans)
		if err != nil {
			return err
		}
		payload := make([]byte, 4+len(meta)+len(body))
		binary.LittleEndian.PutUint32(payload[0:4], uint32(len(meta)))
		copy(payload[4:], meta)
		copy(payload[4+len(meta):], body)

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

// OpenSegment loads a trace segment file's indexes. Any structural
// problem — bad magic, short record, CRC mismatch, malformed meta — fails
// the open; see the format comment for why segments never "recover". Span
// bodies are NOT decoded here: reads pay for the traces they fetch.
func OpenSegment(path string) (*Segment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < len(traceMagic) || string(data[:len(traceMagic)]) != traceMagic {
		return nil, fmt.Errorf("trace segment %s: bad magic", path)
	}
	seg := &Segment{path: path, byID: map[string]*segTrace{}}
	rest := data[len(traceMagic):]
	for len(rest) > 0 {
		if len(rest) < 8 {
			return nil, fmt.Errorf("trace segment %s: truncated record header", path)
		}
		length := binary.LittleEndian.Uint32(rest[0:4])
		crc := binary.LittleEndian.Uint32(rest[4:8])
		if length > maxTraceRecord {
			return nil, fmt.Errorf("trace segment %s: record length %d exceeds cap", path, length)
		}
		if uint32(len(rest)-8) < length {
			return nil, fmt.Errorf("trace segment %s: truncated record payload", path)
		}
		payload := rest[8 : 8+length]
		if crc32.Checksum(payload, castagnoli) != crc {
			return nil, fmt.Errorf("trace segment %s: record CRC mismatch", path)
		}
		if err := seg.addRecord(payload); err != nil {
			return nil, fmt.Errorf("trace segment %s: %w", path, err)
		}
		rest = rest[8+length:]
	}
	sortInfos(seg.infos)
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
	var meta traceMeta
	if err := json.Unmarshal(payload[4:4+metaLen], &meta); err != nil {
		return fmt.Errorf("bad trace meta: %w", err)
	}
	if meta.TraceID == "" || meta.Spans <= 0 {
		return fmt.Errorf("trace %q: empty id or non-positive span count", meta.TraceID)
	}
	if _, dup := s.byID[meta.TraceID]; dup {
		return fmt.Errorf("trace %s appears twice", meta.TraceID)
	}
	// The raw slice aliases the file's one big read buffer; that is fine
	// because both are immutable from here on.
	s.byID[meta.TraceID] = &segTrace{meta: meta, raw: payload[4+metaLen:]}
	s.infos = append(s.infos, TraceInfo{TraceID: meta.TraceID, MinStart: meta.MinStart, MaxEnd: meta.MaxEnd, Spans: meta.Spans})
	if s.spans == 0 || meta.MinStart < s.minT {
		s.minT = meta.MinStart
	}
	if meta.MaxEnd > s.maxT {
		s.maxT = meta.MaxEnd
	}
	s.spans += meta.Spans
	return nil
}

// Trace returns the trace's spans (sorted by start, as frozen), or nil if
// this segment holds no part of the id. Decoding happens here, per fetch —
// a body that passed its CRC yet fails to decode means the file was
// written wrong: an error for the caller, not a panic.
func (s *Segment) Trace(id string) ([]telemetry.Span, error) {
	tr, ok := s.byID[id]
	if !ok {
		return nil, nil
	}
	var spans []telemetry.Span
	if err := json.Unmarshal(tr.raw, &spans); err != nil {
		return nil, fmt.Errorf("trace segment %s: trace %s: %w", s.path, id, err)
	}
	if len(spans) != tr.meta.Spans {
		return nil, fmt.Errorf("trace segment %s: trace %s: body holds %d spans, meta says %d",
			s.path, id, len(spans), tr.meta.Spans)
	}
	return spans, nil
}

// List mirrors Head.List over the frozen data: directory entries for
// traces overlapping [mint, maxt], newest first, straight off the meta —
// no span bytes touched.
func (s *Segment) List(mint, maxt int64) []TraceInfo {
	out := make([]TraceInfo, 0, len(s.infos))
	for _, info := range s.infos {
		if info.MaxEnd < mint || info.MinStart > maxt {
			continue
		}
		out = append(out, info)
	}
	return out
}

// MinTime, MaxTime, NumTraces, and NumSpans describe the frozen data; the
// store uses the time bounds to skip whole segments during reads.
func (s *Segment) MinTime() int64 { return s.minT }
func (s *Segment) MaxTime() int64 { return s.maxT }
func (s *Segment) NumTraces() int { return len(s.byID) }
func (s *Segment) NumSpans() int  { return s.spans }
func (s *Segment) Path() string   { return s.path }
