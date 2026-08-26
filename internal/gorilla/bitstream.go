// Package gorilla implements the two compression tricks from Facebook's
// Gorilla paper (Pelkonen et al., VLDB 2015) that make in-memory
// time-series databases affordable: timestamps as delta-of-delta, values as
// XOR against their predecessor. Both exploit the same fact about telemetry
// — consecutive samples are nearly identical — and both pay off only at the
// bit level: a sample that would take 16 raw bytes (int64 timestamp +
// float64 value) compresses to ~1.37 bytes in the paper's fleet-wide
// measurement. This package is the codec alone, deliberately standalone and
// property-tested; the head block (S7) owns per-series state and calls it.
package gorilla

import "errors"

// bitWriter appends individual bits and bit-fields to a byte slice,
// MSB-first within each byte — compression formats are defined in bits,
// so the byte-oriented world stops here.
type bitWriter struct {
	b []byte
	// free is how many bits remain unused in the last byte of b (0 when
	// the last byte is full or b is empty).
	free uint
}

func (w *bitWriter) writeBit(bit uint64) {
	if w.free == 0 {
		w.b = append(w.b, 0)
		w.free = 8
	}
	w.free--
	if bit != 0 {
		w.b[len(w.b)-1] |= 1 << w.free
	}
}

// writeBits appends the low n bits of v, most significant first.
func (w *bitWriter) writeBits(v uint64, n uint) {
	for n > 0 {
		n--
		w.writeBit((v >> n) & 1)
	}
}

func (w *bitWriter) bytes() []byte { return w.b }

var errShortStream = errors.New("gorilla: bit stream exhausted")

// bitReader consumes what bitWriter produced.
type bitReader struct {
	b   []byte
	off int // next byte index
	// cur is the bit position within b[off] (0 = MSB).
	cur uint
}

func newBitReader(b []byte) *bitReader { return &bitReader{b: b} }

func (r *bitReader) readBit() (uint64, error) {
	if r.off >= len(r.b) {
		return 0, errShortStream
	}
	bit := uint64(r.b[r.off]>>(7-r.cur)) & 1
	if r.cur++; r.cur == 8 {
		r.cur = 0
		r.off++
	}
	return bit, nil
}

func (r *bitReader) readBits(n uint) (uint64, error) {
	var v uint64
	for i := uint(0); i < n; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		v = v<<1 | bit
	}
	return v, nil
}
