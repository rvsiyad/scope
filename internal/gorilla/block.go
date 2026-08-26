package gorilla

import "math"

// The public codec: one Encoder per series, timestamps and values
// interleaved in a single bit stream exactly as the paper stores them —
// per sample, the timestamp's dod bits then the value's XOR bits. This is
// the type the head block (S7) holds per series.

// Encoder compresses one series' samples, appended in time order.
type Encoder struct {
	w    bitWriter
	ts   tsEncoder
	vals valEncoder
	n    int
}

func NewEncoder() *Encoder { return &Encoder{} }

// Append adds one sample. Samples must arrive in non-decreasing time
// order — enforcing that is the store's job, not the codec's.
func (e *Encoder) Append(t int64, v float64) {
	e.ts.append(&e.w, t)
	e.vals.append(&e.w, math.Float64bits(v))
	e.n++
}

// Len reports how many samples have been appended.
func (e *Encoder) Len() int { return e.n }

// Bytes is the compressed block so far. The final byte may be partially
// filled; Len's sample count — not the byte length — is what tells an
// iterator where the stream ends. The slice aliases the encoder's buffer:
// copy it before the next Append if it must outlive one.
func (e *Encoder) Bytes() []byte { return e.w.bytes() }

// BytesPerSample is the number the paper is famous for (theirs: 1.37).
func (e *Encoder) BytesPerSample() float64 {
	if e.n == 0 {
		return 0
	}
	return float64(len(e.w.bytes())) / float64(e.n)
}

// Iterator decodes a block produced by Encoder. The sample count must be
// supplied by the caller (the store tracks it alongside the block) because
// the stream's trailing padding bits are indistinguishable from data.
type Iterator struct {
	r    *bitReader
	ts   tsDecoder
	vals valDecoder
	n    int
	t    int64
	v    float64
	err  error
}

func NewIterator(block []byte, samples int) *Iterator {
	return &Iterator{r: newBitReader(block), n: samples}
}

// Next advances to the next sample; false means the block is exhausted or
// corrupt (see Err).
func (it *Iterator) Next() bool {
	if it.err != nil || it.n == 0 {
		return false
	}
	t, err := it.ts.next(it.r)
	if err != nil {
		it.err = err
		return false
	}
	bits, err := it.vals.next(it.r)
	if err != nil {
		it.err = err
		return false
	}
	it.t, it.v = t, math.Float64frombits(bits)
	it.n--
	return true
}

// At returns the current sample.
func (it *Iterator) At() (int64, float64) { return it.t, it.v }

// Err reports a truncated or corrupt block — a stream that ran out of bits
// before the promised sample count.
func (it *Iterator) Err() error { return it.err }
