package gorilla

// Delta-of-delta timestamp encoding (paper §4.1.1). Telemetry arrives on a
// nearly regular clock — every second, every scrape interval — so while
// timestamps grow without bound and deltas cluster around the interval,
// the *change between consecutive deltas* is almost always zero. Encoding
// that second difference collapses a regular series to one bit per sample:
//
//	'0'                  dod == 0            (the steady-state bit)
//	'10'   + 7 bits      dod in [-63, 64]
//	'110'  + 9 bits      dod in [-255, 256]
//	'1110' + 12 bits     dod in [-2047, 2048]
//	'1111' + 64 bits     everything else
//
// Buckets are the paper's; values are stored offset into the unsigned
// range (dod+63, dod+255, dod+2047). The escape hatch is 64 bits where the
// paper uses 32 — our timestamps are millisecond epochs, and a codec must
// never be the thing that can't represent its input.

// tsEncoder is the per-series timestamp state: last timestamp and last
// delta. The first sample writes a raw 64-bit timestamp; the second
// encodes its delta as a dod against an implicit previous delta of zero.
type tsEncoder struct {
	t     int64
	delta int64
	n     int
}

func (e *tsEncoder) append(w *bitWriter, t int64) {
	if e.n == 0 {
		w.writeBits(uint64(t), 64)
		e.t, e.n = t, 1
		return
	}
	delta := t - e.t
	dod := delta - e.delta
	writeDoD(w, dod)
	e.t, e.delta = t, delta
	e.n++
}

// tsDecoder mirrors tsEncoder.
type tsDecoder struct {
	t     int64
	delta int64
	n     int
}

func (d *tsDecoder) next(r *bitReader) (int64, error) {
	if d.n == 0 {
		v, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		d.t, d.n = int64(v), 1
		return d.t, nil
	}
	dod, err := readDoD(r)
	if err != nil {
		return 0, err
	}
	d.delta += dod
	d.t += d.delta
	d.n++
	return d.t, nil
}

// dodClasses defines the bucket ladder once, shared by writer and reader:
// prefix bits (written value and length), payload width, and the offset
// that maps the signed range into unsigned storage.
var dodClasses = []struct {
	prefix    uint64
	prefixLen uint
	bits      uint
	min, max  int64
}{
	{0b10, 2, 7, -63, 64},
	{0b110, 3, 9, -255, 256},
	{0b1110, 4, 12, -2047, 2048},
}

func writeDoD(w *bitWriter, dod int64) {
	if dod == 0 {
		w.writeBit(0)
		return
	}
	for _, c := range dodClasses {
		if dod >= c.min && dod <= c.max {
			w.writeBits(c.prefix, c.prefixLen)
			w.writeBits(uint64(dod-c.min), c.bits)
			return
		}
	}
	w.writeBits(0b1111, 4)
	w.writeBits(uint64(dod), 64)
}

func readDoD(r *bitReader) (int64, error) {
	bit, err := r.readBit()
	if err != nil {
		return 0, err
	}
	if bit == 0 {
		return 0, nil
	}
	// Count the run of 1s (max 3 more) to find the class.
	ones := 1
	for ones < 4 {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			break
		}
		ones++
	}
	if ones == 4 {
		v, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	}
	c := dodClasses[ones-1]
	v, err := r.readBits(c.bits)
	if err != nil {
		return 0, err
	}
	return int64(v) + c.min, nil
}
