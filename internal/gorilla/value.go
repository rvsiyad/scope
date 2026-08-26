package gorilla

import "math/bits"

// XOR value encoding (paper §4.1.2). Consecutive samples of one series are
// numerically close, and close float64s share their sign, exponent, and
// leading mantissa bits — so v XOR prev is mostly zeros with a short run of
// "meaningful" bits in the middle. Three cases per value:
//
//	'0'                          identical to the previous value (one bit —
//	                             gauges sit still and counters between
//	                             increments are this case)
//	'10' + meaningful bits       the XOR fits inside the previous value's
//	                             leading/trailing-zero window, so the window
//	                             needn't be re-described
//	'11' + 5b leading + 6b len   a new window: leading-zero count, length of
//	     + meaningful bits       the meaningful run, then the run itself
//
// The '10' case is the paper's sneaky win: windows are sticky, so a run of
// similar deltas describes its shape once and then pays only for payload.
// Values are treated as opaque 64-bit patterns (math.Float64bits upstream):
// NaNs, infinities, and negative zero round-trip bit-exactly.

// valEncoder is the per-series value state: previous bit pattern and the
// current leading/trailing window.
type valEncoder struct {
	prev     uint64
	leading  uint
	trailing uint
	n        int
}

func (e *valEncoder) append(w *bitWriter, v uint64) {
	if e.n == 0 {
		w.writeBits(v, 64)
		// A fake impossibly-wide window forces the first XOR to describe
		// its own window instead of inheriting a meaningless one.
		e.prev, e.leading, e.trailing, e.n = v, 65, 65, 1
		return
	}
	e.n++
	xor := v ^ e.prev
	e.prev = v
	if xor == 0 {
		w.writeBit(0)
		return
	}
	w.writeBit(1)

	leading := uint(bits.LeadingZeros64(xor))
	trailing := uint(bits.TrailingZeros64(xor))
	// The 5-bit leading field caps at 31; conceding a bit of compression
	// on tiny XORs beats widening every window descriptor.
	if leading > 31 {
		leading = 31
	}

	if leading >= e.leading && trailing >= e.trailing {
		// Fits the sticky window: reuse it.
		w.writeBit(0)
		w.writeBits(xor>>e.trailing, 64-e.leading-e.trailing)
		return
	}
	// New window.
	e.leading, e.trailing = leading, trailing
	meaningful := 64 - leading - trailing
	w.writeBit(1)
	w.writeBits(uint64(leading), 5)
	// meaningful is in [1, 64]; 64 won't fit 6 bits, so store len-1.
	w.writeBits(uint64(meaningful-1), 6)
	w.writeBits(xor>>trailing, meaningful)
}

// valDecoder mirrors valEncoder.
type valDecoder struct {
	prev     uint64
	leading  uint
	trailing uint
	n        int
}

func (d *valDecoder) next(r *bitReader) (uint64, error) {
	if d.n == 0 {
		v, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		d.prev, d.n = v, 1
		return v, nil
	}
	d.n++
	bit, err := r.readBit()
	if err != nil {
		return 0, err
	}
	if bit == 0 {
		return d.prev, nil
	}
	ctrl, err := r.readBit()
	if err != nil {
		return 0, err
	}
	if ctrl == 1 {
		lead, err := r.readBits(5)
		if err != nil {
			return 0, err
		}
		lenM1, err := r.readBits(6)
		if err != nil {
			return 0, err
		}
		d.leading = uint(lead)
		d.trailing = 64 - d.leading - (uint(lenM1) + 1)
	}
	meaningful, err := r.readBits(64 - d.leading - d.trailing)
	if err != nil {
		return 0, err
	}
	d.prev ^= meaningful << d.trailing
	return d.prev, nil
}
