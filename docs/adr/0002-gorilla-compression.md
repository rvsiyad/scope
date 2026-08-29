# ADR 0002: Gorilla compression for the time-series store

Status: accepted

## Context

A metric sample is a (timestamp, float64) pair — 16 bytes raw. At even
modest ingest rates, raw storage makes an in-memory head block untenable
and a disk footprint embarrassing. Facebook's Gorilla paper (VLDB 2015)
showed that monitoring data compresses absurdly well because of two
regularities: timestamps arrive on a nearly fixed cadence, and adjacent
values are usually close to each other.

## Decision

Implement the paper's two encodings as a standalone, property-tested
codec (`internal/gorilla`):

- **Timestamps: delta-of-delta.** Store the change in the arrival
  interval, not the interval. A perfectly regular series costs one bit
  per timestamp; jitter costs a few bits; only a cadence change pays a
  wide field.
- **Values: XOR against the previous value.** Equal values cost one bit.
  Similar values share sign/exponent/leading mantissa bits, so the XOR
  has long runs of leading and trailing zeros; encode only the meaningful
  window, with a "sticky" window to avoid re-describing it every sample.

Correctness is enforced by round-trip property tests: encode→decode must
be identity on random walks, adversarial timestamps, and special floats
(NaN, infinities, -0), under `go test -race` in CI.

## Consequences

- Measured on real gateway telemetry via `cmd/compressbench`: **3.07
  bytes/sample vs 16 raw (5.2×)**. The paper reports 1.37; the gap is
  explained, not hidden — Gorilla's number comes from scrape-regular
  intervals, while our telemetry is event-driven, so delta-of-delta pays
  for irregular arrival times. Where the data is regular (counters
  emitted on a cadence), our per-series numbers approach the paper's.
- Compression is what makes the head block viable: live series stay in
  memory as compressed streams, not sample slices.
- The trade: encoded streams are append-only and sequential — no random
  access into a block, no in-place edits, and out-of-order samples are
  rejected rather than merged (`ErrOutOfOrder`). The storage layout above
  (immutable segments, ascending appends) is designed around exactly
  those constraints, which is the Prometheus design too.
