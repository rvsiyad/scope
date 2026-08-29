# Benchmarks

Every number here reproduces with one command:

```sh
sh scripts/bench.sh
```

Hardware for the numbers below: **Apple M4, 10 cores, 16 GB RAM, macOS —
2026-08-29.** Numbers are hardware-bound; the *shapes* (the fsync gap, the
µs-scale gateway overhead, where each curve bends) are the portable part.

## Methodology

Three habits, all enforced by `internal/bench` and stated here because a
benchmark that hides any of them reports fiction:

- **Open-loop arrivals.** The load generator schedules every request up
  front on a fixed clock the system under test cannot slow down — the way
  real clients arrive. A closed loop (wait for each response, then send)
  lets a stalling server pace its own exam.
- **Latency from scheduled start** (the coordinated-omission defense).
  Each request's latency is measured from when it was *supposed* to fire,
  and no scheduled request is skipped. When the server stalls, the backlog
  lands in the percentiles instead of quietly stretching the test.
  The capacity probes below show what this buys: an overloaded
  fsync=always collector reports ack p50 in *seconds*, which is what its
  clients would actually experience — not the ~4 ms a closed-loop
  generator would print.
- **Warm-up excluded, distributions reported.** One second of unmeasured
  warm-up per suite (connection pools, series registries, file growth),
  then an HDR-style histogram (~1.6% relative error at every magnitude)
  answering p50/p90/p99/p99.9/max — never an average alone.

The suites run over real loopback HTTP against real stores (WAL fsync per
policy, tsdb with a mid-run flush so queries pay the head/segment merge);
request bodies are pre-marshalled so the measured window prices the
system, not the generator.

## Ingest: throughput and the price of durability

100-point batches, 100 series, counter-shaped values. The full path:
HTTP POST → validate → WAL append (fsync per policy) → head block → 204.

Paced at 20k points/s (comfortably inside capacity for both policies):

| fsync policy | sustained | ack p50 | ack p99 | ack p99.9 |
|---|---|---|---|---|
| `always` | 20k points/s | 5.28 ms | 17.17 ms | 26.35 ms |
| `interval` | 20k points/s | 1.27 ms | 2.44 ms | 6.98 ms |

Capacity probes (raise offered load until the queue shows):

| offered | `always` | `interval` |
|---|---|---|
| 100k points/s | **saturated ~25k/s**, ack p50 7.5 s (backlog) | 100k/s, ack p50 453 µs, p99 15 ms |
| 300k points/s | saturated ~25k/s, ack p50 27 s | 300k/s, ack p50 196 µs, p99 4.2 ms |
| 600k points/s | — | **599k/s**, ack p50 157 µs, p99 10.3 ms |

The story in the numbers: `always` is bounded by the device's serialized
fsync rate (~4 ms each ⇒ ~250 acks/s ⇒ ~25k points/s at this batch size),
and past that bound the honest ack number is queueing delay in seconds.
`interval` acks after the in-memory append and lets a background clock
absorb the fsyncs, sustaining ~600k points/s on this laptop — the
durability/throughput dial the collector's configuration exposes, priced.
Bigger batches move `always`'s ceiling (more points amortize one fsync),
which is why the batch size is stated.

## Query: what a dashboard panel costs

200 series × 30 minutes of 5 s samples (~72k samples), flushed halfway so
every query crosses the head/segment boundary. The dashboard's own
question shapes, 100/s each for 10 s:

| query shape | p50 | p99 | max |
|---|---|---|---|
| `sum by (tenant) (rate(counter[1m]))`, 15 m range / 15 s step | 4.55 ms | 5.41 ms | 6.98 ms |
| `quantile_over_time(0.99, gauge[1m])`, same range | 4.55 ms | 5.28 ms | 10.01 ms |
| `increase(counter{tenant="t07"}[5m])`, instant | 1.22 ms | 1.68 ms | 10.2 ms |

The two range shapes decompress ~60 windows × 100 series per answer; the
instant query shows the index earning its keep — one tenant's five series
selected out of 200 via posting-list intersection, then a single window.

## Gateway overhead: measuring our code, not the provider's GPU

Both arms hit the same stub provider (Ollama's wire shape, zero thinking
time) over identical loopback stacks: direct, then through the full
gateway path — auth, token-budget reserve/settle, cache lookup, router +
circuit breaker, and telemetry emission into a real WAL-backed collector.
150 requests/s for 10 s:

| arm | p50 | p90 | p99 |
|---|---|---|---|
| direct to provider | 995 µs | 1.17 ms | 1.73 ms |
| through gateway | 1.17 ms | 1.45 ms | 2.02 ms |
| **added overhead** | **+176 µs** | — | **+295 µs** |

Against a real LLM call (TTFT hundreds of ms, generations in seconds),
the gateway's added ~0.2–0.3 ms is noise — which is the argument for
putting admission control, caching, and observability in the path at all.

## Compression

Measured by `cmd/compressbench` on real gateway telemetry from the WAL
(method and per-series table in the README's "Gorilla compression"
section): **3.07 bytes/sample vs 16 raw (5.2×)** on mixed counter, gauge,
and duration series. The paper's 1.37 bytes/sample is for Gorilla's
steady regular-interval scrape data; our telemetry is event-driven
(irregular timestamps cost delta-of-delta bits) — the gap and its cause
are part of the result.

## Correctness in CI

The properties the numbers stand on run under `go test -race ./...` on
every push (see `.github/workflows/ci.yml`, including a 1-second smoke
run of these suites so the benchmark code itself cannot rot):

- Gorilla codec round-trip property tests (random walks, adversarial
  timestamps, special floats) — `internal/gorilla`
- Torn-write recovery: truncated tails and corrupt records are skipped
  cleanly, never propagated — `internal/wal`
- Acked-batches-survive-restart: the collector's WAL replay rebuilds
  head state after an unclean death — `internal/collector`
- The pacer's own coordinated-omission test: a stalled system must show
  its queueing delay — `internal/bench`
