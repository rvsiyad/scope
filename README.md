# scope

[![ci](https://github.com/rvsiyad/scope/actions/workflows/ci.yml/badge.svg)](https://github.com/rvsiyad/scope/actions/workflows/ci.yml)

An OpenAI-compatible LLM gateway with an observability stack built from scratch —
write-ahead log, Gorilla-compressed time-series storage, trace store, and a
PromQL-lite query engine. No Prometheus, no ClickHouse: the engine is the point.

Point any OpenAI SDK at it with a one-line base-URL change; every request flows
through auth, token-budget rate limiting, response caching, and provider failover,
and emits spans + metrics into the self-built storage backend.

Status: session 11 (phase C) — the gateway (streaming proxy, hand-rolled
circuit breakers, failover, token-budget rate limiting, response cache,
telemetry emission) is complete, and so is the backend: a durability
spine (from-scratch WAL with torn-write recovery), the Gorilla codec
(delta-of-delta + XOR, measured at 3.07 bytes/sample on real gateway
telemetry vs 16 raw), a full storage engine (head block, immutable
segments, inverted label index, compaction, cardinality guard, kill -9
crash recovery), a trace store, a PromQL-lite query engine, and live
dashboards with a trace waterfall viewer served straight out of the
collector. Remaining: deploy, docs.

Headline numbers (Apple M4 laptop; methodology, capacity probes, and
one-command reproduction in [docs/benchmarks.md](docs/benchmarks.md)):

| What | Number |
|---|---|
| Ingest, crash-proof acks (fsync=always) | ~25k points/s ceiling, ack p50 5.3 ms |
| Ingest, relaxed acks (fsync=interval) | ~600k points/s sustained, ack p99 10 ms |
| Compression on real telemetry | 3.07 bytes/sample vs 16 raw (5.2×) |
| Dashboard query (`sum by rate`, 15 m/15 s) | p99 5.4 ms |
| Gateway added overhead per request | p50 +176 µs, p99 +295 µs |

## Use it like OpenAI

```py
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8090/v1", api_key="unused-for-now")
stream = client.chat.completions.create(
    model="llama3.2:1b",
    messages=[{"role": "user", "content": "hello"}],
    stream=True,
)
```

Or watch the SSE stream raw:

```sh
curl -N localhost:8090/v1/chat/completions -d '{
  "model": "llama3.2:1b",
  "messages": [{"role":"user","content":"count to five"}],
  "stream": true
}'
```

## Layout

| Path | What lives there |
|---|---|
| `cmd/gateway`, `cmd/collector` | entrypoints |
| `internal/gateway` | OpenAI-compatible API, provider adapters, breakers, cache, rate limiter, instrumentation |
| `internal/telemetry` | span/metric model, wire format, batching fire-and-forget emitter |
| `internal/collector` | ingest API: validate → WAL → ack; handoff to the stores |
| `internal/wal` | append-only log, CRC records, torn-write recovery, fsync policies |
| `internal/gorilla` | the paper's codec: delta-of-delta timestamps, XOR values |
| `internal/tsdb` | the storage engine: head block, segment files, inverted label index, unified reads |
| `internal/tracestore` | trace segments, trace-id + time indexes, waterfall reads |
| `internal/query` | PromQL-lite: grammar, window functions, aggregations |
| `internal/ui` | embedded dashboards + trace waterfall viewer (`/ui/`) |
| `docs/` | learning log, ADRs |

## Run

Requires Go 1.27+ and Docker.

```sh
# infra: ollama (local $0 model provider) + postgres
docker compose up -d

# pull the demo model (first time only, ~1.3 GB)
docker compose exec ollama ollama pull llama3.2:1b

# gateway
go run ./cmd/gateway
```

Check everything is wired:

```sh
curl -s localhost:8090/healthz
# {"status":"ok","ollama":"ok","postgres":"ok","providers":[{"name":"ollama","breaker":"closed"}]}
```

## Failover demo

Every provider sits behind its own circuit breaker (closed → open → half-open,
built by hand — see `internal/gateway/breaker.go`). Requests go to the first
provider whose breaker admits them; an open breaker is skipped without spending
a call, and a background probe closes it again once the provider recovers.

Run the gateway with a two-instance chain and kill the primary mid-traffic:

```sh
SCOPE_OLLAMA_URLS=http://localhost:11434,http://localhost:11435 go run ./cmd/gateway
```

```sh
python3 examples/failover_demo.py
```

```sh
docker compose stop ollama   # requests reroute to ollama-2; breaker: ollama-1=open
```

```sh
docker compose start ollama  # probe closes the breaker; traffic returns
```

Streams fail over only before the first byte — once chunks have reached the
client, a replay would duplicate content, so mid-stream failures surface as-is.

## Token-budget rate limiting

Budgets are denominated in LLM tokens, not requests — one request can cost 5
tokens or 5000, so request counting protects nothing that bills by the token.
The catch: a request's true cost is unknown until the stream finishes, which
breaks the classic token bucket. Admission is therefore a three-step protocol
(`internal/gateway/budget.go`):

1. **estimate** the cost before calling the provider (prompt ≈ chars/4,
   completion = `max_tokens` or a default),
2. **reserve** the estimate against the tenant's bucket — or reject with
   `429` and an honest `Retry-After`,
3. **settle** when the true cost is known: refund the overestimate, or charge
   the overrun — driving the bucket into *debt*, because streamed tokens
   can't be un-streamed.

Tenants are configured as `name:api-key:tokens-per-minute` (burst capacity =
one minute of refill); clients authenticate the OpenAI way, so SDKs just set
`api_key`. No tenants configured = open mode, nothing rate limited.

```sh
SCOPE_TENANTS=acme:sk-acme:300,globex:sk-globex:60000 go run ./cmd/gateway
```

```sh
python3 examples/budget_demo.py   # concurrent greedy clients vs one budget
```

`/healthz` shows each tenant's live balance and admission counters; a
rejected request never reaches the provider, and a client disconnect
mid-stream still settles for the tokens that actually streamed.

## Response cache

Every hit is a provider call not made — measured, not implied: `/healthz`
reports hits, misses, and tokens saved. Only requests pinned to
`temperature: 0` are cached (at any higher temperature the provider is
*supposed* to answer differently, and a memorized reply would silently
change that); entries are keyed by a normalized hash of tenant + model +
sampling params + messages, so tenants never share entries and equivalent
requests always do. TTL bounds staleness, LRU bounds memory, and a cache hit
bypasses the token budget entirely — it consumes zero provider tokens, so
there is nothing honest to charge. A completion cached from a JSON request
serves streaming clients too (replayed as SSE), and vice versa.

## Telemetry

Every request emits its whole story: a span tree (`request → auth → cache
lookup → reserve → provider → settle`) where the provider span carries the
TTFT marker and token counts, plus metric samples per request: cumulative
counters (`gateway_requests_total`, `gateway_tokens_total`,
`gateway_cost_usd` — running totals, the shape `rate()` expects; a gateway
restart resets them to zero, which is exactly the counter reset the query
engine heals) and gauge-shaped measurements (`gateway_request_duration_ms`,
`gateway_ttft_ms`). Emission is fire-and-forget by
law: a non-blocking send onto a bounded buffer, batched to the collector in
the background; when the collector is slow or down the gateway drops and
*counts* — never blocks, never queues unboundedly (`internal/telemetry`).

```sh
go run ./cmd/collector    # ingest API on :9091 (stub — phase B grows the WAL here)
```

```sh
SCOPE_COLLECTOR_URL=http://localhost:9091 SCOPE_PRICE_PER_M_TOKENS=400 go run ./cmd/gateway
```

```sh
python3 examples/cache_telemetry_demo.py   # miss vs hit vs bypass, with receipts
```

The demo asks the same temperature-0 question twice (the second answer
arrives ~1000x faster from cache), shows a default-temperature request
correctly bypassing, and then reads both `/healthz` endpoints to prove every
span and metric sample actually reached the collector.

## Write-ahead log

The collector's `204` is a durability promise, not a pleasantry: each batch
is validated, appended to a write-ahead log (`internal/wal` — length-prefixed,
CRC32C-checksummed records), and only then acknowledged. Recovery keeps the
longest valid prefix: a torn final write (the crash case) is detected by
checksum and truncated away, so a record either fully happened or never did.
On restart the log replays into the stores through the `Consumer` handoff —
which is why phase B's stores get to live purely in memory.

When the ack happens is the durability/throughput dial (`SCOPE_WAL_SYNC`),
and the gap is why the dial exists — measured on this repo's benchmark
(`go test -bench=Append ./internal/wal/`, Apple M4, 1 KiB records):

| policy | an ack survives | measured |
|---|---|---|
| `always` (default) | power loss | ~257 appends/s |
| `interval` | process death now; power loss after ≤ one tick | ~617k appends/s |
| `never` | process death (the page cache is the kernel's) | ~619k appends/s |

Prove the contract with a real `kill -9` mid-ingest (self-contained — builds
and manages its own collector):

```sh
python3 examples/crash_recovery_demo.py
```

The script counts acks, murders the process with no shutdown hook, restarts
it on the same log, and checks every acknowledged batch replayed — in a live
run, 118/118 acks survived plus one in-flight batch that was appended but
killed before its 204 left the socket, which is exactly the semantics the
ack promises.

## Gorilla compression

`internal/gorilla` implements the two tricks from Facebook's Gorilla paper
(Pelkonen et al., VLDB 2015) that make in-memory TSDBs affordable:
timestamps stored as **delta-of-delta** (a regular scrape clock costs one
bit per sample) and float values as **XOR against their predecessor** with
sticky leading/trailing-zero windows (an unchanged value costs one bit;
similar values pay only for their differing mantissa bits). The codec is
standalone and property-tested — NaN, infinities, denormals, and negative
zero round-trip bit-exactly, and hostile input may compress badly but must
never decode wrongly.

Measured on this repo's own telemetry (a live gateway run replayed from the
collector WAL — `go run ./cmd/compressbench -wal <path>`):

| series | samples | bytes/sample | vs 16 raw |
|---|---|---|---|
| `gateway_requests_total{cache="hit",...}` | 600 | 0.95 | 16.9x |
| `gateway_tokens_total` | 623 | 1.01 | 15.8x |
| `gateway_cost_usd` | 623 | 1.12 | 14.3x |
| `gateway_request_duration_ms` | 623 | 8.92 | 1.8x |
| **total (all series)** | **2515** | **3.07** | **5.2x** |

The spread is the honest story: counter-shaped series land right at the
paper's fleet-wide 1.37, while continuous millisecond durations churn the
full mantissa every sample and settle for ~2x. `compressbench` re-decodes
every block and compares bit patterns before reporting — a compression
number from a codec that can't round-trip would be worse than no number.

## Time-series storage engine

`internal/tsdb` is Prometheus's storage architecture, miniaturized — the
same shape as the engine behind every serious metrics product:

- **Head block:** every series' newest samples, held in memory as one
  *live Gorilla stream per series* — appends compress on arrival, so the
  head's memory cost is the measured ~1–3 bytes/sample, not 16. Samples
  per series must be strictly ascending; the delta-encoded append-only
  stream physically can't hold anything else, which is why Prometheus has
  the same rule.
- **Segments:** a flush freezes the head into an immutable,
  time-partitioned file (write `.tmp` → fsync → rename → fsync the
  directory) and the head starts over. Immutability is what keeps the rest
  simple: segment reads need no locks, and compaction can replace files
  wholesale with an atomic rename.
- **Inverted label index:** `label pair → posting list of series IDs`, a
  search engine's data structure pointed at telemetry. Multi-matcher
  queries intersect posting lists (shortest first) instead of scanning
  series.
- **Unified reads:** one `Select` over head + segments, merged per series
  identity — a query can't tell where the head/segment boundary falls,
  which is the point: that boundary moves on every flush.

Segment files reuse the WAL's framing (length-prefixed, CRC32C) but invert
its recovery contract, deliberately: a WAL ends in a torn write on every
crash, so recovery keeps the longest valid prefix — a segment was fsynced
and atomically renamed into place, so there is no legal way for one to be
half-good, and any bad byte refuses to open rather than serving partial
history.

The engine runs live inside the collector: accepted metrics flow into the
head through the same `Consumer` socket as everything else, a maintenance
loop (`SCOPE_TSDB_FLUSH`, default 60s) flushes and compacts, and on
restart the collector's WAL replay repopulates the head — samples already
flushed into segments come back as duplicates that the read path dedupes
and the next compaction removes physically.

- **Compaction** merges all segments into one and is crash-safe by
  ordering plus idempotence: the merged output atomically replaces the
  oldest input before the newer inputs are deleted, and a crash anywhere
  in between leaves only harmless subsets that dedupe away. This buys
  cheap reads with write amplification — the read/write/space
  amplification triangle, chosen deliberately and documented in the code.
- **Retention** (`SCOPE_TSDB_RETENTION`, default: keep everything) rides
  along free: the merge is already re-encoding every sample, so expiry is
  just "don't copy it forward".
- **Cardinality guard** (`SCOPE_TSDB_MAX_SERIES`, default 10,000): a
  sample that would mint a new series past the cap is rejected and counted
  — one unbounded label value (a request id, a raw URL) otherwise mints
  series without limit and melts the index, which is how real TSDBs die.

Prove the whole story with a real `kill -9` mid-ingest, mid-flush,
mid-compaction:

```sh
python3 examples/tsdb_crash_recovery_demo.py
```

Unlike the WAL demo, this one verifies by *querying*: after the kill and
restart, every acknowledged sample must come back from the real read path
(head + segments + merge + dedupe), exactly once. A live run: SIGKILL
after 760 acked samples with 500ms flush/compact cycles in flight —
760/760 recovered, 1 segment on disk.

## Trace store

`internal/tracestore` is the *other* storage engine — and the reason there
are two is the deepest storage lesson in the repo: **layout follows read
pattern**. Metrics are scanned by label over a time range, so the tsdb
stores per-series Gorilla streams behind an inverted label index. Traces
are fetched whole by id ("click this request, show me its waterfall"), so
here the unit of storage is the complete span tree and the index is a
plain `trace id → record` map. Same skeleton, deliberately different
organs:

- **Shared with the tsdb:** an in-memory head fed through the collector's
  `Consumer` socket (so WAL replay rebuilds it after a crash, for free),
  immutable segment files with the same length-prefixed CRC32C framing,
  the same `.tmp` → fsync → rename flush, and the same refuse-to-open
  contract on any damaged byte.
- **Different on purpose:** no ordering constraint on arrival (nothing is
  delta-encoded), records keyed by id instead of labels, span bodies
  decoded lazily (a directory listing touches only meta headers), and **no
  merge compaction** — a read-by-id is one map probe per segment, so many
  small segments cost microseconds where a range scan would degrade
  linearly. Retention just drops whole expired segment files.
- **Traces stay whole across everything:** a trace split by a flush, or
  re-delivered by WAL replay after a graceful shutdown, reassembles on
  every read (merge all sources, dedupe by span id). Sampling
  (`SCOPE_TRACE_KEEP`, default 1 = keep all) hashes the *trace id*, so
  every span of a trace gets the same verdict across batches, restarts,
  and collectors.

The collector serves the read side directly:

```sh
curl 'localhost:9091/v1/traces?limit=5'          # request log, newest first
curl 'localhost:9091/v1/traces/<trace_id>'       # full waterfall span tree
```

The waterfall endpoint returns the nested tree — each span with its
timings, duration, and attrs (tenant, model, token counts, cost live on
the root) — which is exactly what the phase-C UI will render. See it end
to end against a live gateway:

```sh
python3 examples/trace_waterfall_demo.py
```

## Query engine

`internal/query` is what turns the storage engine into a database: a
PromQL-lite layer — selectors, window functions, aggregations, and a tiny
hand-rolled grammar — evaluated over the tsdb's unified reads, served by
the collector:

```sh
curl --get 'localhost:9091/v1/query' \
  --data-urlencode 'query=rate(gateway_tokens_total{tenant="acme"}[5m])'
```

```sh
curl --get 'localhost:9091/v1/query_range' \
  --data-urlencode 'query=sum by (tenant) (rate(gateway_tokens_total[5m]))' \
  --data "start=$(($(date +%s)*1000 - 600000))" --data "end=$(($(date +%s)*1000))" --data 'step=30s'
```

The vocabulary is exactly what the phase-C dashboards need, with the two
calculations every observability engineer must actually understand
implemented from scratch and fixture-tested against hand-computed numbers:

- **`rate` / `increase` with counter-reset healing** — a sample below its
  predecessor means the process restarted, so that sample's whole value is
  growth since the restart (`10, 20, ↯5, 8` → increase 18). One documented
  divergence from Prometheus: no extrapolation — observed growth over the
  observed span, hand-computable and honest about only what was seen.
- **`quantile_over_time`** (p50/p95/p99) — the φ-quantile of the raw
  samples in the window, linear interpolation between order statistics,
  Prometheus's method exactly. The gateway emits raw duration samples, so
  quantiles are exact for what was kept; pre-bucketed histograms (and
  their cross-instance aggregatability) are the documented upgrade.
- **`sum/avg/min/max/count by (labels)`** — cross-series folds at each
  step that compose with window functions, because both speak the same
  matrix shape: `sum by (tenant) (rate(...))`.
- Also `avg/sum/min/max/count_over_time`, `=`/`!=` matchers, and range
  queries with **step alignment** (starts floor onto the step grid, so a
  refreshing dashboard sees stable buckets, not re-bucketed history).

Semantics follow Prometheus where a dashboard would notice: instant
lookback (5m, closed window `[t-lookback, t]`), left-open range windows
`(t-r, t]` so a boundary sample belongs to one window, staleness as
*absence* (a series that stopped reporting contributes nothing — not a
phantom zero), and `rate()` dropping `__name__` from its output. One
`Select` against the store per selector per query; every per-step
computation is a forward-only pass over the selected samples.

Watch real queries answer over live gateway traffic:

```sh
python3 examples/query_demo.py
```

## Dashboards

The collector serves its own UI at [`localhost:9091/ui/`](http://localhost:9091/ui/)
— embedded into the binary with `go:embed`, so the whole observability
backend ships as one artifact with no node toolchain, asset pipeline, or
second server. The page is plain JS and a hand-rolled SVG chart component,
and it is a pure client of the read APIs above: every panel is a
`/v1/query_range` question asked every 5 seconds over a 15-minute window —

- requests/s split by outcome — `sum by (outcome) (rate(gateway_requests_total[1m]))`
- time-to-first-token p50/p95/p99 — `quantile_over_time(0.99, gateway_ttft_ms[1m])`
- tokens/s by model, spend per minute by tenant
- cache hit rate, derived client-side as hit ÷ (hit + miss) — the query
  language has no division, and the one consumer wanting a ratio is the
  right place for it

The **Traces** tab is the request log (`/v1/traces`, newest first); click
any request and its span tree renders as a waterfall — auth → reserve →
cache lookup → provider call → settle, bars scaled to the trace's wall
time, token counts and cost on the spans. Polling over push is a choice,
not a shortcut: a dashboard is a repeated *question* ("the last 15 minutes,
as of now"), and re-asking a stateless query API is trivially resumable and
cacheable where per-viewer push state is neither.

Drive a few minutes of mixed traffic through the whole stack and watch
every panel move:

```sh
python3 examples/dashboard_demo.py
```

## Test

```sh
go test ./...
```
