# scope

[![ci](https://github.com/rvsiyad/scope/actions/workflows/ci.yml/badge.svg)](https://github.com/rvsiyad/scope/actions/workflows/ci.yml)

An OpenAI-compatible LLM gateway with an observability stack built from scratch —
write-ahead log, Gorilla-compressed time-series storage, trace store, and a
PromQL-lite query engine. No Prometheus, no ClickHouse: the engine is the point.

Point any OpenAI SDK at it with a one-line base-URL change; every request flows
through auth, token-budget rate limiting, response caching, and provider failover,
and emits spans + metrics into the self-built storage backend.

Status: session 5 (phase B underway) — the gateway (streaming proxy,
hand-rolled circuit breakers, failover, token-budget rate limiting,
response cache, telemetry emission) is complete, and the backend has its
durability spine: a from-scratch write-ahead log with torn-write recovery
behind the collector's ingest API.

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
| `docs/` | learning log, ADRs |

Later phases add `tsdb` (the storage engine), `tracestore`, `query`, and `ui`.

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
TTFT marker and token counts, plus per-request metric samples
(`gateway_requests_total`, `gateway_request_duration_ms`, `gateway_ttft_ms`,
`gateway_tokens_total`, `gateway_cost_usd`). Emission is fire-and-forget by
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

## Test

```sh
go test ./...
```
