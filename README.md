# scope

[![ci](https://github.com/rvsiyad/scope/actions/workflows/ci.yml/badge.svg)](https://github.com/rvsiyad/scope/actions/workflows/ci.yml)

An OpenAI-compatible LLM gateway with an observability stack built from scratch —
write-ahead log, Gorilla-compressed time-series storage, trace store, and a
PromQL-lite query engine. No Prometheus, no ClickHouse: the engine is the point.

Point any OpenAI SDK at it with a one-line base-URL change; every request flows
through auth, token-budget rate limiting, response caching, and provider failover,
and emits spans + metrics into the self-built storage backend.

Status: session 1 — streaming proxy working end to end (Ollama provider).

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
| `cmd/gateway` | gateway entrypoint |
| `internal/gateway` | OpenAI-compatible API, provider adapters, breakers, cache, rate limiter |
| `docs/` | learning log, ADRs |

Later phases add `collector` (WAL ingest), `tsdb` (the storage engine),
`tracestore`, `query`, and `ui`.

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
# {"status":"ok","ollama":"ok","postgres":"ok"}
```

## Test

```sh
go test ./...
```
