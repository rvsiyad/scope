"""Live query engine demo: real gateway traffic answered by PromQL-lite.

Run the collector, then the gateway pointed at it (a fast tsdb flush makes
the head/segment boundary participate; a demo price puts dollars on the
cost series):

    SCOPE_TSDB_FLUSH=5s go run ./cmd/collector
    SCOPE_COLLECTOR_URL=http://localhost:9091 \
    SCOPE_PRICE_PER_M_TOKENS=400 go run ./cmd/gateway

then:

    python3 examples/query_demo.py

The script sends a handful of chat completions through the gateway (cache
misses and hits both, so the outcome labels differ), gives the emitter a
moment to batch, and then asks the collector's query API the questions a
dashboard would:

  * rate(gateway_tokens_total[2m])          -- tokens per second
  * sum by (cache) (rate(gateway_requests_total[2m]))
                                            -- request rate, split hit/miss
  * quantile_over_time(0.5|0.99, gateway_request_duration_ms[2m])
                                            -- p50 / p99 request latency
  * increase(gateway_cost_usd[2m])          -- dollars spent in the window

Every number comes back through the real path: parser -> engine -> unified
head/segment reads -> window math. Stdlib only — no dependencies.
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request

GATEWAY = "http://localhost:8090"
COLLECTOR = "http://localhost:9091"
REQUESTS = 4


def chat(i: int):
    body = {
        "model": "llama3.2:1b",
        # temperature 0 with a repeating question: the repeats hit the
        # cache, so the request-rate query has both outcomes to split.
        "temperature": 0,
        "messages": [{"role": "user",
                      "content": f"In one short sentence, what is a time series? (v{i % 2})"}],
        "max_tokens": 50,
    }
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        json.load(resp)


def query(expr: str):
    url = COLLECTOR + "/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.load(resp)["result"]


def show(title: str, expr: str, unit: str = ""):
    print(f"\n  {title}\n    {expr}")
    result = query(expr)
    if not result:
        print("    (no data in the window)")
        return
    for series in result:
        name = series["series"] or "{}"
        v = series["samples"][-1]["v"]
        print(f"    {name:42s} {v:12.4f} {unit}")


def main():
    print(f"sending {REQUESTS} chat completions through the gateway...")
    for i in range(REQUESTS):
        chat(i)
        print(f"  request {i + 1}/{REQUESTS} done")
    print("waiting for the emitter's batches to land...")
    time.sleep(3)

    show("Tokens per second (the README's promised query):",
         "rate(gateway_tokens_total[2m])", "tokens/s")
    show("Request rate, split by cache outcome:",
         "sum by (cache) (rate(gateway_requests_total[2m]))", "req/s")
    show("Request latency p50:",
         "quantile_over_time(0.5, gateway_request_duration_ms[2m])", "ms")
    show("Request latency p99:",
         "quantile_over_time(0.99, gateway_request_duration_ms[2m])", "ms")
    show("Dollars spent in the window:",
         "increase(gateway_cost_usd[2m])", "USD")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        raise SystemExit(f"cannot reach the stack ({e}); start the collector "
                         "and the gateway as shown in the docstring") from e
