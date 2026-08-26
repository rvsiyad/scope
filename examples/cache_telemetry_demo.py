"""Live cache + telemetry demo: the same question twice, and the receipts.

Run the collector, then the gateway pointed at it (a demo price makes the
savings show up in dollars):

    go run ./cmd/collector
    SCOPE_COLLECTOR_URL=http://localhost:9091 \
    SCOPE_PRICE_PER_M_TOKENS=400 go run ./cmd/gateway

then:

    python3 examples/cache_telemetry_demo.py

Round 1 asks a temperature-0 question: a cache miss, so the provider
generates the answer at LLM speed. Round 2 asks the exact same question and
is served from the cache — same body, orders of magnitude faster, zero
provider tokens spent. Round 3 asks with default temperature to show the
cache correctly refusing to memorize random sampling. After each round the
gateway's /healthz shows the cache scoreboard (hits, misses, tokens saved),
and at the end the collector's /healthz proves the span trees and metric
samples for every request actually arrived at the backend.

Stdlib only — no dependencies.
"""

import json
import time
import urllib.error
import urllib.request

GATEWAY = "http://localhost:8090"
COLLECTOR = "http://localhost:9091"
QUESTION = "In one sentence, what does a write-ahead log do?"


def healthz(base: str) -> dict:
    try:
        with urllib.request.urlopen(base + "/healthz", timeout=5) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:  # 503 when degraded; body is still JSON
        return json.load(e)


def cache_line() -> str:
    c = healthz(GATEWAY).get("cache") or {}
    return (f"cache: entries={c.get('entries', 0)} hits={c.get('hits', 0)} "
            f"misses={c.get('misses', 0)} tokens_saved={c.get('tokens_saved', 0)}")


def chat(temperature):
    body = {
        "model": "llama3.2:1b",
        "messages": [{"role": "user", "content": QUESTION}],
        "max_tokens": 80,
    }
    if temperature is not None:
        body["temperature"] = temperature
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    start = time.monotonic()
    with urllib.request.urlopen(req, timeout=120) as resp:
        answer = json.load(resp)["choices"][0]["message"]["content"]
    return time.monotonic() - start, answer


def round_trip(label, temperature):
    elapsed, answer = chat(temperature)
    shown = answer if len(answer) <= 90 else answer[:87] + "..."
    print(f"{label}: {elapsed * 1000:7.1f} ms  {shown!r}")
    print(f"          {cache_line()}")
    return elapsed


def main() -> None:
    print(f"Question (temperature=0): {QUESTION!r}\n")
    miss = round_trip("round 1 (miss — provider generates)", 0)
    hit = round_trip("round 2 (hit  — served from cache) ", 0)
    if hit > 0:
        print(f"\nspeedup: {miss / hit:,.0f}x — the hit never touched the provider\n")
    round_trip("round 3 (default temperature — bypass)", None)

    # Telemetry is fire-and-forget and batched; give the last flush a beat.
    time.sleep(2)
    col = healthz(COLLECTOR)
    print(f"\ncollector received: {col.get('batches_received', 0)} batches, "
          f"{col.get('spans_received', 0)} spans, "
          f"{col.get('metrics_received', 0)} metric samples "
          f"({col.get('invalid_rejected', 0)} rejected)")
    tel = healthz(GATEWAY).get("telemetry") or {}
    print(f"gateway emitted:    {tel.get('spans_emitted', 0)} spans, "
          f"{tel.get('metrics_emitted', 0)} metrics in {tel.get('batches_sent', 0)} batches "
          f"(dropped={tel.get('dropped', 0)}, flush_errors={tel.get('flush_errors', 0)})")


if __name__ == "__main__":
    main()
