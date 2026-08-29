"""Live dashboard demo: the whole system on one screen.

Run the collector with a fast flush, then the gateway with two tenants and
a demo price so every panel has something to say:

    SCOPE_TSDB_FLUSH=15s SCOPE_TRACE_FLUSH=15s go run ./cmd/collector
    SCOPE_COLLECTOR_URL=http://localhost:9091 \
    SCOPE_TENANTS=acme:sk-acme:60000,globex:sk-globex:60000 \
    SCOPE_PRICE_PER_M_TOKENS=400 go run ./cmd/gateway

then open the dashboard and start the traffic:

    open http://localhost:9091/ui/
    python3 examples/dashboard_demo.py

The script plays a small production: two tenants asking questions through
the gateway for a few minutes — some repeated at temperature 0 (cache
hits), some fresh (misses, real Ollama generations), the occasional
oversized ask. Watch the request rate split by outcome, TTFT percentiles
spread as hits (microseconds to first byte) mix with misses (model speed),
spend accrue per tenant, and the cache hit rate climb as the question pool
starts repeating. Then switch to the Traces tab and click any request:
auth → reserve → cache lookup → provider call → settle, with tokens and
timings on every span — the trace store's whole reason to exist.

Stdlib only — no dependencies. Ctrl-C stops it early.
"""

import json
import random
import sys
import threading
import time
import urllib.error
import urllib.request

GATEWAY = "http://localhost:8090"
DURATION_S = 180
TENANTS = [("acme", "sk-acme"), ("globex", "sk-globex")]
QUESTIONS = [
    "In one sentence, what does a write-ahead log do?",
    "In one sentence, why do time series compress well?",
    "In one sentence, what is a circuit breaker for?",
    "In one sentence, what does an inverted index map?",
    "In one sentence, what is tail latency?",
    "In one sentence, why do observability UIs poll?",
]


def chat(api_key: str, question: str, cacheable: bool) -> str:
    body = {
        "model": "llama3.2:1b",
        "messages": [{"role": "user", "content": question}],
        "max_tokens": 60,
    }
    if cacheable:
        body["temperature"] = 0  # deterministic -> cache may serve it
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            json.load(resp)
            return "ok"
    except urllib.error.HTTPError as e:
        return f"http {e.code}"
    except OSError as e:
        return f"error ({e})"


def main() -> None:
    rng = random.Random()
    deadline = time.time() + DURATION_S
    counts: dict[str, int] = {}
    lock = threading.Lock()

    def one_request() -> None:
        tenant, key = rng.choice(TENANTS)
        # Weight toward the front of the pool so repeats (hence cache
        # hits) become common as the run goes on.
        question = QUESTIONS[min(rng.randrange(len(QUESTIONS)),
                                 rng.randrange(len(QUESTIONS)))]
        outcome = chat(key, question, cacheable=rng.random() < 0.7)
        with lock:
            counts[outcome] = counts.get(outcome, 0) + 1
            total = sum(counts.values())
        line = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"\r{total:4d} requests | {line}   ", end="", flush=True)

    print(f"driving traffic for {DURATION_S}s — watch http://localhost:9091/ui/")
    threads: list[threading.Thread] = []
    try:
        while time.time() < deadline:
            t = threading.Thread(target=one_request, daemon=True)
            t.start()
            threads.append(t)
            time.sleep(rng.uniform(0.4, 2.0))
    except KeyboardInterrupt:
        print("\nstopped early")
    for t in threads:
        t.join(timeout=5)
    print("\ndone — the dashboard keeps the last 15 minutes on screen")


if __name__ == "__main__":
    sys.exit(main())
