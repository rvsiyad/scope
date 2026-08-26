"""Live token-budget demo: greedy concurrent clients can't overrun a budget.

Run the gateway with two tenants — one on a tight budget, one comfortable:

    SCOPE_TENANTS=acme:sk-acme:300,globex:sk-globex:60000 go run ./cmd/gateway

then:

    python3 examples/budget_demo.py

Four "acme" clients fire streaming requests at once, each asking for up to
100 tokens. Admission reserves the estimated cost up front, so only what fits
in acme's 300-token budget gets in; the rest are rejected with 429 and an
honest Retry-After. Settle then trues each admitted request up to the
provider's real count. Meanwhile a "globex" request sails through — budgets
are per-tenant. Watch the budgets refill between rounds in /healthz.

Stdlib only — no dependencies.
"""

import json
import threading
import time
import urllib.error
import urllib.request

GATEWAY = "http://localhost:8090"
ACME, GLOBEX = "sk-acme", "sk-globex"


def budgets() -> str:
    try:
        with urllib.request.urlopen(GATEWAY + "/healthz", timeout=5) as resp:
            health = json.load(resp)
    except urllib.error.HTTPError as e:  # 503 when degraded; body is still JSON
        health = json.load(e)
    except OSError as e:
        return f"healthz unreachable: {e}"
    return " | ".join(
        f"{b['name']}: {b['available_tokens']}/{b['capacity_tokens']} tokens, "
        f"admitted={b['admitted']} rejected={b['rejected']} charged={b['tokens_charged']}"
        for b in health.get("budgets", [])
    )


def chat(api_key: str) -> str:
    body = json.dumps({
        "model": "llama3.2:1b",
        "messages": [{"role": "user", "content": "Write one short sentence about weather."}],
        "max_tokens": 100,
        "stream": True,
    }).encode()
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions", data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            chunks = 0
            for line in resp:
                if line.startswith(b"data: ") and not line.startswith(b"data: [DONE]"):
                    chunks += 1
            ms = (time.monotonic() - start) * 1000
            return f"200 admitted, {chunks} chunks streamed in {ms:.0f}ms"
    except urllib.error.HTTPError as e:
        detail = json.load(e)["error"].get("code", "?")
        retry = e.headers.get("Retry-After")
        suffix = f", Retry-After: {retry}s" if retry else ""
        return f"{e.code} rejected ({detail}{suffix})"
    except OSError as e:
        return f"unreachable: {e}"


def round_of_greedy_clients(n: int) -> None:
    results: list[str] = [""] * n
    def worker(i: int) -> None:
        results[i] = chat(ACME)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i, r in enumerate(results):
        print(f"  acme client {i + 1}: {r}")


def main() -> None:
    print(f"budgets: {budgets()}\n")

    print("round 1 — four greedy acme clients at once:")
    round_of_greedy_clients(4)
    print(f"  globex (own budget): {chat(GLOBEX)}")
    print(f"  budgets: {budgets()}\n")

    print("round 2 — immediately again (acme should mostly bounce):")
    round_of_greedy_clients(4)
    print(f"  budgets: {budgets()}\n")

    wait = 30
    print(f"waiting {wait}s for refill...")
    time.sleep(wait)
    print(f"  budgets: {budgets()}\n")

    print("round 3 — after refill:")
    round_of_greedy_clients(2)
    print(f"  budgets: {budgets()}")


if __name__ == "__main__":
    main()
