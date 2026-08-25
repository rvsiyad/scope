"""Live failover demo: watch the gateway route around a dead provider.

Run the gateway with a failover chain:

    SCOPE_OLLAMA_URLS=http://localhost:11434,http://localhost:11435 \
        go run ./cmd/gateway

then start this script and, while it loops, kill and revive the primary:

    docker compose stop ollama    # requests keep flowing via ollama-2
    docker compose start ollama   # probe closes the breaker, traffic returns

Stdlib only — no dependencies.
"""

import json
import time
import urllib.error
import urllib.request

GATEWAY = "http://localhost:8090"


def breakers() -> str:
    try:
        with urllib.request.urlopen(GATEWAY + "/healthz", timeout=5) as resp:
            health = json.load(resp)
    except urllib.error.HTTPError as e:  # 503 when degraded; body is still JSON
        health = json.load(e)
    except OSError as e:
        return f"healthz unreachable: {e}"
    return " ".join(f"{p['name']}={p['breaker']}" for p in health.get("providers", []))


def chat() -> str:
    body = json.dumps({
        "model": "llama3.2:1b",
        "messages": [{"role": "user", "content": "Say one short word."}],
        "max_tokens": 5,
    }).encode()
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions", data=body,
        headers={"Content-Type": "application/json"},
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            content = json.load(resp)["choices"][0]["message"]["content"]
            status = resp.status
    except urllib.error.HTTPError as e:
        status, content = e.code, json.load(e)["error"]["message"]
    except OSError as e:
        return f"gateway unreachable: {e}"
    ms = (time.monotonic() - start) * 1000
    return f"{status} {ms:6.0f}ms {content.strip()!r:24}"


def main() -> None:
    print(f"Looping requests against {GATEWAY} — Ctrl-C to stop.")
    print("Try: docker compose stop ollama   (then: docker compose start ollama)\n")
    while True:
        print(f"[{time.strftime('%H:%M:%S')}] {chat()}  breakers: {breakers()}")
        time.sleep(1)


if __name__ == "__main__":
    main()
