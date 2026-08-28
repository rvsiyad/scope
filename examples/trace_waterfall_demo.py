"""Live trace waterfall demo: send a request, then watch its span tree.

Run the collector, then the gateway pointed at it (a demo price makes cost
show up on the root span):

    go run ./cmd/collector
    SCOPE_COLLECTOR_URL=http://localhost:9091 \
    SCOPE_PRICE_PER_M_TOKENS=400 go run ./cmd/gateway

then:

    python3 examples/trace_waterfall_demo.py

The script sends one chat completion through the gateway, waits for the
emitter's batch to land, pulls the request log from the collector
(GET /v1/traces), takes the newest trace, and renders its waterfall
(GET /v1/traces/<id>) as ASCII: each phase span drawn as a bar positioned
and scaled by its real timings, with the token count and cost the request
actually incurred read straight off the root span's attrs. This is the
read path the phase-C UI will render — proven from the terminal first.

Stdlib only — no dependencies.
"""

import json
import time
import urllib.error
import urllib.request

GATEWAY = "http://localhost:8090"
COLLECTOR = "http://localhost:9091"
WIDTH = 58  # characters for the full trace duration


def chat() -> float:
    body = {
        "model": "llama3.2:1b",
        "messages": [{"role": "user", "content": "In one sentence, why do observability UIs draw waterfalls?"}],
        "max_tokens": 60,
    }
    req = urllib.request.Request(
        GATEWAY + "/v1/chat/completions", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    t0 = time.monotonic()
    with urllib.request.urlopen(req, timeout=120) as resp:
        json.load(resp)
    return (time.monotonic() - t0) * 1000


def newest_trace_id() -> str:
    # The emitter batches in the background; poll briefly until the trace
    # shows up in the request log.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with urllib.request.urlopen(COLLECTOR + "/v1/traces?limit=1", timeout=5) as resp:
            traces = json.load(resp)["traces"]
        if traces:
            return traces[0]["trace_id"]
        time.sleep(0.2)
    raise SystemExit("no trace arrived at the collector — is the gateway "
                     "running with SCOPE_COLLECTOR_URL set?")


def render(node, t0, total_ns, depth):
    left = int(WIDTH * (node["start"] - t0) / total_ns)
    bar = max(1, int(WIDTH * (node["end"] - node["start"]) / total_ns))
    label = ("  " * depth + node["name"]).ljust(16)[:16]
    print(f"  {label} |{' ' * left}{'█' * bar}{' ' * (WIDTH - left - bar)}| "
          f"{node['duration_ms']:9.2f} ms")
    for child in node.get("children") or []:
        render(child, t0, total_ns, depth + 1)


def main():
    print("sending one chat completion through the gateway...")
    wall = chat()
    print(f"  answered in {wall:.0f} ms end to end\n")

    trace_id = newest_trace_id()
    with urllib.request.urlopen(f"{COLLECTOR}/v1/traces/{trace_id}", timeout=5) as resp:
        waterfall = json.load(resp)

    roots = waterfall["roots"]
    t0 = min(r["start"] for r in roots)
    t1 = max(r["end"] for r in roots)
    print(f"trace {trace_id} — {waterfall['spans']} spans")
    for root in roots:
        render(root, t0, t1 - t0, 0)

    attrs = roots[0].get("attrs") or {}
    interesting = {k: attrs[k] for k in
                   ("tenant", "model", "provider", "outcome", "cache",
                    "tokens_total", "cost_usd") if k in attrs}
    if interesting:
        print("\n  root attrs: " +
              ", ".join(f"{k}={v}" for k, v in interesting.items()))


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        raise SystemExit(f"cannot reach the stack ({e}); start the collector "
                         "and the gateway as shown in the docstring") from e
