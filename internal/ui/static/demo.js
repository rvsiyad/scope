// The guided demo: a scripted run of real requests through the gateway,
// narrated step by step on the dashboard page. Nothing is staged — every
// request takes the same auth → rate-limit → cache → provider path any
// OpenAI SDK client would, so the tour doubles as proof the pipeline works
// end to end: the charts move because the collector really ingested the
// telemetry, and the final step links to the actual trace of the first
// request.

"use strict";

(function () {
  const MODEL = "llama3.2:1b";     // what deploy/setup.sh pulls
  const KEY = "sk-scope-demo";     // the published demo tenant; open mode ignores it
  const STEPS = 5;

  const $ = (id) => document.getElementById(id);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  // The UI is served by the collector, so the gateway is another origin by
  // construction. Guess it from where we are — localhost and raw IPs keep
  // the default port, a deployed hostname gets an api. prefix
  // (scope.example → api.scope.example) — and let ?gateway=<url> override,
  // remembered so the override survives navigation.
  function gatewayBase() {
    const override = new URLSearchParams(location.search).get("gateway");
    if (override) {
      try { localStorage.setItem("scope-gateway", override); } catch (e) { /* private mode */ }
    }
    let saved = null;
    try { saved = localStorage.getItem("scope-gateway"); } catch (e) { /* ditto */ }
    const host = location.hostname;
    const base = saved
      || (host === "localhost" || /^[\d.]+$/.test(host)
        ? "http://" + host + ":8090"
        : location.protocol + "//api." + location.host);
    return base.replace(/\/+$/, "");
  }
  const GW = gatewayBase();

  function step(n, main, sub) {
    $("demo-dots").textContent = "●".repeat(n) + "○".repeat(STEPS - n);
    $("demo-step").textContent = main;
    $("demo-sub").replaceChildren(sub || "");
  }

  // flash draws the eye to the panel where the last action's effect lands.
  function flash(...chartIds) {
    for (const id of chartIds) {
      const panel = $("panel-" + id);
      if (!panel) continue;
      panel.classList.add("flash");
      setTimeout(() => panel.classList.remove("flash"), 1600);
    }
  }

  function traceLink(traceId, label) {
    const a = document.createElement("a");
    a.href = "traces.html?trace=" + encodeURIComponent(traceId);
    a.textContent = label;
    return a;
  }

  // chat sends one real completion through the gateway. Streaming responses
  // are read chunk by chunk off the SSE body so the reply can be shown
  // arriving token by token — and so TTFT is measured here the way a user
  // feels it, from send to first token.
  async function chat(prompt, { stream, maxTokens, onToken } = {}) {
    const sent = performance.now();
    const res = await fetch(GW + "/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + KEY },
      body: JSON.stringify({
        model: MODEL,
        messages: [{ role: "user", content: prompt }],
        temperature: 0,           // pinned: only deterministic requests are cacheable
        max_tokens: maxTokens || 48,
        stream: !!stream,
      }),
    });
    const traceId = res.headers.get("X-Scope-Trace-Id");
    if (!res.ok) {
      let msg = "gateway returned " + res.status;
      try { msg = (await res.json()).error.message; } catch (e) { /* keep the status */ }
      const err = new Error(msg);
      err.status = res.status;
      err.retryAfter = res.headers.get("Retry-After");
      throw err;
    }
    if (!stream) {
      await res.json();
      return { traceId, ms: performance.now() - sent };
    }
    let ttft = 0;
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split("\n");
      buf = lines.pop(); // an incomplete line waits for the next chunk
      for (const line of lines) {
        if (!line.startsWith("data: ") || line === "data: [DONE]") continue;
        const delta = JSON.parse(line.slice(6)).choices?.[0]?.delta?.content;
        if (!delta) continue;
        if (!ttft) ttft = performance.now() - sent;
        if (onToken) onToken(delta);
      }
    }
    return { traceId, ttft, ms: performance.now() - sent };
  }

  const fmtMs = (ms) => ms >= 1000 ? (ms / 1000).toFixed(1) + " s" : Math.round(ms) + " ms";

  async function runDemo() {
    // A per-run tag keeps the story identical on every run: step 1 is
    // always a cache miss (nobody has asked this exact question before),
    // which makes step 2 — the same question again — always a hit.
    const tag = Date.now().toString(36);
    const ask = "In one short sentence, what does an LLM gateway do? (run " + tag + ")";

    step(1, "Sending one real request through the gateway. The reply streams in below as the model writes it.",
      "same path as any OpenAI SDK: auth, token budget check, cache lookup (a miss, this question is new), provider");
    const out = $("demo-out");
    out.hidden = false;
    out.textContent = "";
    const first = await chat(ask, { stream: true, onToken: (t) => { out.textContent += t; } });
    step(1, "The model answered. That " + fmtMs(first.ttft) + " wait for the first token is what the TTFT chart measures.",
      "the gateway measured it too; within a few seconds it shows up on the TTFT and requests/s charts");
    flash("req", "ttft");
    await sleep(5000);

    step(2, "Now the exact same question again.",
      "deterministic requests (temperature 0) are cacheable, and the reply above is already in the response cache");
    const again = await chat(ask, { stream: false });
    step(2, "Answered from the cache in " + fmtMs(again.ms) + ". No provider call, no tokens billed.",
      "a hit costs nothing on purpose: budgets exist because provider tokens cost money, and a hit uses none");
    flash("cache");
    await sleep(5000);

    step(3, "A short burst of fresh requests, so the rate charts have something to show.",
      "sending 1 of 3…");
    for (let i = 1; i <= 3; i++) {
      $("demo-sub").textContent = "sending " + i + " of 3… (a 1b model on a free-tier CPU takes its time)";
      await chat("Reply with only the word ping. (burst " + tag + "-" + i + ")", { stream: false, maxTokens: 16 });
    }
    step(3, "Sent. Watch requests/s, tokens/s, and spend tick up on the next poll.",
      "the $/min panel is the demo tenant paying a configured per-token price, so the cost pipeline has numbers");
    flash("req", "tok", "cost");
    await sleep(6000);

    step(4, "Every request you just sent left a full trace in the trace store.");
    const sub = $("demo-sub");
    sub.replaceChildren(
      traceLink(first.traceId, "Open the waterfall for the first request"),
      ": auth, cache lookup, budget reserve, provider, and settle, each phase timed"
    );
    await sleep(8000);

    step(5, "Done. Full path: gateway, write-ahead log, time-series and trace stores, then the queries behind these charts.",
      "");
    $("demo-sub").replaceChildren(
      "Try it yourself: point any OpenAI SDK at " + GW + "/v1 with API key " + KEY + ", or ",
      traceLink(first.traceId, "look at your demo run's trace"),
      "."
    );
  }

  $("demo-run").addEventListener("click", async () => {
    const btn = $("demo-run");
    btn.disabled = true;
    btn.textContent = "Running…";
    try {
      // Reachability first: a wrong gateway guess should fail with advice,
      // not with a bare TypeError after the first step.
      await fetch(GW + "/healthz").catch(() => {
        throw Object.assign(new Error("no gateway at " + GW), { unreachable: true });
      });
      await runDemo();
    } catch (err) {
      if (err.status === 429) {
        step(0, "The rate limiter stepped in: the demo tenant's shared token budget is spent.",
          "that budget is the guardrail on a deliberately public API key; try again in "
          + (err.retryAfter ? err.retryAfter + " s" : "a minute"));
      } else if (err.unreachable) {
        step(0, err.message, "the dashboards read the collector, which is up; the gateway is a second service, "
          + "point the demo at yours with ?gateway=<url>");
      } else {
        step(0, "The demo hit a snag: " + err.message,
          "the dashboards themselves are fine, the charts keep polling the collector");
      }
    }
    btn.textContent = "Run again";
    btn.disabled = false;
  });
})();
