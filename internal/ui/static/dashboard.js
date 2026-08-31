// The dashboard page: five charts, each backed by /v1/query_range — the
// same PromQL-lite any client could send. The page POLLS on an interval
// rather than holding a push stream, which is how real observability UIs
// (Grafana included) work, for a load-bearing reason: a dashboard is a
// repeated *question* ("the last 15 minutes, as of now"), and re-asking a
// stateless query API is trivially resumable, cacheable, and cheap for the
// server to answer — where per-viewer push state is none of those. At a
// 5s interval the freshness difference is invisible to a human.

"use strict";

(function () {
  const WINDOW_MS = 15 * 60 * 1000; // the question: "the last 15 minutes"
  const STEP = "15s";               // 60 points per series per answer
  const POLL_MS = 5000;

  // Fixed categorical slot order (see style.css). Slots are handed to
  // series as they FIRST appear and the assignment is never revisited, so
  // a series keeps its color when the set around it changes — color
  // follows the entity, not its position in today's result.
  const SLOTS = ["--series-1", "--series-2", "--series-3", "--series-4",
    "--series-5", "--series-6", "--series-7", "--series-8"];
  const css = getComputedStyle(document.documentElement);
  const slotColor = (i) => css.getPropertyValue(SLOTS[Math.min(i, SLOTS.length - 1)]).trim();
  const assigned = new Map(); // chartId + series name -> slot index

  function colorFor(chartId, name) {
    const key = chartId + ":" + name;
    if (!assigned.has(key)) {
      let used = 0;
      for (const k of assigned.keys()) if (k.startsWith(chartId + ":")) used++;
      assigned.set(key, used);
    }
    return slotColor(assigned.get(key));
  }

  // legendName turns `gateway_requests_total{outcome="ok"}` into `ok`
  // (values of the grouped-by labels, joined). An unlabelled series gets
  // the chart's own fallback name.
  function legendName(seriesKey, fallback) {
    const brace = seriesKey.indexOf("{");
    const inner = brace >= 0 ? seriesKey.slice(brace + 1, -1) : "";
    const values = [];
    for (const m of inner.matchAll(/[a-zA-Z_][a-zA-Z0-9_]*=("(?:[^"\\]|\\.)*")/g)) {
      values.push(JSON.parse(m[1]));
    }
    return values.length ? values.join(" · ") : fallback;
  }

  async function queryRange(expr) {
    const end = Date.now();
    const url = "../v1/query_range?" + new URLSearchParams({
      query: expr, start: end - WINDOW_MS, end: end, step: STEP,
    });
    const res = await fetch(url);
    if (!res.ok) throw new Error(expr + ": " + (await res.text()));
    return (await res.json()).result;
  }

  const fmtNum = (v) => v >= 100 ? v.toFixed(0) : v >= 1 ? v.toFixed(1) : v.toFixed(2);
  const fmtMs = (v, axis) => axis ? fmtNum(v) : fmtNum(v) + " ms";
  const fmtUsd = (v) => "$" + (v >= 1 ? v.toFixed(2) : v.toFixed(4));
  const fmtPct = (v) => v.toFixed(0) + "%";

  // Each chart owns its questions. `series` maps one query's answer to
  // chart series; `derive` (cache hit rate) computes a series the query
  // language has no division for — done here, in the one consumer that
  // wants a ratio, rather than growing the engine for a percentage.
  const grid = document.getElementById("charts");
  const charts = [
    {
      id: "req",
      title: "Requests / s by outcome",
      format: fmtNum,
      query: 'sum by (outcome) (rate(gateway_requests_total[1m]))',
      fallback: "all",
    },
    {
      id: "ttft",
      title: "Time to first token (ms)",
      format: fmtMs,
      multi: [
        { name: "p50", expr: 'quantile_over_time(0.5, gateway_ttft_ms[1m])' },
        { name: "p95", expr: 'quantile_over_time(0.95, gateway_ttft_ms[1m])' },
        { name: "p99", expr: 'quantile_over_time(0.99, gateway_ttft_ms[1m])' },
      ],
    },
    {
      id: "tok",
      title: "Tokens / s by model",
      format: fmtNum,
      query: 'sum by (model) (rate(gateway_tokens_total[1m]))',
      fallback: "all models",
    },
    {
      id: "cost",
      title: "Spend ($ / min) by tenant",
      format: fmtUsd,
      query: 'sum by (tenant) (increase(gateway_cost_usd[1m]))',
      fallback: "all tenants",
    },
    {
      id: "cache",
      title: "Cache hit rate (%)",
      format: fmtPct,
      maxY: 100,
      query: 'sum by (cache) (rate(gateway_requests_total[1m]))',
      derive: (result) => {
        // hits / (hits + misses) per timestamp; bypasses (streaming,
        // uncacheable) are neither hits nor misses, so they stay out of
        // the denominator.
        const byCache = {};
        for (const s of result) {
          const kind = legendName(s.series, "");
          byCache[kind] = new Map(s.samples.map((p) => [p.t, p.v]));
        }
        const hits = byCache["hit"] || new Map();
        const misses = byCache["miss"] || new Map();
        const points = [];
        const times = new Set([...hits.keys(), ...misses.keys()]);
        for (const t of [...times].sort((a, b) => a - b)) {
          const h = hits.get(t) || 0, m = misses.get(t) || 0;
          if (h + m > 0) points.push([t, (100 * h) / (h + m)]);
        }
        return points.length ? [{ name: "hit rate", points }] : [];
      },
    },
  ];

  for (const c of charts) {
    c.chart = makeChart(grid, { id: c.id, title: c.title, format: c.format, maxY: c.maxY });
  }

  function toPoints(samples) {
    return samples.map((p) => [p.t, p.v]);
  }

  async function refreshChart(c) {
    if (c.multi) {
      const answers = await Promise.all(c.multi.map((q) => queryRange(q.expr)));
      c.chart.update(c.multi.flatMap((q, i) => {
        const s = answers[i][0]; // quantile_over_time collapses to one series
        return s ? [{ name: q.name, color: colorFor(c.id, q.name), points: toPoints(s.samples) }] : [];
      }));
      return;
    }
    const result = await queryRange(c.query);
    if (c.derive) {
      c.chart.update(c.derive(result).map((s) => ({ ...s, color: colorFor(c.id, s.name) })));
      return;
    }
    // Stable order: sort by series key so slot assignment on first sight
    // is deterministic run to run.
    result.sort((a, b) => (a.series < b.series ? -1 : 1));
    c.chart.update(result.map((s) => {
      const name = legendName(s.series, c.fallback);
      return { name, color: colorFor(c.id, name), points: toPoints(s.samples) };
    }));
  }

  const status = document.getElementById("status");
  const statusText = document.getElementById("status-text");

  async function refresh() {
    try {
      await Promise.all(charts.map(refreshChart));
      const health = await (await fetch("../healthz")).json();
      status.className = "status ok";
      statusText.textContent =
        `${health.metrics_received ?? 0} metrics · ${health.spans_received ?? 0} spans ingested`;
    } catch (err) {
      status.className = "status down";
      statusText.textContent = "collector unreachable";
      console.error(err);
    }
  }

  refresh();
  setInterval(refresh, POLL_MS);
})();
