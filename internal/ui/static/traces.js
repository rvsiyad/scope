// The traces page: the request log (/v1/traces, newest first) and the
// span waterfall (/v1/traces/{id}) — the read the trace store exists for.
// The waterfall needs no chart machinery: each span is a horizontal bar
// positioned and sized as a PERCENTAGE of the whole trace's wall time, so
// the layout is three divs and arithmetic — the trace's own structure
// (parent/child nesting, sorted starts) arrives pre-built from the API.

"use strict";

(function () {
  const POLL_MS = 5000;
  const LIMIT = 50;

  // Same fixed-slot color discipline as the dashboard: a phase name gets
  // a slot the first time it appears and keeps it — auth is always the
  // same color, in every trace, all session.
  const SLOTS = ["--series-1", "--series-2", "--series-3", "--series-4",
    "--series-5", "--series-6", "--series-7", "--series-8"];
  const css = getComputedStyle(document.documentElement);
  const assigned = new Map();
  function colorFor(name) {
    if (!assigned.has(name)) assigned.set(name, assigned.size);
    return css.getPropertyValue(SLOTS[assigned.get(name) % SLOTS.length]).trim();
  }

  const fmtMs = (ms) => (ms >= 100 ? ms.toFixed(0) : ms >= 10 ? ms.toFixed(1) : ms.toFixed(2)) + " ms";
  const fmtTime = (ns) => new Date(ns / 1e6).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  });

  const log = document.getElementById("log");
  const wfPanel = document.getElementById("waterfall-panel");
  const wfTitle = document.getElementById("waterfall-title");
  const wf = document.getElementById("waterfall");
  let selected = null;

  function cell(text, cls) {
    const td = document.createElement("td");
    if (cls) td.className = cls;
    td.textContent = text;
    return td;
  }

  function renderLog(traces) {
    const table = document.createElement("table");
    const head = document.createElement("tr");
    for (const h of ["time", "trace", "tenant", "model", "cache", "outcome", "tokens", "duration", "spans"]) {
      const th = document.createElement("th");
      th.textContent = h;
      head.appendChild(th);
    }
    table.appendChild(head);
    for (const t of traces) {
      const a = t.attrs || {};
      const row = document.createElement("tr");
      row.className = "req" + (t.trace_id === selected ? " selected" : "");
      row.append(
        cell(fmtTime(t.start)),
        cell(t.trace_id.slice(0, 8), "mono"),
        cell(a.tenant || "—"),
        cell(a.model || "—"),
        cell(a.cache || "—"),
        cell(a.outcome || "—", a.outcome === "error" ? "bad" : ""),
        cell(a.tokens_total || "—", "num"),
        cell(fmtMs(t.duration_ms), "num"),
        cell(String(t.spans), "num"),
      );
      row.addEventListener("click", () => select(t.trace_id));
      table.appendChild(row);
    }
    log.replaceChildren(table);
    if (traces.length === 0) {
      const note = document.createElement("div");
      note.className = "empty";
      note.textContent = "no traces yet: run the demo on the dashboards page, or send a request through the gateway";
      log.replaceChildren(note);
    }
  }

  // One waterfall row per span, depth-first so children sit under their
  // parent; the bar's offset and width are percentages of the root span's
  // wall time. Attrs (tenant, tokens, cost) ride in the row's title.
  function renderWaterfall(trace) {
    wfPanel.hidden = false;
    wfTitle.textContent = `Trace ${trace.trace_id} — ${trace.spans} spans`;
    let t0 = Infinity, t1 = -Infinity;
    (function span(nodes) {
      for (const n of nodes) {
        if (n.start < t0) t0 = n.start;
        if (n.end > t1) t1 = n.end;
        span(n.children || []);
      }
    })(trace.roots);
    const total = Math.max(t1 - t0, 1);

    const rows = [];
    (function walk(nodes, depth) {
      for (const n of nodes) {
        rows.push({ n, depth });
        walk(n.children || [], depth + 1);
      }
    })(trace.roots, 0);

    wf.replaceChildren();
    for (const { n, depth } of rows) {
      const row = document.createElement("div");
      row.className = "wf-row";
      if (n.attrs) {
        row.title = Object.entries(n.attrs).map(([k, v]) => `${k}=${v}`).join("\n");
      }
      const name = document.createElement("div");
      name.className = "wf-name";
      name.style.paddingLeft = depth * 16 + "px";
      name.textContent = n.name;
      const track = document.createElement("div");
      track.className = "wf-track";
      const bar = document.createElement("div");
      bar.className = "wf-bar";
      bar.style.left = ((n.start - t0) / total * 100).toFixed(2) + "%";
      bar.style.width = Math.max((n.end - n.start) / total * 100, 0.4).toFixed(2) + "%";
      bar.style.background = colorFor(n.name);
      const dur = document.createElement("span");
      dur.className = "wf-dur";
      dur.textContent = fmtMs(n.duration_ms);
      // The label sits after the bar unless the bar reaches the right
      // edge, where it would overflow the track — then it sits inside.
      if ((n.end - t0) / total > 0.85) {
        dur.classList.add("inside");
        bar.appendChild(dur);
        track.appendChild(bar);
      } else {
        track.append(bar, dur);
        dur.style.left = ((n.end - t0) / total * 100).toFixed(2) + "%";
      }
      row.append(name, track);
      wf.appendChild(row);
    }
  }

  async function select(id) {
    selected = id;
    const res = await fetch("../v1/traces/" + encodeURIComponent(id));
    if (!res.ok) return;
    renderWaterfall(await res.json());
    for (const row of log.querySelectorAll("tr.req")) row.classList.remove("selected");
    refresh(); // repaint the selection highlight promptly
    wfPanel.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  const status = document.getElementById("status");
  const statusText = document.getElementById("status-text");

  async function refresh() {
    try {
      const res = await fetch("../v1/traces?limit=" + LIMIT);
      if (!res.ok) throw new Error(await res.text());
      renderLog((await res.json()).traces);
      const health = await (await fetch("../healthz")).json();
      status.className = "status ok";
      const tr = health.traces || {};
      statusText.textContent = `${(tr.head_traces ?? 0)} traces in head · ${(tr.segments ?? 0)} segments`;
    } catch (err) {
      status.className = "status down";
      statusText.textContent = "collector unreachable";
      console.error(err);
    }
  }

  // ?trace=<id> deep-links straight to one waterfall — how the demo (or
  // anything holding an X-Scope-Trace-Id) hands off to this page.
  const preselect = new URLSearchParams(location.search).get("trace");
  if (preselect) select(preselect);

  refresh();
  setInterval(refresh, POLL_MS);
})();
