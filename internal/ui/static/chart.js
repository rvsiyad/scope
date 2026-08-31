// A hand-rolled SVG line chart — the one chart component the dashboard
// needs, so it is written here rather than imported: no chart library, no
// CDN, nothing to load at runtime. The collector serves everything the
// page uses. Marks stay thin (2px lines), grid and axes recessive, and the
// hover layer (crosshair + tooltip) is standard equipment: a dashboard
// whose exact numbers are unreadable is a poster, not a tool.

"use strict";

(function () {
  const SVG = "http://www.w3.org/2000/svg";
  const W = 460, H = 170;                       // viewBox; CSS scales it
  const PAD = { top: 8, right: 12, bottom: 20, left: 44 };

  function el(name, attrs) {
    const node = document.createElementNS(SVG, name);
    for (const k in attrs) node.setAttribute(k, attrs[k]);
    return node;
  }

  // One tooltip for the whole page — only one pointer exists.
  const tooltip = document.createElement("div");
  tooltip.className = "tooltip";
  tooltip.style.display = "none";
  document.body.appendChild(tooltip);

  // niceCeil rounds up to 1/2/5 × 10^k so the y axis lands on numbers a
  // human recognizes instead of the data's ragged maximum.
  function niceCeil(x) {
    if (!(x > 0)) return 1;
    const pow = Math.pow(10, Math.floor(Math.log10(x)));
    for (const m of [1, 2, 5, 10]) if (x <= m * pow) return m * pow;
    return 10 * pow;
  }

  function timeLabel(ms) {
    return new Date(ms).toLocaleTimeString([], {
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    });
  }

  // makeChart(container, {title, format}) -> {update(series)} where series
  // is [{name, color, points: [[tMillis, value], ...]}]. Points arrive on
  // the query engine's aligned step grid, so every series shares x
  // positions — which is what lets the crosshair read one timestamp across
  // all of them.
  window.makeChart = function (container, opts) {
    const panel = document.createElement("section");
    panel.className = "panel";
    if (opts.id) panel.id = "panel-" + opts.id; // lets the demo flash the panel an effect lands on
    const title = document.createElement("h2");
    title.textContent = opts.title;
    panel.appendChild(title);
    const body = document.createElement("div");
    panel.appendChild(body);
    const legend = document.createElement("div");
    legend.className = "legend";
    panel.appendChild(legend);
    container.appendChild(panel);

    const fmt = opts.format || ((v) => v.toFixed(2));
    let current = [];

    function showEmpty() {
      body.replaceChildren();
      const note = document.createElement("div");
      note.className = "empty";
      note.textContent = "no data yet: press Run demo above, or send a request through the gateway";
      body.appendChild(note);
      legend.replaceChildren();
    }

    function draw() {
      const series = current.filter((s) => s.points.length > 0);
      if (series.length === 0) return showEmpty();

      const svg = el("svg", { viewBox: `0 0 ${W} ${H}` });
      const x0 = PAD.left, x1 = W - PAD.right;
      const y0 = H - PAD.bottom, y1 = PAD.top;

      let tMin = Infinity, tMax = -Infinity, vMax = 0;
      for (const s of series) {
        for (const [t, v] of s.points) {
          if (t < tMin) tMin = t;
          if (t > tMax) tMax = t;
          if (v > vMax) vMax = v;
        }
      }
      if (tMax === tMin) tMax = tMin + 1;
      const yTop = opts.maxY || niceCeil(vMax);   // zero-based always: rates and latencies both lie when clipped
      const xOf = (t) => x0 + ((t - tMin) / (tMax - tMin)) * (x1 - x0);
      const yOf = (v) => y0 - (Math.min(v, yTop) / yTop) * (y0 - y1);

      // Grid + y ticks: 4 hairlines, labels in muted ink.
      for (let i = 0; i <= 4; i++) {
        const v = (yTop * i) / 4;
        const y = yOf(v);
        svg.appendChild(el("line", { x1: x0, x2: x1, y1: y, y2: y, class: i === 0 ? "axisline" : "gridline" }));
        const label = el("text", { x: x0 - 6, y: y + 3.5, "text-anchor": "end", class: "tick" });
        label.textContent = fmt(v, true);
        svg.appendChild(label);
      }
      // X ticks: first, middle, last of the window.
      for (const t of [tMin, (tMin + tMax) / 2, tMax]) {
        const anchor = t === tMin ? "start" : t === tMax ? "end" : "middle";
        const label = el("text", { x: xOf(t), y: H - 6, "text-anchor": anchor, class: "tick" });
        label.textContent = timeLabel(t);
        svg.appendChild(label);
      }

      for (const s of series) {
        const d = s.points
          .map(([t, v], i) => `${i ? "L" : "M"}${xOf(t).toFixed(1)},${yOf(v).toFixed(1)}`)
          .join("");
        svg.appendChild(el("path", { d, class: "series", stroke: s.color }));
      }

      // Hover: nearest shared timestamp -> crosshair, a dot per series,
      // and the tooltip listing every series' value at that instant.
      const hover = el("g", { style: "display:none" });
      const cross = el("line", { y1: y1, y2: y0, class: "crosshair" });
      hover.appendChild(cross);
      const dots = series.map((s) => {
        const dot = el("circle", { r: 3.5, fill: s.color, class: "hoverdot" });
        hover.appendChild(dot);
        return dot;
      });
      svg.appendChild(hover);

      const overlay = el("rect", {
        x: x0, y: y1, width: x1 - x0, height: y0 - y1,
        fill: "transparent",
      });
      overlay.addEventListener("mousemove", (ev) => {
        const rect = svg.getBoundingClientRect();
        const t = tMin + ((ev.clientX - rect.left) / rect.width * W - x0) / (x1 - x0) * (tMax - tMin);
        // All series share the step grid; index into the longest one.
        const ref = series.reduce((a, b) => (a.points.length >= b.points.length ? a : b));
        let best = 0;
        for (let i = 1; i < ref.points.length; i++) {
          if (Math.abs(ref.points[i][0] - t) < Math.abs(ref.points[best][0] - t)) best = i;
        }
        const ts = ref.points[best][0];
        cross.setAttribute("x1", xOf(ts));
        cross.setAttribute("x2", xOf(ts));
        const rows = [];
        series.forEach((s, i) => {
          const p = s.points.find(([pt]) => pt === ts);
          if (!p) { dots[i].style.display = "none"; return; }
          dots[i].style.display = "";
          dots[i].setAttribute("cx", xOf(p[0]));
          dots[i].setAttribute("cy", yOf(p[1]));
          rows.push({ name: s.name, color: s.color, v: p[1] });
        });
        hover.style.display = "";
        tooltip.replaceChildren();
        const when = document.createElement("div");
        when.className = "t";
        when.textContent = timeLabel(ts);
        tooltip.appendChild(when);
        for (const r of rows) {
          const row = document.createElement("div");
          row.className = "row";
          const chip = document.createElement("span");
          chip.className = "chip";
          chip.style.background = r.color;
          const name = document.createElement("span");
          name.textContent = r.name;
          const val = document.createElement("span");
          val.className = "v";
          val.textContent = fmt(r.v);
          row.append(chip, name, val);
          tooltip.appendChild(row);
        }
        tooltip.style.display = "";
        const tw = tooltip.offsetWidth;
        const flip = ev.clientX + 16 + tw > window.innerWidth;
        tooltip.style.left = (flip ? ev.clientX - 12 - tw : ev.clientX + 16) + "px";
        tooltip.style.top = ev.clientY + 12 + "px";
      });
      overlay.addEventListener("mouseleave", () => {
        hover.style.display = "none";
        tooltip.style.display = "none";
      });
      svg.appendChild(overlay);

      body.replaceChildren(svg);

      // Legend: always present for >=2 series; harmless for one.
      legend.replaceChildren();
      for (const s of series) {
        const item = document.createElement("span");
        item.className = "item";
        const chip = document.createElement("span");
        chip.className = "chip";
        chip.style.background = s.color;
        const name = document.createElement("span");
        name.textContent = s.name;
        item.append(chip, name);
        legend.appendChild(item);
      }
    }

    showEmpty();
    return {
      update(series) {
        current = series;
        draw();
      },
    };
  };
})();
