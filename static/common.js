/* Helper umum + grafik SVG tanpa library (tetap jalan saat demo offline). */
const $ = (s, r = document) => r.querySelector(s);
const NS = "http://www.w3.org/2000/svg";
const C = { member: "#1b5fc1", casual: "#e8590c", grid: "#e1e7ef", ink: "#0e1a2b", muted: "#5b6878", green: "#1e8e5a" };

const fmt = (n, d = 0) => (n == null || isNaN(n) ? "–" : Number(n).toLocaleString("id-ID", { maximumFractionDigits: d, minimumFractionDigits: d }));
const pct = (x, d = 0) => (x == null ? "–" : fmt(x * 100, d) + "%");
const bytes = (b) => (b > 1e6 ? fmt(b / 1e6, 1) + " MB" : b > 1e3 ? fmt(b / 1e3, 0) + " KB" : b + " B");
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

async function api(url, opts) {
  const r = await fetch(url, opts);
  let j = {};
  try { j = await r.json(); } catch (e) { /* bukan JSON */ }
  if (!r.ok) throw new Error(j.error || "Permintaan gagal (" + r.status + ")");
  return j;
}

/* status di jalur navigasi */
async function refreshLine() {
  try {
    const s = await api("/api/pipeline/status");
    const set = (id, cls, text) => { const e = $("#stop-" + id); if (!e) return; e.classList.remove("done", "warn"); if (cls) e.classList.add(cls); $("small", e).textContent = text; };
    set("input", s.clean.length ? "done" : "", s.clean.length ? s.clean.length + " file bersih" : "Belum ada data");
    set("pipeline", s.hdfs.ok ? (s.hdfs.files.length ? "done" : "warn") : "warn",
      s.hdfs.ok ? "HDFS aktif, " + s.hdfs.files.length + " file" : "HDFS belum terhubung");
    set("dashboard", s.analysis.has_results ? "done" : "", s.analysis.has_results ? "Hasil siap" : "Belum dianalisis");
  } catch (e) { /* abaikan */ }
}
document.addEventListener("DOMContentLoaded", refreshLine);

/* ---------------- grafik ---------------- */
function mk(tag, attrs = {}, text) {
  const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (text != null) e.textContent = text;
  return e;
}
function niceMax(v) {
  if (v <= 0) return 1;
  const p = Math.pow(10, Math.floor(Math.log10(v))), f = v / p;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p;
}

/* line chart. series: [{name,color,values(null=putus),dash}] */
function lineChart(host, { labels, series, yFmt = (v) => fmt(v), xEvery = 1, height = 260, yMin = 0, zeroLine = false }) {
  host.innerHTML = "";
  const W = 720, H = height, m = { l: 52, r: 12, t: 10, b: 28 };
  const svg = mk("svg", { viewBox: `0 0 ${W} ${H}`, class: "chart", role: "img" });
  const all = series.flatMap((s) => s.values).filter((v) => v != null);
  const mn = Math.min(...all), mx = Math.max(...all);
  const lo2 = yMin === "auto" ? (mn < 0 ? -niceMax(-mn) : 0) : yMin;
  const hi = niceMax(Math.max(mx, 0.0001));
  const x = (i) => m.l + (labels.length === 1 ? 0 : (i * (W - m.l - m.r)) / (labels.length - 1));
  const y = (v) => m.t + (1 - (v - lo2) / (hi - lo2)) * (H - m.t - m.b);
  for (let k = 0; k <= 4; k++) {
    const v = lo2 + ((hi - lo2) * k) / 4;
    svg.append(mk("line", { x1: m.l, x2: W - m.r, y1: y(v), y2: y(v), stroke: C.grid }));
    svg.append(mk("text", { x: m.l - 8, y: y(v) + 4, "text-anchor": "end" }, yFmt(v)));
  }
  if (zeroLine && lo2 < 0) svg.append(mk("line", { x1: m.l, x2: W - m.r, y1: y(0), y2: y(0), stroke: "#9aa7b8" }));
  labels.forEach((l, i) => { if (i % xEvery === 0) svg.append(mk("text", { x: x(i), y: H - 8, "text-anchor": "middle" }, l)); });
  series.forEach((s) => {
    let d = "", pen = false;
    s.values.forEach((v, i) => { if (v == null) { pen = false; return; } d += (pen ? "L" : "M") + x(i).toFixed(1) + " " + y(v).toFixed(1); pen = true; });
    if (s.band) {
      const up = s.band.map((b, i) => [x(i), y(b[1])]), lw = s.band.map((b, i) => [x(i), y(b[0])]).reverse();
      svg.append(mk("path", { d: "M" + up.concat(lw).map((p) => p.join(" ")).join("L") + "Z", fill: s.color, opacity: ".14" }));
    }
    svg.append(mk("path", { d, fill: "none", stroke: s.color, "stroke-width": 2.4, "stroke-linejoin": "round", "stroke-dasharray": s.dash || "" }));
    if (s.dots) s.values.forEach((v, i) => { if (v != null) { const c = mk("circle", { cx: x(i), cy: y(v), r: 3.5, fill: s.color }); c.append(mk("title", {}, `${s.name}, ${labels[i]}: ${yFmt(v)}`)); svg.append(c); } });
  });
  host.append(svg);
}

/* bar vertikal. items: [{label,value,color}] */
function barChart(host, { items, yFmt = (v) => fmt(v), height = 240, zero = true }) {
  host.innerHTML = "";
  const W = 720, H = height, m = { l: 52, r: 12, t: 10, b: 28 };
  const svg = mk("svg", { viewBox: `0 0 ${W} ${H}`, class: "chart", role: "img" });
  const hi = niceMax(Math.max(...items.map((i) => i.value), 0.0001));
  const y = (v) => m.t + (1 - v / hi) * (H - m.t - m.b);
  for (let k = 0; k <= 4; k++) {
    const v = (hi * k) / 4;
    svg.append(mk("line", { x1: m.l, x2: W - m.r, y1: y(v), y2: y(v), stroke: C.grid }));
    svg.append(mk("text", { x: m.l - 8, y: y(v) + 4, "text-anchor": "end" }, yFmt(v)));
  }
  const bw = (W - m.l - m.r) / items.length;
  items.forEach((it, i) => {
    const r = mk("rect", { x: m.l + i * bw + bw * 0.18, y: y(it.value), width: bw * 0.64, height: H - m.b - y(it.value), fill: it.color || C.member, rx: 3 });
    r.append(mk("title", {}, `${it.label}: ${yFmt(it.value)}`));
    svg.append(r, mk("text", { x: m.l + i * bw + bw / 2, y: H - 8, "text-anchor": "middle" }, it.label));
  });
  host.append(svg);
}

/* bar horizontal HTML. items: [{label,value,color}] ; negatif = ke kiri dari tengah */
function hbars(host, items, { f = (v) => fmt(v, 1), diverging = false } = {}) {
  host.innerHTML = "";
  const max = Math.max(...items.map((i) => Math.abs(i.value)), 0.0001);
  items.forEach((it) => {
    const row = document.createElement("div");
    row.className = "hb";
    const w = (Math.abs(it.value) / max) * (diverging ? 50 : 100);
    const style = diverging
      ? `width:${w}%;${it.value >= 0 ? "left:50%" : `left:${50 - w}%`};background:${it.color || (it.value >= 0 ? C.casual : C.member)}`
      : `left:0;width:${w}%;background:${it.color || C.member}`;
    row.innerHTML = `<div class="l" title="${esc(it.label)}">${esc(it.label)}</div><div class="tr"><i style="${style}"></i></div><div class="v">${f(it.value)}</div>`;
    host.append(row);
  });
}
