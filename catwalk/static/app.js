/* Catwalk frontend — vanilla JS, no build step, no external deps. */
"use strict";

const $ = (id) => document.getElementById(id);

const state = {
  path: "/",
  page: 1,
  pageSize: 20,
  sort: "name",
  order: "asc",
  type: "all",
  nameFilter: "",
};

let listing = null;            // last successfully rendered /api/ls response
const pageCache = new Map();   // listingKey+page -> response (client prefetch)
const PAGE_CACHE_MAX = 50;
let listingToken = 0;          // guards against stale listing renders
let forwardStreak = 0;
const rollups = new Map();     // path -> {children: {name -> stats}, totals}
let rollupToken = 0;           // guards against stale poll renders
let activeRollupJobId = null;
let activeRollupCancelToken = null;
let rollupInFlight = null;     // path whose rollup is being computed, else null

// loadPage outcomes: a superseded load must not touch the UI (a newer load
// owns it), while a failed load must leave honestly empty panels behind.
const LOAD_OK = "ok";
const LOAD_FAILED = "failed";
const LOAD_SUPERSEDED = "superseded";
const treeNodes = new Map();   // path -> {li, childrenUl, expanded}
let treeRoot = "/";

/* ---------- helpers ---------- */

async function api(path, params, options = {}) {
  const url = new URL(path, location.origin);
  for (const [k, v] of Object.entries(params || {})) {
    if (v !== "" && v !== null && v !== undefined) url.searchParams.set(k, v);
  }
  const res = await fetch(url, options);
  const body = await res.json().catch(() => ({}));
  if (!res.ok && res.status !== 202) {
    const err = new Error(body.detail || `${res.status} ${res.statusText}`);
    err.status = res.status;
    throw err;
  }
  return { status: res.status, body };
}

function humanSize(n) {
  if (n === null || n === undefined) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = n;
  for (const u of units) {
    if (v < 1024 || u === "PiB") {
      return u === "B" ? `${v} B` : `${v.toFixed(1)} ${u}`;
    }
    v /= 1024;
  }
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso.slice(0, 23) + "Z"); // backend emits UTC, no suffix
  return isNaN(d) ? iso : d.toLocaleString();
}

function normDir(p) {
  p = "/" + (p || "").replace(/^\/+|\/+$/g, "");
  return p === "/" ? "/" : p + "/";
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function listingKey() {
  return JSON.stringify([state.path, state.sort, state.order, state.type,
                         state.nameFilter, state.pageSize]);
}

/* ---------- header: views, breadcrumb, freshness, theme ---------- */

async function loadViews() {
  try {
    const { body } = await api("/api/views");
    const sel = $("view-select");
    for (const v of body.views || []) {
      const o = document.createElement("option");
      o.value = v.path;
      o.textContent = `${v.name}  (${v.path})`;
      sel.appendChild(o);
    }
    if (body.vms_unavailable) {
      const o = document.createElement("option");
      o.disabled = true;
      o.textContent = "VMS unavailable — type a path instead";
      sel.appendChild(o);
    }
  } catch (e) { /* view browsing is optional */ }
}

async function loadHealth() {
  try {
    const { body } = await api("/api/health");
    const bits = [body.snapshot_hint];
    if (body.mode === "mock") bits.push("· MOCK DATA");
    if (!body.catalog_reachable) {
      showBanner("VAST Catalog is not reachable on this cluster — " +
                 (body.vastdb || "check /api/health"));
    }
    $("freshness").textContent = bits.join(" ");
  } catch (e) {
    $("freshness").textContent = "backend unreachable";
  }
}

function renderBreadcrumb() {
  const bc = $("breadcrumb");
  bc.textContent = "";
  const parts = state.path.split("/").filter(Boolean);
  const rootLink = el("a", "", "/");
  rootLink.onclick = () => navigate("/");
  bc.appendChild(rootLink);
  let acc = "";
  parts.forEach((part, i) => {
    acc += "/" + part;
    const target = acc + "/";
    const link = el("a", "", part);
    link.onclick = () => navigate(target);
    bc.appendChild(link);
    if (i < parts.length - 1) bc.appendChild(el("span", "sep", "/"));
  });
}

/* The breadcrumb doubles as the free-form path input (essential when VMS is
   absent and there are no views to pick from): segments are links, clicking
   the empty space swaps in an editable field. Enter navigates; Esc or
   clicking away restores the breadcrumb. */
function editPath() {
  const bc = $("breadcrumb");
  if (bc.querySelector("input")) return;
  bc.textContent = "";
  const input = document.createElement("input");
  input.type = "text";
  input.value = state.path;
  input.placeholder = "/path/to/browse";
  input.spellcheck = false;
  input.autocomplete = "off";
  bc.appendChild(input);
  input.focus();
  input.select();
  let finished = false;
  const done = (commit) => {
    if (finished) return;                     // blur fires again on removal
    finished = true;
    const value = input.value;
    renderBreadcrumb();
    if (commit && value.trim()) {
      const p = normDir(value);
      rebuildTree(p);
      navigate(p);
    }
  };
  input.onkeydown = (e) => {
    if (e.key === "Enter") { e.preventDefault(); done(true); }
    else if (e.key === "Escape") done(false);
  };
  input.onblur = () => done(false);
}

function showBanner(msg) {
  $("banner").textContent = msg;
  $("banner").classList.remove("hidden");
}
function hideBanner() { $("banner").classList.add("hidden"); }

function initTheme() {
  const saved = localStorage.getItem("catwalk-theme");
  if (saved) document.documentElement.dataset.theme = saved;
  $("theme-toggle").onclick = () => {
    const cur = document.documentElement.dataset.theme === "light" ? "dark" : "light";
    document.documentElement.dataset.theme = cur;
    localStorage.setItem("catwalk-theme", cur);
  };
}

/* ---------- listing table + pager + prefetch ---------- */

async function fetchPage(page) {
  const key = listingKey() + "#" + page;
  if (pageCache.has(key)) return pageCache.get(key);
  const promise = api("/api/ls", {
    path: state.path, page, page_size: state.pageSize, sort: state.sort,
    order: state.order, type: state.type, name_filter: state.nameFilter,
  }).then(r => r.body);
  pageCache.set(key, promise);   // cache the promise: dedup in-flight fetches
  while (pageCache.size > PAGE_CACHE_MAX) {
    pageCache.delete(pageCache.keys().next().value);
  }
  try {
    return await promise;
  } catch (e) {
    if (pageCache.get(key) === promise) pageCache.delete(key);
    throw e;
  }
}

async function loadPage(page, { fromNav = false } = {}) {
  const token = ++listingToken;
  const expectedKey = listingKey();
  let data;
  try {
    data = await fetchPage(page);
  } catch (e) {
    if (token !== listingToken) return LOAD_SUPERSEDED; // a newer load owns the UI
    showBanner(`listing failed: ${e.message}`);
    clearListingUI();
    return LOAD_FAILED;
  }
  if (token !== listingToken || expectedKey !== listingKey()) return LOAD_SUPERSEDED;
  listing = data;
  state.page = data.page;
  renderTable(data);
  renderPager(data);
  if (data.truncated) {
    showBanner(`showing first ${data.total_rows.toLocaleString()} entries — ` +
               "narrow with a name or type filter to see the rest");
  } else if (!fromNav) {
    hideBanner();
  }
  // Transparent prefetch: next page always, +2 on a forward streak.
  if (data.page < data.pages) fetchPage(data.page + 1).catch(() => {});
  if (forwardStreak >= 2 && data.page + 2 <= data.pages) {
    fetchPage(data.page + 2).catch(() => {});
  }
  updateExportLinks();
  return LOAD_OK;
}

function renderTable(data) {
  const tbody = $("table-body");
  tbody.textContent = "";
  $("table-empty").classList.toggle("hidden", data.entries.length > 0);
  const rollup = rollups.get(state.path);

  for (const e of data.entries) {
    const tr = document.createElement("tr");
    const isDir = e.element_type === "DIR";
    if (isDir) tr.className = "dir";

    const name = el("td", "name", e.name);
    name.title = e.name;
    if (isDir) name.onclick = () => navigate(data.path + e.name + "/");
    tr.appendChild(name);

    tr.appendChild(el("td", "etype", e.element_type.toLowerCase()));

    const size = el("td", "num", isDir ? "—" : humanSize(e.size));
    size.title = e.size == null ? "logical size unknown" : `logical size: ${e.size} B`;
    tr.appendChild(size);

    const used = el("td", "num dim", isDir ? "—" : humanSize(e.used));
    used.title = e.used == null ? "used bytes unknown" : `used (post-reduction): ${e.used} B`;
    tr.appendChild(used);

    const mt = el("td", "dim", fmtTime(e.mtime));
    mt.title = `mtime (UTC): ${e.mtime}`;
    tr.appendChild(mt);

    const at = el("td", "dim", fmtTime(e.atime));
    at.title = `atime (UTC): ${e.atime}`;
    tr.appendChild(at);

    tr.appendChild(el("td", "dim", e.owner_name || "—"));

    const ru = el("td", "num");
    if (isDir) {
      const stats = rollup && rollup.children[e.name];
      if (stats) {
        ru.textContent = `${humanSize(stats.total_bytes)} · ${stats.file_count.toLocaleString()} files`;
        ru.title = `descendant total; last modified ${fmtTime(stats.last_modified)}`;
      } else if (rollup) {
        // Rollup finished but this child is not in it. A truncated result
        // only carries the largest children; a complete one seeds every
        // child dir that existed when it was computed, so absence there
        // means this directory is newer than the rollup.
        ru.textContent = "—";
        ru.className = "num dim";
        ru.title = rollup.truncated
          ? "not among the largest children in the rollup (result truncated)"
          : "not present when this rollup was computed (directory is newer)";
      } else if (rollupInFlight === state.path) {
        ru.textContent = "…";
        ru.className = "num dim";
        ru.title = "rollup still computing";
      } else {
        // No rollup and none in flight: it failed or was skipped (the
        // rollup panel below says why) — "…" would read as a hang.
        ru.textContent = "—";
        ru.className = "num dim";
        ru.title = "no rollup available for this directory (see the rollup panel)";
      }
    }
    tr.appendChild(ru);

    tbody.appendChild(tr);
  }

  document.querySelectorAll("thead th[data-sort]").forEach(th => {
    const active = th.dataset.sort === state.sort;
    th.classList.toggle("sorted", active);
    th.textContent = th.textContent.replace(/ [▲▼]$/, "") +
      (active ? (state.order === "asc" ? " ▲" : " ▼") : "");
  });

  const summary = [
    `${data.total_dirs.toLocaleString()} dirs`,
    `${data.total_files.toLocaleString()} files`,
  ];
  if (data.total_other) summary.push(`${data.total_other.toLocaleString()} other`);
  $("listing-summary").textContent = summary.join(" · ") +
    (data.truncated ? " (truncated)" : "");
}

function renderPager(data) {
  $("page-info").textContent = `page ${data.page.toLocaleString()} / ${data.pages.toLocaleString()}`;
  $("prev-page").disabled = data.page <= 1;
  $("next-page").disabled = data.page >= data.pages;
  $("jump-input").max = data.pages;
}

function resetListing() {
  pageCache.clear();
  forwardStreak = 0;
}

function clearListingUI() {
  // Honest empty state after a failed load: no rows, pager, or summary from
  // a directory (or sort/filter state) we are no longer showing.
  listing = null;
  $("table-body").textContent = "";
  $("table-empty").classList.add("hidden");
  $("listing-summary").textContent = "";
  $("page-info").textContent = "—";
  $("prev-page").disabled = true;
  $("next-page").disabled = true;
  $("jump-input").removeAttribute("max"); // a stale max blocks valid retries
  updateExportLinks();
}

/* ---------- rollup ---------- */

function cancelActiveRollup() {
  // Invalidate any in-flight rollup load/poll chain and detach from the
  // server-side job. Every transition (navigate, new rollup) goes through
  // here so a stale poll can never repaint panels that were cleared.
  rollupToken++;
  rollupInFlight = null;
  if (activeRollupJobId) {
    api("/api/rollup/status",
        { job_id: activeRollupJobId, cancel_token: activeRollupCancelToken },
        { method: "DELETE" }).catch(() => {});
    activeRollupJobId = null;
    activeRollupCancelToken = null;
  }
}

function resetRollupPanels() {
  $("rollup-totals").textContent = "";
  $("rollup-children").textContent = "";
  $("export-rollup").classList.add("hidden");
}

function renderRollup(result) {
  const byName = Object.create(null);
  for (const c of result.children) byName[c.name] = c;
  rollups.delete(result.path);
  rollups.set(result.path, {
    children: byName,
    totals: result.totals,
    truncated: !!result.children_truncated,
  });
  while (rollups.size > 50) rollups.delete(rollups.keys().next().value);

  const t = result.totals;
  $("rollup-status").textContent =
    `rollup of ${result.path} — scanned ${result.rows_scanned.toLocaleString()} ` +
    `file rows in ${result.elapsed_s}s` +
    (result.from_cache ? " (cached)" : "") +
    (result.children_truncated
      ? ` — showing ${result.children.length.toLocaleString()} largest of ` +
        `${result.total_children.toLocaleString()} groups`
      : "");
  const totals = $("rollup-totals");
  totals.textContent = "";
  totals.appendChild(el("b", "", `${t.file_count.toLocaleString()} files`));
  totals.appendChild(document.createTextNode(" · "));
  totals.appendChild(el("b", "", humanSize(t.total_bytes)));
  totals.appendChild(document.createTextNode(` logical · ${humanSize(t.total_used)} used · last modified ${fmtTime(t.last_modified)}`));

  const wrap = $("rollup-children");
  wrap.textContent = "";
  let maxBytes = 1;
  for (const child of result.children) maxBytes = Math.max(maxBytes, child.total_bytes);
  for (const c of result.children) {
    const row = el("div", "rc-row");
    const name = el("span", "rc-name", c.name === "." ? "(files here)" : c.name);
    if (c.name !== ".") name.onclick = () => navigate(result.path + c.name + "/");
    row.appendChild(name);
    const track = el("div", "rc-bar-track");
    const bar = el("div", "rc-bar");
    bar.style.width = `${(100 * c.total_bytes / maxBytes).toFixed(2)}%`;
    track.appendChild(bar);
    row.appendChild(track);
    const sz = el("span", "", humanSize(c.total_bytes));
    sz.title = `${c.total_bytes} B logical · ${c.total_used} B used`;
    row.appendChild(sz);
    row.appendChild(el("span", "rc-dim", `${c.file_count.toLocaleString()} f`));
    row.appendChild(el("span", "rc-dim", `mod ${fmtTime(c.last_modified)}`));
    wrap.appendChild(row);
  }

  if (listing && listing.path === state.path && result.path === state.path) {
    renderTable(listing);
  }
  annotateTree(result.path);
  if (result.path === state.path) {
    // The export href is built from state.path; unhiding it for another
    // path would offer a CSV of a rollup that was never computed (404).
    $("export-rollup").classList.remove("hidden");
    updateExportLinks();
  }
}

function rollupSettled() {
  // Terminal state (success, failure, or policy skip) for the rollup owning
  // the current token: flip any "…" table cells to their honest state.
  rollupInFlight = null;
  if (listing && listing.path === state.path) renderTable(listing);
}

async function loadRollup(path) {
  cancelActiveRollup();
  const token = ++rollupToken;
  rollupInFlight = path;
  resetRollupPanels();
  renderRollupProgress(`computing rollup of ${path} …`);
  let r;
  try {
    r = await api("/api/rollup", { path });
  } catch (e) {
    if (token !== rollupToken) return;
    if (e.status === 403) {
      // deliberate server policy (root = full-namespace scan), not a failure
      $("rollup-status").textContent =
        "rollup skipped at / — browse into a directory for per-folder " +
        "totals (or start Catwalk with CATWALK_ALLOW_ROOT=1)";
    } else {
      $("rollup-status").textContent = `rollup unavailable: ${e.message}`;
    }
    rollupSettled();
    return;
  }
  if (token !== rollupToken) {
    if (r.status === 202 && r.body.job_id) {
      api("/api/rollup/status",
          { job_id: r.body.job_id, cancel_token: r.body.cancel_token },
          { method: "DELETE" }).catch(() => {});
    }
    return;
  }
  if (r.status === 200) { rollupInFlight = null; renderRollup(r.body); return; }

  // 202: poll the job until done.
  const jobId = r.body.job_id;
  activeRollupJobId = jobId;
  activeRollupCancelToken = r.body.cancel_token;
  const poll = async () => {
    if (token !== rollupToken) return;
    let s;
    try {
      s = await api("/api/rollup/status", { job_id: jobId });
    } catch (e) {
      if (token !== rollupToken) return;
      $("rollup-status").textContent = `rollup failed: ${e.message}`;
      rollupSettled();
      return;
    }
    if (token !== rollupToken) return;
    if (s.body.status === "done") {
      if (activeRollupJobId === jobId) {
        activeRollupJobId = null;
        activeRollupCancelToken = null;
      }
      rollupInFlight = null;
      renderRollup(s.body.result);
      return;
    }
    if (s.body.status === "error" || s.body.status === "cancelled") {
      if (activeRollupJobId === jobId) {
        activeRollupJobId = null;
        activeRollupCancelToken = null;
      }
      $("rollup-status").textContent = `rollup failed: ${s.body.error}`;
      rollupSettled();
      return;
    }
    const queueText = s.body.status === "queued"
      ? `queued for ${s.body.queued_s}s`
      : `${(s.body.rows_scanned || 0).toLocaleString()} rows scanned, ` +
        `${s.body.elapsed_s}s elapsed`;
    renderRollupProgress(`rollup of ${path} ${s.body.status} — ${queueText}`);
    setTimeout(poll, 1000);
  };
  setTimeout(poll, 1000);
}

function renderRollupProgress(message) {
  const status = $("rollup-status");
  status.textContent = "";
  status.appendChild(el("span", "spin", "◌"));
  status.appendChild(document.createTextNode(` ${message}`));
}

/* ---------- tree ---------- */

function makeTreeNode(path, label) {
  const li = document.createElement("li");
  const node = el("div", "node");
  const twist = el("span", "twist", "▸");
  const name = el("span", "", label);
  const badge = el("span", "badge");
  node.append(twist, name, badge);
  li.appendChild(node);
  const rec = { li, node, twist, badge, childrenUl: null, expanded: false };
  treeNodes.set(path, rec);

  twist.onclick = (ev) => { ev.stopPropagation(); toggleNode(path); };
  node.onclick = () => navigate(path);
  return li;
}

async function toggleNode(path) {
  const rec = treeNodes.get(path);
  if (!rec) return;
  if (rec.expanded) {
    rec.expanded = false;
    rec.twist.textContent = "▸";
    if (rec.childrenUl) rec.childrenUl.classList.add("hidden");
    return;
  }
  rec.expanded = true;
  rec.twist.textContent = "▾";
  if (rec.childrenUl) { rec.childrenUl.classList.remove("hidden"); return; }

  rec.twist.textContent = "◌";
  let data;
  try {
    data = await api("/api/ls",
      { path, type: "dir", page_size: 100, sort: "name", order: "asc" });
  } catch (e) {
    rec.twist.textContent = "▸";
    rec.expanded = false;
    return;
  }
  rec.twist.textContent = "▾";
  const ul = document.createElement("ul");
  for (const d of data.body.entries) {
    ul.appendChild(makeTreeNode(path + d.name + "/", d.name));
  }
  if (data.body.total_dirs > data.body.entries.length) {
    const more = data.body.total_dirs - data.body.entries.length;
    ul.appendChild(el("li", "more", `… +${more.toLocaleString()} more dirs`));
  }
  if (!data.body.entries.length && !data.body.total_dirs) {
    rec.twist.textContent = "·";
  }
  rec.childrenUl = ul;
  rec.li.appendChild(ul);
  annotateTree(path);
}

function rebuildTree(rootPath) {
  treeRoot = rootPath;
  const tree = $("tree");
  tree.textContent = "";
  treeNodes.clear();
  const ul = document.createElement("ul");
  ul.appendChild(makeTreeNode(rootPath, rootPath === "/" ? "/" : rootPath.replace(/\/$/, "")));
  tree.appendChild(ul);
  toggleNode(rootPath);
}

async function expandTo(path) {
  if (!path.startsWith(treeRoot)) return;
  const rel = path.slice(treeRoot.length).split("/").filter(Boolean);
  let acc = treeRoot;
  for (const part of rel) {
    const rec = treeNodes.get(acc);
    if (rec && !rec.expanded) await toggleNode(acc);
    acc = acc + part + "/";
    if (!treeNodes.has(acc)) break;
  }
  highlightTree();
}

function highlightTree() {
  for (const [p, rec] of treeNodes) {
    rec.node.classList.toggle("current", p === state.path);
  }
}

function annotateTree(rollupPath) {
  // One rollup owns the badges of its directory level: paint the children it
  // reports and clear the rest, so badges from an older rollup (or children a
  // truncated result no longer vouches for) cannot mix vintages.
  const rollup = rollups.get(rollupPath);
  if (!rollup) return;
  for (const [p, rec] of treeNodes) {
    if (p === rollupPath || !p.startsWith(rollupPath)) continue;
    const name = p.slice(rollupPath.length, -1);
    if (name.includes("/")) continue; // deeper descendant, not a direct child
    const stats = rollup.children[name];
    rec.badge.textContent = stats ? humanSize(stats.total_bytes) : "";
  }
}

/* ---------- navigation ---------- */

async function navigate(path) {
  state.path = normDir(path);
  const target = state.path;
  state.page = 1;
  state.nameFilter = "";
  $("name-filter").value = "";
  resetListing();
  hideBanner();
  renderBreadcrumb();
  // Tear down the previous directory's async work before the first await:
  // a surviving rollup poll would repaint the panels this transition owns.
  cancelActiveRollup();
  resetRollupPanels();
  $("rollup-status").textContent = "";
  updateExportLinks();
  highlightTree();

  if (!state.path.startsWith(treeRoot)) rebuildTree(state.path);

  const result = await loadPage(1, { fromNav: true });
  if (state.path !== target) return;        // a newer navigate owns the UI
  if (result === LOAD_FAILED) return;       // loadPage cleared the listing; keep panels empty
  // LOAD_OK — or superseded by a same-path reload (sort/filter/pager click
  // mid-navigate), whose render is equally valid for target: finish up.
  expandTo(target);
  loadRollup(target);
}

function updateExportLinks() {
  const q = new URLSearchParams({
    path: state.path, sort: state.sort, order: state.order,
    type: state.type, name_filter: state.nameFilter,
  });
  $("export-listing").href = `/api/export/listing?${q}`;
  $("export-rollup").href =
    `/api/export/rollup?${new URLSearchParams({ path: state.path })}`;
}

/* ---------- wiring ---------- */

function refreshListing() {
  state.page = 1;
  resetListing();
  loadPage(1);
}

function init() {
  initTheme();
  loadHealth();
  loadViews();

  $("view-select").onchange = (e) => {
    if (e.target.value) {
      rebuildTree(normDir(e.target.value));
      navigate(e.target.value);
    }
  };

  $("breadcrumb").onclick = (e) => {
    if (e.target.tagName !== "A" && e.target.tagName !== "INPUT") editPath();
  };

  $("prev-page").onclick = () => {
    forwardStreak = 0;
    if (state.page > 1) loadPage(state.page - 1);
  };
  $("next-page").onclick = () => {
    forwardStreak++;
    loadPage(state.page + 1);
  };
  $("jump-form").onsubmit = (e) => {
    e.preventDefault();
    const p = parseInt($("jump-input").value, 10);
    forwardStreak = 0;
    if (p >= 1) loadPage(p);
  };
  $("page-size").onchange = (e) => {
    state.pageSize = parseInt(e.target.value, 10);
    refreshListing();
  };
  $("type-filter").onchange = (e) => {
    state.type = e.target.value;
    refreshListing();
  };

  let filterTimer = null;
  $("name-filter").oninput = (e) => {
    clearTimeout(filterTimer);
    filterTimer = setTimeout(() => {
      state.nameFilter = e.target.value.trim();
      refreshListing();
    }, 350);
  };

  document.querySelectorAll("thead th[data-sort]").forEach(th => {
    th.onclick = () => {
      const key = th.dataset.sort;
      if (state.sort === key) {
        state.order = state.order === "asc" ? "desc" : "asc";
      } else {
        state.sort = key;
        state.order = key === "name" ? "asc" : "desc";
      }
      refreshListing();
    };
  });

  rebuildTree("/");
  navigate("/");
}

init();
