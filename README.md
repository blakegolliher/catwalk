# Catwalk 🐈 — interactive VAST Catalog file browser

Catwalk is a web-based file browser for the **VAST Catalog** — the queryable,
snapshot-indexed metadata table of every file, directory, and object on a VAST
cluster. Instead of walking the filesystem over NFS/S3 (slow, invasive, and
hard on the cluster), it answers everything from the Catalog's tabular index
over the VAST DB SDK: pick a view, browse the tree, page through directories
with millions of entries, and see per-directory space rollups aggregated over
*all* descendants. It is a single Python process for a jump host — `pip
install`, one command, open a browser; no build step, no database server, no
Trino.

## Quickstart (demo / mock mode — no cluster needed)

```bash
pip install -e .
catwalk start --mock        # background; prints the URLs it listens on
# open http://localhost:8080
catwalk status              # pid, port, /api/health summary
catwalk stop
```

Mock mode serves a deterministic synthetic namespace (~1M files under
`/bench-2b`, ~50k elements under `/projects`, small home dirs) and mock
views. It doubles as the demo mode for showing Catwalk to customers without
touching their cluster.

## Quickstart (real cluster)

```bash
pip install -e .

mkdir -p ~/.catwalk
(umask 077; cat > ~/.catwalk/catwalk.env <<'EOF'
# Data plane (required) — data VIP pool DNS name or VIP, NOT the VMS address
VASTDB_ENDPOINT=http://pool1.lab.vast.com
VASTDB_ACCESS_KEY=...
VASTDB_SECRET_KEY=...
# Fan out across every VIP in the pool
CATWALK_AUTO_ENDPOINTS=1

# Control plane (optional — enables the view selector and capacity estimates)
VMS_ADDRESS=vms.lab.vast.com
VMS_USER=...
VMS_PASSWORD=...
EOF
)

catwalk start --port 8080
```

Without VMS credentials the view selector is empty and you type paths
directly — catalog browsing never depends on VMS.

## Running it

`catwalk start` validates the configuration, detaches from the terminal
(survives logout), waits for the server socket, and prints one URL per
reachable address (a `0.0.0.0` bind — the default — is expanded via `/proc`
to every IP assigned on the host, so the report tells you if you are bound to
loopback only). `catwalk status` shows pid, port, uptime, and an
`/api/health` probe (exit code 0 up / 3 down); `catwalk stop` shuts it down
(`--force` to SIGKILL). `catwalk --help` lists per-command options.

State lives in `CATWALK_STATE_DIR` (default `~/.catwalk`):

    catwalk.env    optional KEY=VALUE settings, loaded by start/run
    catwalk.json   pid, bind address, resolved port, log path, start time
    catwalk.log    server output, appended across restarts

One state dir manages one instance — to run several, give each its own
`CATWALK_STATE_DIR`. `--port 0` picks a free port and prints it.

Foreground alternatives: `catwalk run` (same flags, logs to the terminal), or
plain `uvicorn catwalk.app:app --host 0.0.0.0 --port 8080` — note the bare
`uvicorn` CLI defaults to `127.0.0.1` and does not read `catwalk.env`.

## Configuration

Two connection planes are used and never mixed: `vastpy` → VMS (HTTPS :443)
for enumerating views, and `vastdb` → **data VIPs** for every catalog query.

Settings come from three places; later ones win:

1. **Env file** — `catwalk start`/`run` load `<CATWALK_STATE_DIR>/catwalk.env`
   (default `~/.catwalk/catwalk.env`) if it exists, or the file given with
   `--env-file`. Plain `KEY=VALUE` lines with the variable names below;
   `#` comments on their own line; an optional `export ` prefix is tolerated
   so the file can also be `source`d. Keep it `chmod 600` — it holds keys.
2. **Environment variables** already set in the shell.
3. **CLI flags** (`catwalk start --port 8081 ...`).

Variables (the `catwalk` launcher accepts equivalent CLI flags):

| Variable | Default | Meaning |
|---|---|---|
| `VASTDB_ENDPOINT` | — | Data VIP pool URL. **Never** the VMS address. |
| `VASTDB_ACCESS_KEY` / `VASTDB_SECRET_KEY` | — | S3 keys for the data plane |
| `VASTDB_DATA_ENDPOINTS` | — | Explicit comma-separated VIP URLs |
| `CATWALK_AUTO_ENDPOINTS` | off | Resolve the endpoint's A records, query across every VIP |
| `VMS_ADDRESS` / `VMS_USER` / `VMS_PASSWORD` | — | VMS credentials (optional). Address is a hostname or IP (a pasted `http(s)://` URL is normalized; VMS is always reached over HTTPS :443) |
| `CATWALK_HOST` / `CATWALK_PORT` | `0.0.0.0` / `8080` | Bind address (port `0` = pick a free port) |
| `CATWALK_STATE_DIR` | `~/.catwalk` | Env-file/pid/log location for `catwalk start`/`stop`/`status`; one instance per dir |
| `CATWALK_PAGE_DEFAULT` / `CATWALK_PAGE_MAX` | `20` / `100` | Page size bounds |
| `CATWALK_CACHE_TTL` | `900` | Cache TTL seconds — align with the catalog snapshot cadence |
| `CATWALK_LISTING_CAP` | `500000` | Max rows cached per directory listing |
| `CATWALK_CACHE_MAX_BYTES` | `2 GiB` | LRU bound on cached listings |
| `CATWALK_NUM_SPLITS` | `64` | `QueryConfig.num_splits` for scans |
| `CATWALK_QUERY_CONCURRENCY` | `24` | SDK reader threads per query (the VIP list is repeated to this length; `0` disables) |
| `CATWALK_QUERY_THREADS` | `8` | Global high-level catalog query budget shared by interactive requests, prefetch, warming, and rollups |
| `CATWALK_QUERY_TIMEOUT` | `60` | Per-socket-read timeout (seconds) on catalog queries, so a dead VIP cannot pin a query slot forever; `0` disables |
| `CATWALK_PREFETCH_CHILDREN` | `8` | After a listing, warm the cache for this many child dirs in the background (`0` disables) |
| `CATWALK_WARM_PATHS` | — | Comma-separated roots to cache-warm at startup and keep warm (e.g. `/projects,/home`; unset disables the warmer) |
| `CATWALK_WARM_DEPTH` | `2` | Warm listings this many levels below each warm root (`0` = roots only) |
| `CATWALK_WARM_THREADS` | `4` | Worker threads for warm passes |
| `CATWALK_WARM_MAX_DIRS` | `500` | Max listings per warm pass (BFS stops at the cap) |
| `CATWALK_WARM_INTERVAL` | `0.75 × TTL` | Seconds between refresh passes |
| `CATWALK_ROLLUP_THREADS` | `8` | Aggregation threads consuming a rollup's batch stream |
| `CATWALK_ROLLUP_WORKERS` | `2` | Concurrent background rollups |
| `CATWALK_ROLLUP_QUEUE_MAX` | `16` | Maximum queued rollups; excess requests receive HTTP 429 |
| `CATWALK_ROLLUP_SYNC_TIMEOUT` | `5` | Seconds before a rollup goes async (HTTP 202 + polling) |
| `CATWALK_ROLLUP_CACHE_MAX_BYTES` | `256 MiB` | LRU byte bound for completed rollups |
| `CATWALK_ROLLUP_MAX_GROUPS` | `10000` | Safety ceiling on distinct folder groups in one rollup |
| `CATWALK_ROLLUP_MAX_DEPTH` | `8` | Maximum accepted rollup grouping depth |
| `CATWALK_ROLLUP_RESPONSE_CHILDREN` | `500` | Largest child groups returned to the browser/API by default; exports retain all computed groups |
| `CATWALK_ALLOW_ROOT` | off | Permit a rollup of `/` (a full-namespace scan) |
| `CATWALK_MOCK` | off | Serve the synthetic namespace instead of a cluster |

## Tenant scoping (there is no tenant option — on purpose)

The Big Catalog is **automatically scoped, server-side, to the tenant of the
S3 identity you query with** — a tenant's key sees exactly its own namespace
and nothing else. To browse a tenant with Catwalk, run it with that tenant's
credentials:

- an S3 key for a user **in that tenant**, with an identity policy granting
  `s3:Tabular*` (same requirements as the audit DB — no DATABASE view, no
  per-tenant catalog setup);
- that tenant's **own VIP pool** as `VASTDB_ENDPOINT` — keys are bound to
  their tenant's VIPs, and a key replayed against another tenant's VIP gets
  `403 Forbidden` before the transaction starts.

Run one Catwalk instance per tenant to give several tenants a browser at
once: give each its own `CATWALK_STATE_DIR` (holding that tenant's
`catwalk.env` with its VIP pool, keys, and port) and `catwalk start` them
independently.

Do **not** try to filter on the catalog's `tenant_id` column: it holds
internal ids that do not match VMS tenant ids (VMS tenant 57 can appear as
54; default-tenant rows show `-1`) — and you don't need it, since scoping is
enforced by the server anyway.

## How it answers fast

- A directory listing is **one** catalog query (`parent_path == "/dir/"` —
  trailing slash matters); the result is cached as an Arrow table and every
  page is a memory slice. Sort changes re-sort the cached table; the client
  additionally prefetches upcoming pages, so paging forward never waits on
  the network.
- Listings stop at `CATWALK_LISTING_CAP` rows and the UI shows a truncation
  banner; name/type filters are pushed into the query so a filtered listing
  can escape the cap. A type/name-filtered request is derived in memory from
  the cached unfiltered listing whenever that cache is complete.
- After each listing, the first `CATWALK_PREFETCH_CHILDREN` child directories
  are listed speculatively in the background (a dedicated 2-thread pool), so
  clicking into a child usually serves from cache in milliseconds. A click on
  a child whose prefetch is mid-flight joins it instead of racing it with a
  duplicate scan; a click on one still queued cancels the queued prefetch and
  runs directly.
- With `CATWALK_WARM_PATHS` set, a background warmer walks each root
  breadth-first at startup and caches every listing down to
  `CATWALK_WARM_DEPTH` levels, then re-runs periodically so the warm set
  never falls out of the TTL cache — the first page a user opens is served
  from memory. Refresh passes skip listings recently re-queried by real
  browsing, and each pass is capped at `CATWALK_WARM_MAX_DIRS` listings.
  `/api/health` reports the warmer's last pass.
- A rollup streams every descendant `FILE` row under the prefix through a
  vectorized per-batch aggregation (ported from `catalog_folder_report.py`),
  grouped by depth-1 child. Fast rollups return inline; slow ones return
  `202` with a job you can poll (`/api/rollup/status`), and results are
  cached. Empty child directories are seeded from a `DIR` listing so they
  still appear. Work is bounded and cancellable: the queue rejects excess
  scans, grouping stops at `CATWALK_ROLLUP_MAX_GROUPS`, and browser navigation
  cancels obsolete asynchronous jobs. Identical concurrent rollups share one
  job; each `202` carries a per-client `cancel_token`, and the shared scan is
  only cancelled once every client has detached with its token.
- Health, view, and capacity endpoints run on their own small thread budget
  with a bounded probe, so `/api/health` answers even while every catalog
  query slot is busy — and `CATWALK_QUERY_TIMEOUT` keeps a black-holed VIP
  from pinning those slots forever.

## Performance notes (measured against a 6-VIP lab, ~840M-row catalog)

- The vastdb SDK runs **one reader thread per `data_endpoints` entry**, so
  Catwalk repeats the VIP list up to `CATWALK_QUERY_CONCURRENCY` (24) —
  without this a 6-VIP pool caps every query at 6 threads.
- `num_splits=64` matters even for tiny listings: the SDK's auto-estimate
  derives from *total* table rows and measured ~5x slower for a 3-entry
  directory. `num_sub_splits` is best left at its default (4); higher values
  measured strictly worse.
- Rollup aggregation is pipelined: the select() stream feeds a pool of
  `CATWALK_ROLLUP_THREADS` aggregation threads, so transport and per-batch
  group_by overlap.
- Expect a **~2s latency floor on any first visit to a directory** — that is
  catalog-side query latency, not something client settings change. Subtree
  scan throughput varies widely with path depth (long `parent_path` strings
  dominate transferred bytes). Catwalk's answer to both is caching: revisits
  and re-sorts are served from memory until the TTL expires.

## The freshness caveat (read this once)

**The Catalog is a snapshot, not live state.** It is re-indexed from periodic
filesystem snapshots — typically every 30 minutes, minimum 15. Catwalk shows
this in the header on every page. If a user says "I just deleted that file
and it's still listed", this is why. Size caches (`CATWALK_CACHE_TTL`,
default 15 min) to the snapshot cadence.

Also note `size` vs `used`: `size` is logical bytes, `used` is bytes consumed
after data reduction. Catwalk shows both, labeled; they are never summed
interchangeably. Rollups deliberately refuse time filters — filtering a
rollup by mtime would silently misreport "last modified".

## Correctness validation

The rollup tests recompute totals directly from the mock catalog and compare
them with Catwalk's grouped result. If you also have the external
`catalog_folder_report.py` utility from the original catalog tooling, you can
validate a real install by running it and Catwalk against the same path and
catalog snapshot:

```bash
catalog_folder_report.py --path /some/dir --mode folders
curl -s "localhost:8080/api/rollup?path=/some/dir"
```

File counts, total bytes, and last-modified/last-accessed per child must
match exactly (both aggregate over all descendant FILE rows of the same
catalog snapshot; if a new snapshot lands between the two runs, re-run).

## API

| Endpoint | Purpose |
|---|---|
| `GET /api/views` | Views from VMS (60s cache); `vms_unavailable: true` when VMS is absent |
| `GET /api/ls?path=&page=&page_size=&sort=&order=&type=&name_filter=` | Paged directory listing |
| `GET /api/rollup?path=&depth=&child_limit=` | Descendant rollup; `200` inline or `202 {job_id, cancel_token}`; child results are size-ranked and bounded |
| `GET /api/rollup/status?job_id=` | Job progress (rows scanned) and final result |
| `DELETE /api/rollup/status?job_id=&cancel_token=` | Detach from a shared rollup job; cancels the scan when the last client detaches |
| `GET /api/capacity?path=` | VMS sampled capacity estimate (best-effort) |
| `GET /api/export/listing`, `GET /api/export/rollup` | CSV downloads |
| `GET /api/health` | vastdb / VMS / catalog reachability; always answers, even when every query slot is busy |

## Tests

```bash
pip install -e .[dev]
pytest
```

All tests run against the mock catalog — no cluster or secrets. CSV listing
exports mark incomplete data in both `X-Catwalk-Truncated` and the download
filename; spreadsheet-formula prefixes in catalog strings are escaped with a
leading apostrophe.
