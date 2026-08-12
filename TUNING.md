# Tuning & troubleshooting Catwalk

Everything here is optional: the defaults work on a healthy cluster. Read
this when first visits feel slow, when you want warm-cache navigation, or
when you need to see where time is going.

The one mental model that matters: **catalog queries are server-bound.**
A directory listing has a ~2 s latency floor no matter what the client
does, and concurrent queries split the cluster's capacity between them —
more client-side parallelism makes each query slower, not the answer
faster. Caching, not concurrency, is the lever, and every knob below
exists either to cache more or to observe what the cluster is doing.

## How it answers fast

- The VAST Catalog keeps periodic snapshots of itself (typically every 15
  minutes, named `big_catalog_<UTC timestamp>`). Catwalk polls for the newest
  one (`CATWALK_SNAPSHOT_POLL`, one cheap S3 LIST), **pins every query to
  it**, and keys both caches by its name. Cache invalidation is therefore
  exact: a new snapshot flips every cached entry at once, listings and
  rollups always describe the same catalog state, and results are consistent
  and repeatable within an epoch instead of drifting with live ingest. The
  TTL remains only as a backstop. On clusters without catalog snapshots (or
  with `CATWALK_SNAPSHOT_PIN=0`) queries run against the live table and
  caching falls back to pure TTL. The pinned snapshot name is reported as
  `catalog_snapshot` in listing, rollup, and health responses.
- A directory listing is **one** catalog query (`parent_path == "/dir/"` —
  trailing slash matters); the result is cached as an Arrow table and every
  page is a memory slice. Sort changes re-sort the cached table; the client
  additionally prefetches upcoming pages, so paging forward never waits on
  the network. Concurrent cache misses for the same listing are coalesced
  into a single scan (single-flight) — a burst of identical clicks costs one
  query, not one per user.
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
  stays cached. Refresh passes skip listings that still have plenty of life
  left, and each pass is capped at `CATWALK_WARM_MAX_DIRS` listings.
  `/api/health` reports the warmer's last pass.
- A rollup streams every descendant `FILE` row under the prefix through a
  vectorized per-batch aggregation, grouped by depth-1 child. Fast rollups
  return inline; slow ones return `202` with a job you can poll
  (`/api/rollup/status`), and results are cached. Empty child directories are
  seeded from a `DIR` listing so they still appear. Work is bounded and
  cancellable: the queue rejects excess scans, grouping stops at
  `CATWALK_ROLLUP_MAX_GROUPS`, and browser navigation cancels obsolete
  asynchronous jobs. Identical concurrent rollups share one job; each `202`
  carries a per-client `cancel_token`, and the shared scan is only cancelled
  once every client has detached with its token.
- Health, view, and capacity endpoints run on their own small thread budget
  with a bounded probe, so `/api/health` answers even while every catalog
  query slot is busy — and `CATWALK_QUERY_TIMEOUT` keeps a black-holed VIP
  from pinning those slots forever. View and capacity answers are cached for
  60s.

## Tuning variables

Set these like any other configuration (env file, environment, or CLI
flags — see the README's Configuration section).

| Variable | Default | Meaning |
|---|---|---|
| `CATWALK_CACHE_TTL` | `900` | Cache TTL seconds (backstop; with snapshot pinning, epoch changes do the real invalidation) |
| `CATWALK_LISTING_CAP` | `500000` | Max rows cached per directory listing |
| `CATWALK_CACHE_MAX_BYTES` | `2 GiB` | LRU bound on cached listings |
| `CATWALK_SNAPSHOT_PIN` | on | Pin queries to the newest catalog snapshot and key caches by it (`0` disables: live queries, TTL-only invalidation) |
| `CATWALK_SNAPSHOT_POLL` | `60` | Seconds between checks for a newer catalog snapshot |
| `CATWALK_SNAPSHOT_PREFIX` | `big_catalog` | Catalog snapshot name prefix (names must sort chronologically within it) |
| `CATWALK_NUM_SPLITS` | `64` | `QueryConfig.num_splits` for scans |
| `CATWALK_QUERY_CONCURRENCY` | `24` | SDK reader threads per query (the VIP list is repeated to this length; `0` disables) |
| `CATWALK_QUERY_THREADS` | `8` | Global catalog query budget shared by interactive requests, prefetch, warming, and rollups |
| `CATWALK_QUERY_TIMEOUT` | `60` | Per-socket-read timeout (seconds) on catalog queries, so a dead VIP cannot pin a query slot forever; `0` disables |
| `CATWALK_PREFETCH_CHILDREN` | `8` | After a listing, warm the cache for this many child dirs in the background (`0` disables) |
| `CATWALK_WARM_PATHS` | — | Comma-separated roots to cache-warm at startup and keep warm (e.g. `/projects,/home`; unset disables the warmer) |
| `CATWALK_WARM_DEPTH` | `2` | Warm listings this many levels below each warm root (`0` = roots only) |
| `CATWALK_WARM_THREADS` | `4` | Worker threads for warm passes |
| `CATWALK_WARM_MAX_DIRS` | `500` | Max listings per warm pass (BFS stops at the cap) |
| `CATWALK_WARM_INTERVAL` | `0.75 × TTL` | Seconds between refresh passes |
| `CATWALK_PAGE_DEFAULT` / `CATWALK_PAGE_MAX` | `20` / `100` | Page size bounds |
| `CATWALK_ROLLUP_THREADS` | `8` | Aggregation threads consuming a rollup's batch stream |
| `CATWALK_ROLLUP_WORKERS` | `2` | Concurrent background rollups |
| `CATWALK_ROLLUP_QUEUE_MAX` | `16` | Maximum queued rollups; excess requests receive HTTP 429 |
| `CATWALK_ROLLUP_SYNC_TIMEOUT` | `5` | Seconds before a rollup goes async (HTTP 202 + polling) |
| `CATWALK_ROLLUP_CACHE_MAX_BYTES` | `256 MiB` | LRU byte bound for completed rollups |
| `CATWALK_ROLLUP_MAX_GROUPS` | `10000` | Safety ceiling on distinct folder groups in one rollup |
| `CATWALK_ROLLUP_MAX_DEPTH` | `8` | Maximum accepted rollup grouping depth |
| `CATWALK_ROLLUP_RESPONSE_CHILDREN` | `500` | Largest child groups returned to the browser/API by default; exports retain all computed groups |
| `CATWALK_ALLOW_ROOT` | off | Permit a rollup of `/` (a full-namespace scan) |

## A warming recipe that stays out of the way

Warming everything aggressively backfires: a warm pass running several
threads is itself concurrent catalog load, so it inflates every query it
overlaps with — including the interactive clicks it was meant to speed up.
On a busy or latency-prone cluster, this configuration keeps top-level
navigation warm with near-zero steady-state cost:

```bash
CATWALK_WARM_PATHS=/
CATWALK_WARM_DEPTH=1
CATWALK_WARM_THREADS=1
CATWALK_CACHE_TTL=3600
CATWALK_WARM_INTERVAL=300
```

Why it works: with snapshot pinning, freshness comes from epoch changes,
so a long TTL is safe. Warm passes then find every same-epoch entry
comfortably alive and skip it — a pass inside an epoch is nearly free.
Only when a new catalog snapshot lands do the cache keys change, and the
next pass (at most 5 minutes later) re-walks the warm set once, gently,
one query at a time. Steady state: warm navigation, one background query
trickling every couple of seconds for a few minutes per epoch flip.

The failure mode to avoid is the opposite shape — short TTL, aggressive
interval, several warm threads — which re-walks the entire warm set every
pass at full parallelism and can saturate the cluster continuously (the
log signature: tiny listings taking 15–20 s with `gate wait 0.00s`, and
health probes timing out).

## Reading the log for timing

Every catalog query and API request logs one timestamped line with its
duration, so `catwalk.log` answers "where did the time go" directly:

    ... catwalk.catalog [AnyIO worker thread] listing /jono/: 1204 rows in 3.42s (gate wait 0.00s, epoch big_catalog_...)
    ... catwalk.catalog [prefetch_0] listing /jono/gns/: 88 rows in 2.10s (gate wait 1.35s, epoch big_catalog_...)
    ... catwalk.catalog [rollup_1] rollup /jono/ depth=1: 8123456 rows, 14 groups in 41.20s (epoch big_catalog_...)
    ... catwalk.http [...] 10.1.2.3 GET /api/ls?path=%2Fjono%2F -> 200 in 3.431s

The thread tag says who paid: `AnyIO worker thread` is an interactive
request, `prefetch_N` / `warmer` / `rollup_N` are background work. The
**gate wait** vs total split is the diagnostic that matters:

- **High gate wait** — the request queued behind other catalog work.
  Raise `CATWALK_QUERY_THREADS`, or reduce prefetch/warm/rollup pressure.
- **Low gate wait, long total** — cluster-side query latency. Caching and
  warming are the lever, not client tuning.

API responses also carry an `X-Catwalk-Elapsed` header, and `/api/ls`
reports `from_cache` and `query_elapsed_s`, so automation can measure
without the log. `/api/health` reports per-cache
hit/miss/coalesced/expired/evicted counters and the warmer's last pass —
check the hit rate there before changing any knob.

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
  and re-sorts are served from memory until a newer catalog snapshot appears
  (or the TTL expires, where snapshots are unavailable).
- Cluster latency variance is large (identical 3-row listings measured
  2–10 s apart on different days) — never trust a single-run A/B against a
  live cluster; interleave and repeat.
