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
directly (click the breadcrumb) — catalog browsing never depends on VMS.

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

Core variables (the `catwalk` launcher accepts equivalent CLI flags):

| Variable | Default | Meaning |
|---|---|---|
| `VASTDB_ENDPOINT` | — | Data VIP pool URL. **Never** the VMS address. |
| `VASTDB_ACCESS_KEY` / `VASTDB_SECRET_KEY` | — | S3 keys for the data plane |
| `VASTDB_DATA_ENDPOINTS` | — | Explicit comma-separated VIP URLs |
| `CATWALK_AUTO_ENDPOINTS` | off | Resolve the endpoint's A records, query across every VIP |
| `VMS_ADDRESS` / `VMS_USER` / `VMS_PASSWORD` | — | VMS credentials (optional). Address is a hostname or IP (a pasted `http(s)://` URL is normalized; VMS is always reached over HTTPS :443) |
| `CATWALK_HOST` / `CATWALK_PORT` | `0.0.0.0` / `8080` | Bind address (port `0` = pick a free port) |
| `CATWALK_STATE_DIR` | `~/.catwalk` | Env-file/pid/log location for `catwalk start`/`stop`/`status`; one instance per dir |
| `CATWALK_MOCK` | off | Serve the synthetic namespace instead of a cluster |

Every performance and behavior knob — caching and snapshot pinning, query
concurrency and timeouts, prefetch, cache warming, rollup bounds — is
documented in **[TUNING.md](TUNING.md)**, along with how to read the timing
log and a recommended warming recipe. The defaults work; start there only if
first visits feel slow.

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

## The freshness caveat (read this once)

**The Catalog is a snapshot, not live state.** It is re-indexed from periodic
filesystem snapshots — typically every 30 minutes, minimum 15. Catwalk shows
this in the header on every page. If a user says "I just deleted that file
and it's still listed", this is why. Catwalk tracks the newest catalog
snapshot automatically, pins queries to it, and invalidates its caches the
moment a new one lands (see [TUNING.md](TUNING.md)); the snapshot in use is
reported as `catalog_snapshot` in API responses.

Also note `size` vs `used`: `size` is logical bytes, `used` is bytes consumed
after data reduction. Catwalk shows both, labeled; they are never summed
interchangeably. Rollups deliberately refuse time filters — filtering a
rollup by mtime would silently misreport "last modified".

## API

| Endpoint | Purpose |
|---|---|
| `GET /api/views` | Views from VMS (60s cache); `vms_unavailable: true` when VMS is absent |
| `GET /api/ls?path=&page=&page_size=&sort=&order=&type=&name_filter=` | Paged directory listing |
| `GET /api/rollup?path=&depth=&child_limit=` | Descendant rollup; `200` inline or `202 {job_id, cancel_token}`; child results are size-ranked and bounded |
| `GET /api/rollup/status?job_id=` | Job progress (rows scanned) and final result |
| `DELETE /api/rollup/status?job_id=&cancel_token=` | Detach from a shared rollup job; cancels the scan when the last client detaches |
| `GET /api/capacity?path=` | VMS sampled capacity estimate (best-effort, 60s cache) |
| `GET /api/export/listing`, `GET /api/export/rollup` | CSV downloads |
| `GET /api/health` | vastdb / VMS / catalog reachability + per-cache hit/miss stats; always answers, even when every query slot is busy |

Every response carries an `X-Catwalk-Elapsed` header; listing responses
include `from_cache` and `query_elapsed_s`.

## Tests

```bash
pip install -e .[dev]
pytest
```

All tests run against the mock catalog — no cluster or secrets. The rollup
tests recompute totals directly from the mock catalog and compare them with
Catwalk's grouped result, so aggregation is validated end to end. CSV listing
exports mark incomplete data in both `X-Catwalk-Truncated` and the download
filename; spreadsheet-formula prefixes in catalog strings are escaped with a
leading apostrophe.
