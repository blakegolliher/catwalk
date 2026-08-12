"""Catwalk FastAPI application.

Run:  CATWALK_MOCK=1 uvicorn catwalk.app:app         (demo, no cluster)
      uvicorn catwalk.app:app                        (real: env vars per README)

All catalog/VMS calls are blocking SDK calls. They run in bounded worker pools
so the event loop stays responsive, with one process-wide query budget.
"""

from __future__ import annotations

import functools
import logging
import re
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import anyio
import anyio.to_thread
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .cache import TTLCache
from .catalog import SNAPSHOT_HINT, ListingService, RollupService, make_backend, normalize_prefix
from .config import load_config
from .jobs import JobManager, JobQueueFullError
from .netinfo import report_listening
from .vms import VMSService
from .warmer import CacheWarmer

_state: dict = {}

_access_log = logging.getLogger("catwalk.http")


def _configure_logging():
    """Timestamped, thread-tagged lines for every catwalk.* logger.

    uvicorn only configures its own loggers; without this, catwalk's INFO
    lines (query timing, warm passes, epoch flips) would go nowhere. The
    thread name distinguishes interactive requests from prefetch/warmer/
    rollup work in the same log."""
    root = logging.getLogger("catwalk")
    if root.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname).1s %(name)s [%(threadName)s] %(message)s")
    )
    root.addHandler(handler)
    root.setLevel(logging.INFO)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    _configure_logging()
    cfg = load_config()
    _state["cfg"] = cfg
    # query_gate is the real backend budget (max concurrent catalog queries).
    # The limiter only bounds request worker threads and is deliberately
    # larger: a request holds its limiter slot *while waiting* for the gate,
    # and background work (warmer/prefetch/rollups) also feeds the gate — a
    # limiter the same size as the gate would let gate-blocked cold queries
    # starve cache hits, which never need the gate at all.
    _state["limiter"] = anyio.CapacityLimiter(cfg.query_threads + 16)
    # Control-plane calls (health/views/capacity) get their own small lane so
    # they stay answerable while every data slot is busy or wedged.
    _state["ctl_limiter"] = anyio.CapacityLimiter(4)
    _state["query_gate"] = threading.BoundedSemaphore(cfg.query_threads)
    _state["vms"] = VMSService(cfg)
    # Control-plane cache (views, capacity): VMS answers are cheap but not
    # free, and the UI re-asks on every navigation. 60s staleness is fine.
    _state["ctl_cache"] = TTLCache(60.0)
    _state["jobs"] = JobManager(max_workers=cfg.rollup_workers, max_queue=cfg.rollup_queue_max)
    _state["backend"] = None
    _state["backend_error"] = None
    report_listening(cfg.host, cfg.port)
    listings = warmer = None
    try:
        backend = make_backend(cfg)
        listings = ListingService(backend, cfg, query_gate=_state["query_gate"])
        rollups = RollupService(backend, cfg, query_gate=_state["query_gate"])
        warmer = CacheWarmer(listings, cfg)
        warmer.start()
        # Publish only after every service is up: a backend stored earlier
        # would satisfy the None-guards in _listings()/_rollups() while the
        # services are missing, turning the intended 503 into a KeyError.
        _state["backend"] = backend
        _state["listings"] = listings
        _state["rollups"] = rollups
        _state["warmer"] = warmer
    except Exception as e:
        # Start anyway so /api/health can explain what is wrong.
        _state["backend_error"] = str(e)
        if warmer is not None:
            warmer.stop()
        if listings is not None:
            listings.close()
    yield
    jobs = _state.get("jobs")
    if jobs is not None:
        # Cancel scans first so they cannot hold the shared query budget while
        # the warmer and speculative-listing pools are shutting down.
        jobs.close(wait=True)
    warmer = _state.get("warmer")
    if warmer is not None:
        warmer.stop()
    listings = _state.get("listings")
    if listings is not None:
        listings.close()
    _state.clear()


app = FastAPI(title="Catwalk", docs_url=None, redoc_url=None, lifespan=_lifespan)


@app.middleware("http")
async def _timed_access_log(request, call_next):
    """Replace uvicorn's untimed access log (the CLI disables it) with one
    line per API request including wall-clock duration, and stamp the same
    number on the response for curl/automation."""
    t0 = time.monotonic()
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        elapsed = time.monotonic() - t0
        response.headers["X-Catwalk-Elapsed"] = f"{elapsed:.3f}"
        target = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        client = request.client.host if request.client else "-"
        _access_log.info(
            "%s %s %s -> %d in %.3fs",
            client,
            request.method,
            target,
            response.status_code,
            elapsed,
        )
    return response


async def _run(fn, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        functools.partial(fn, *args, **kwargs), limiter=_state["limiter"]
    )


async def _run_ctl(fn, *args, **kwargs):
    """Control-plane variant of _run: does not compete with data queries."""
    return await anyio.to_thread.run_sync(
        functools.partial(fn, *args, **kwargs), limiter=_state["ctl_limiter"]
    )


async def _probe(fn, timeout_s: float = 5.0):
    """Bounded health probe: returns None instead of hanging on a wedged
    backend. On timeout the worker thread is abandoned (it cannot be killed);
    it exits when the underlying call's own network timeout fires."""
    try:
        with anyio.fail_after(timeout_s):
            return await anyio.to_thread.run_sync(
                fn, limiter=_state["ctl_limiter"], abandon_on_cancel=True
            )
    except TimeoutError:
        return None


async def _wait_job(job, timeout: float) -> bool:
    """Wait without occupying a worker-thread slot needed by real queries."""
    deadline = time.monotonic() + timeout
    while not job.event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        await anyio.sleep(min(0.05, remaining))
    return True


def _listings() -> ListingService:
    if _state["backend"] is None:
        raise HTTPException(503, f"catalog backend unavailable: {_state['backend_error']}")
    return _state["listings"]


def _rollups() -> RollupService:
    if _state["backend"] is None:
        raise HTTPException(503, f"catalog backend unavailable: {_state['backend_error']}")
    return _state["rollups"]


# ---- views ------------------------------------------------------------------


@app.get("/api/views")
async def api_views():
    cfg = _state["cfg"]
    if cfg.mock and _state["backend"] is not None:
        return {"views": _state["backend"].get_views(), "vms_unavailable": False}
    cache: TTLCache = _state["ctl_cache"]
    cached = cache.get(("views",))
    if cached is not None:
        return cached
    result = await _run_ctl(_state["vms"].get_views)
    cache.put(("views",), result)
    return result


# ---- listings ---------------------------------------------------------------


@app.get("/api/ls")
async def api_ls(
    path: str = Query(..., min_length=1),
    page: int = 1,
    page_size: int | None = None,
    sort: str = "name",
    order: str = "asc",
    type: str = "all",
    name_filter: str = "",
):
    if type not in ("all", "file", "dir", "other"):
        raise HTTPException(400, "type must be all|file|dir|other")
    try:
        return await _run(
            _listings().list_page,
            path,
            page=page,
            page_size=page_size,
            sort=sort,
            order=order,
            type_filter=type,
            name_filter=name_filter,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"catalog query failed: {e}") from e


# ---- rollups ----------------------------------------------------------------


@app.get("/api/rollup")
async def api_rollup(
    path: str = Query(..., min_length=1),
    depth: int = 1,
    child_limit: int | None = Query(None, ge=1),
):
    cfg = _state["cfg"]
    rollups = _rollups()
    prefix = normalize_prefix(path)
    if depth < 1 or depth > cfg.rollup_max_depth:
        raise HTTPException(400, f"depth must be between 1 and {cfg.rollup_max_depth}")
    if prefix == "/" and not cfg.allow_root:
        raise HTTPException(
            403,
            "Refusing a rollup of the entire namespace ('/'); set CATWALK_ALLOW_ROOT=1 to enable.",
        )
    cached = rollups.cached(prefix, depth)
    if cached is not None:
        return rollups.public_result({**cached, "from_cache": True}, child_limit=child_limit)

    jobs: JobManager = _state["jobs"]
    try:
        job, cancel_token = jobs.submit(
            ("rollup", prefix, depth), lambda cb: rollups.compute(prefix, depth, rows_cb=cb)
        )
    except JobQueueFullError as e:
        raise HTTPException(429, str(e), headers={"Retry-After": "2"}) from e
    finished = await _wait_job(job, cfg.rollup_sync_timeout)
    if finished:
        if job.status in ("error", "cancelled"):
            raise HTTPException(job.error_status, f"rollup failed: {job.error}")
        return rollups.public_result(job.result, child_limit=child_limit)
    status_url = f"/api/rollup/status?job_id={job.id}"
    if child_limit is not None:
        status_url += f"&child_limit={child_limit}"
    # cancel_token is per-client even when the job itself is shared, so it
    # goes in the 202 body only — never in shared status snapshots.
    return JSONResponse(
        status_code=202,
        content={**job.snapshot(), "status_url": status_url, "cancel_token": cancel_token},
    )


@app.get("/api/rollup/status")
async def api_rollup_status(job_id: str, child_limit: int | None = Query(None, ge=1)):
    job = _state["jobs"].get(job_id)
    if job is None:
        raise HTTPException(
            404,
            "unknown job_id (completed jobs are pruned after a few minutes; re-request the rollup)",
        )
    snap = job.snapshot()
    if job.status == "done":
        snap["result"] = _rollups().public_result(job.result, child_limit=child_limit)
    return snap


@app.delete("/api/rollup/status")
async def api_rollup_cancel(job_id: str, cancel_token: str = Query(..., min_length=1)):
    job = _state["jobs"].cancel(job_id, watcher=cancel_token)
    if job is None:
        raise HTTPException(404, "unknown job_id")
    # Still "running" here means other clients are watching the shared job:
    # this client detached, the computation continues for them.
    return job.snapshot()


# ---- capacity (VMS, best-effort sampled estimates) --------------------------


@app.get("/api/capacity")
async def api_capacity(path: str = Query(..., min_length=1)):
    cache: TTLCache = _state["ctl_cache"]
    key = ("capacity", path)
    cached = cache.get(key)
    if cached is not None:
        return cached
    # Failures ("VMS unreachable") are cached too: a down VMS should not be
    # re-probed on every panel refresh.
    result = await _run_ctl(_state["vms"].get_capacity, path)
    cache.put(key, result)
    return result


# ---- CSV export -------------------------------------------------------------


def _safe_content_disposition(filename: str) -> str:
    filename = "".join(ch for ch in filename if " " <= ch != "\x7f")
    fallback = re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._") or "catwalk.csv"
    fallback = fallback[:180]
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(filename, safe='')}"


def _spreadsheet_safe(table: pa.Table) -> pa.Table:
    """Prevent catalog-controlled strings from becoming spreadsheet formulas."""
    columns = []
    for field, column in zip(table.schema, table.columns, strict=True):
        if pa.types.is_string(field.type):
            dangerous = pc.match_substring_regex(column, pattern=r"^[=+@\-\t\r\n]")
            column = pc.if_else(
                dangerous, pc.binary_join_element_wise(pa.scalar("'"), column, ""), column
            )
        columns.append(column)
    return pa.table(columns, names=table.column_names)


def _csv_response(table: pa.Table, filename: str, extra_headers: dict | None = None) -> Response:
    # Ownership transfers to the response iterator, whose finally block closes it.
    buf = tempfile.SpooledTemporaryFile(  # noqa: SIM115
        max_size=8 * 1024**2, mode="w+b"
    )
    try:
        pa_csv.write_csv(_spreadsheet_safe(table), buf)
    except Exception:
        buf.close()
        raise
    buf.seek(0)

    def chunks():
        try:
            while data := buf.read(1024**2):
                yield data
        finally:
            buf.close()

    headers = {"Content-Disposition": _safe_content_disposition(filename)}
    headers.update(extra_headers or {})
    return StreamingResponse(chunks(), media_type="text/csv", headers=headers)


@app.get("/api/export/listing")
async def api_export_listing(
    path: str = Query(..., min_length=1),
    sort: str = "name",
    order: str = "asc",
    type: str = "all",
    name_filter: str = "",
):
    if type not in ("all", "file", "dir", "other"):
        raise HTTPException(400, "type must be all|file|dir|other")
    try:
        table, truncated = await _run(
            _listings().listing_table,
            path,
            sort=sort,
            order=order,
            type_filter=type,
            name_filter=name_filter,
        )
    except Exception as e:
        raise HTTPException(502, f"catalog query failed: {e}") from e
    cols, names = [], []
    for name in table.column_names:
        col = table.column(name)
        if name in ("mtime", "atime"):
            # timestamp -> "2026-07-17 17:30:29.902030175" -> ISO-8601 "T"
            col = pc.replace_substring(
                col.cast(pa.string()), pattern=" ", replacement="T", max_replacements=1
            )
        cols.append(col)
        names.append(name)
    safe = normalize_prefix(path).strip("/").replace("/", "_") or "root"
    suffix = "-TRUNCATED" if truncated else ""
    return _csv_response(
        pa.table(dict(zip(names, cols, strict=True))),
        f"catwalk-listing-{safe}{suffix}.csv",
        {"X-Catwalk-Truncated": str(truncated).lower(), "X-Catwalk-Row-Count": str(table.num_rows)},
    )


@app.get("/api/export/rollup")
async def api_export_rollup(path: str = Query(..., min_length=1), depth: int = 1):
    cfg = _state["cfg"]
    if depth < 1 or depth > cfg.rollup_max_depth:
        raise HTTPException(400, f"depth must be between 1 and {cfg.rollup_max_depth}")
    cached = _rollups().cached(path, depth)
    if cached is None:
        raise HTTPException(
            404, "rollup not computed yet -- request /api/rollup for this path first"
        )
    children = cached["children"]
    table = pa.table(
        {
            "folder": [c["name"] for c in children],
            "file_count": [c["file_count"] for c in children],
            "total_bytes": [c["total_bytes"] for c in children],
            "total_used": [c["total_used"] for c in children],
            "last_modified": [c["last_modified"] for c in children],
            "last_accessed": [c["last_accessed"] for c in children],
        }
    )
    safe = normalize_prefix(path).strip("/").replace("/", "_") or "root"
    return _csv_response(table, f"catwalk-rollup-{safe}.csv")


# ---- health -----------------------------------------------------------------


@app.get("/api/health")
async def api_health():
    cfg = _state["cfg"]
    if _state["backend"] is None:
        backend_health = {"vastdb": f"error: {_state['backend_error']}", "catalog_reachable": False}
    else:
        backend_health = await _probe(_state["backend"].health)
        if backend_health is None:
            backend_health = {
                "vastdb": "error: health probe timed out",
                "catalog_reachable": False,
                # The probe drowned, but the pinned epoch is known locally --
                # report it so a busy cluster doesn't read as "pinning broken".
                "catalog_snapshot": _state["backend"].peek_epoch(),
            }
    if cfg.mock:
        vms_health = "mock"
    else:
        vms_health = await _probe(_state["vms"].health) or "error: health probe timed out"
    warmer = _state.get("warmer")
    caches = {"control": _state["ctl_cache"].stats()}
    if _state["backend"] is not None:
        caches["listings"] = _state["listings"].cache.stats()
        caches["rollups"] = _state["rollups"].cache.stats()
    return {
        "vastdb": backend_health.get("vastdb"),
        "catalog_reachable": backend_health.get("catalog_reachable", False),
        "vms": vms_health,
        "mode": "mock" if cfg.mock else "vastdb",
        "snapshot_hint": SNAPSHOT_HINT,
        "catalog_snapshot": backend_health.get("catalog_snapshot"),
        "warmer": warmer.stats() if warmer and warmer.enabled else None,
        "caches": caches,
    }


# ---- static frontend (mounted last so /api/* wins) --------------------------

app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
