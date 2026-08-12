"""Catalog access layer: listings, pagination, and descendant rollups.

Query patterns are ported from catalog_folder_report.py / customer_verbatim.py:
  - directory listing = equality on parent_path WITH trailing slash
  - rollup = streaming scan of FILE rows under a prefix, aggregated per batch
    with pyarrow (aggregate_reader / group_key / FolderStats)

The real backend talks to data VIPs via the vastdb SDK (never the VMS
address). The mock backend (catwalk.mock) implements the same primitives.
"""

from __future__ import annotations

import contextlib
import logging
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc

from .cache import TTLCache
from .config import Config

# The catalog columns Catwalk uses, with their types (the real table has
# more). The mock backend generates tables with exactly this schema.
SCHEMA = pa.schema(
    [
        ("parent_path", pa.string()),
        ("name", pa.string()),
        ("element_type", pa.string()),
        ("size", pa.int64()),
        ("used", pa.int64()),
        ("mtime", pa.timestamp("ns")),
        ("atime", pa.timestamp("ns")),
        ("owner_name", pa.string()),
        ("extension", pa.string()),
        ("nlinks", pa.int64()),
    ]
)

LISTING_COLUMNS = [
    "name",
    "element_type",
    "size",
    "used",
    "mtime",
    "atime",
    "owner_name",
    "extension",
    "nlinks",
]
ROLLUP_COLUMNS = ["parent_path", "size", "used", "atime", "mtime"]

SORT_KEYS = {"name", "size", "mtime", "atime", "owner_name"}

SNAPSHOT_HINT = "Catalog data is snapshot-based (typically ≤30 min behind the live filesystem)"

_MAX_SORT_VARIANTS = 3

# How long an interactive request will wait for an in-flight prefetch of the
# same directory before giving up and querying itself. Generous versus the
# 2-8s typical listing, but bounded: a prefetch hung on a dead VIP must not
# pin a user request for minutes (a fresh query may reach a healthy VIP).
_PREFETCH_JOIN_TIMEOUT_S = 15.0

log = logging.getLogger("catwalk.catalog")


class RollupTooWideError(RuntimeError):
    """The requested grouping would create an unsafe in-memory result."""

    status_code = 413


# ---- path / formatting helpers (from catalog_folder_report.py) -------------


def normalize_prefix(path: str) -> str:
    """Ensure the prefix ends with '/' so /admin cannot match /administrator."""
    path = "/" + path.strip("/")
    return path if path == "/" else path + "/"


def ns_to_iso(ns: int | None) -> str:
    """int64 epoch-nanoseconds -> ISO-8601 UTC string with nanosecond precision."""
    if ns is None:
        return ""
    dt = datetime.fromtimestamp(ns // 10**9, tz=timezone.utc).replace(tzinfo=None)
    return f"{dt.isoformat()}.{ns % 10**9:09d}"


def group_key(parent_path: str, prefix: str, depth: int) -> str:
    """Map a file's parent_path to its top-level group below prefix."""
    parts = [p for p in parent_path[len(prefix) :].split("/") if p]
    return "/".join(parts[:depth]) if parts else "."


def newest(current_ns: int | None, candidate_ns: int | None) -> int | None:
    if current_ns is None:
        return candidate_ns
    if candidate_ns is None:
        return current_ns
    return max(current_ns, candidate_ns)


class FolderStats:
    """Running totals for one folder group."""

    __slots__ = ("atime_ns", "bytes", "files", "mtime_ns", "used")

    def __init__(self):
        self.files = 0
        self.bytes = 0
        self.used = 0
        self.mtime_ns = None
        self.atime_ns = None

    def add(
        self,
        files: int,
        nbytes: int | None,
        used: int | None,
        mtime_ns: int | None,
        atime_ns: int | None,
    ) -> None:
        self.files += files
        self.bytes += nbytes or 0
        self.used += used or 0
        self.mtime_ns = newest(self.mtime_ns, mtime_ns)
        self.atime_ns = newest(self.atime_ns, atime_ns)


def aggregate_reader(
    reader: Iterable[pa.RecordBatch],
    prefix: str,
    depth: int,
    groups: dict[str, FolderStats],
    rows_cb: Callable[[int], None] | None = None,
    max_groups: int = 0,
) -> int:
    """Consume RecordBatches, folding rows into per-group FolderStats.

    Each batch is grouped with pyarrow (vectorized); only the per-batch group
    summaries touch Python. Dictionary encoding already visits each distinct
    parent path only once per batch; deliberately do not retain every parent
    path across the whole subtree.
    """
    rows = 0
    for batch in reader:
        if batch.num_rows == 0:
            continue
        enc = pc.dictionary_encode(batch.column("parent_path"))
        keys = []
        for path in enc.dictionary.to_pylist():
            keys.append(group_key(path, prefix, depth))
        summary = (
            pa.table(
                {
                    "key": pc.take(pa.array(keys, pa.string()), enc.indices),
                    "size": batch.column("size"),
                    "used": batch.column("used"),
                    "mtime": batch.column("mtime").cast(pa.int64()),
                    "atime": batch.column("atime").cast(pa.int64()),
                }
            )
            .group_by("key")
            .aggregate(
                [
                    # mode="all": FILE rows with a null size are still files;
                    # the default only_valid count would drop them.
                    ("size", "count", pc.CountOptions(mode="all")),
                    ("size", "sum"),
                    ("used", "sum"),
                    ("mtime", "max"),
                    ("atime", "max"),
                ]
            )
        )
        for row in summary.to_pylist():
            key = row["key"]
            if key not in groups and max_groups and len(groups) >= max_groups:
                raise RollupTooWideError(
                    f"rollup exceeds CATWALK_ROLLUP_MAX_GROUPS={max_groups}; "
                    "use a smaller depth or a narrower path"
                )
            groups.setdefault(key, FolderStats()).add(
                row["size_count"],
                row["size_sum"],
                row["used_sum"],
                row["mtime_max"],
                row["atime_max"],
            )
        rows += batch.num_rows
        if rows_cb:
            rows_cb(rows)
    return rows


def merge_groups(
    groups: dict[str, FolderStats],
    partial: dict[str, FolderStats],
    max_groups: int = 0,
) -> None:
    for key, stats in partial.items():
        if key not in groups and max_groups and len(groups) >= max_groups:
            raise RollupTooWideError(
                f"rollup exceeds CATWALK_ROLLUP_MAX_GROUPS={max_groups}; "
                "use a smaller depth or a narrower path"
            )
        groups.setdefault(key, FolderStats()).add(
            stats.files, stats.bytes, stats.used, stats.mtime_ns, stats.atime_ns
        )


# ---- real backend (vastdb SDK over data VIPs) -------------------------------

CATALOG_BUCKET = "vast-big-catalog-bucket"


def newest_catalog_snapshot(api, prefix: str) -> str | None:
    """Name of the newest catalog snapshot, or None if there are none.

    One (paged) S3 LIST against the catalog bucket -- no transaction, no
    data scan. Snapshot names embed a zero-padded UTC timestamp
    (big_catalog_2026-08-12_19_01_25_UTC), so within one naming prefix the
    lexicographic max is the newest.
    """
    names: list[str] = []
    token = None
    while True:
        page, truncated, token = api.list_snapshots(
            bucket=CATALOG_BUCKET, name_prefix=prefix, next_token=token
        )
        names.extend(page)
        if not truncated or not page:
            break
    snaps = [n.strip("/").removeprefix(".snapshot/") for n in names]
    return max(snaps) if snaps else None


def _drop_sdk_concurrency_notice(record: logging.LogRecord) -> bool:
    return "heuristic for concurrency" not in record.getMessage()


def _silence_sdk_noise():
    """Drop the SDK's per-query 'Using the number of endpoints as a heuristic
    for concurrency.' warning -- vastdb logs it unconditionally on every
    select(), which is one line of noise per listing. Other vastdb.table
    warnings still pass through."""
    log = logging.getLogger("vastdb.table")
    if _drop_sdk_concurrency_notice not in log.filters:
        log.addFilter(_drop_sdk_concurrency_notice)


class VastBackend:
    """Catalog queries via the vastdb SDK. One session per process.

    Tenant scoping is automatic and server-side: the catalog only ever
    returns rows belonging to the tenant of the S3 identity used to connect
    (keys are bound to their tenant's VIP pool). To browse one tenant, run
    Catwalk with that tenant's key and VIP -- do NOT try to filter on the
    catalog's tenant_id column; it holds internal ids that do not match VMS
    tenant ids (default-tenant rows show -1)."""

    def __init__(self, cfg: Config):
        import vastdb  # deferred so mock mode never needs the SDK

        _silence_sdk_noise()
        if not (cfg.endpoint and cfg.access_key and cfg.secret_key):
            raise RuntimeError(
                "VASTDB_ENDPOINT / VASTDB_ACCESS_KEY / VASTDB_SECRET_KEY must be "
                "set (or run with CATWALK_MOCK=1)"
            )
        self.cfg = cfg
        self.data_endpoints = cfg.resolved_data_endpoints()
        # SDK concurrency == len(data_endpoints): repeat VIPs to hit the
        # configured concurrency instead of being capped at the VIP count.
        self.fanout = cfg.fanout_endpoints()
        # Without a timeout the SDK's requests block forever on a black-holed
        # VIP, permanently pinning a slot of the shared query budget. The
        # timeout is per socket read, so long streaming scans that keep
        # delivering batches are unaffected.
        self.session = vastdb.connect(
            endpoint=cfg.endpoint,
            access=cfg.access_key,
            secret=cfg.secret_key,
            timeout=cfg.query_timeout or None,
        )
        self._epoch: str | None = None
        self._epoch_checked = float("-inf")
        self._epoch_lock = threading.Lock()

    def _query_config(self, num_splits: int | None = None):
        from vastdb.config import QueryConfig

        return QueryConfig(data_endpoints=self.fanout, num_splits=num_splits or self.cfg.num_splits)

    def current_epoch(self) -> str | None:
        """Newest catalog snapshot name, refreshed at most every
        cfg.snapshot_poll seconds. None when pinning is disabled or the
        cluster has no catalog snapshots -- queries then run against the
        live table and caching degrades to pure TTL.

        Non-blocking for concurrent callers: whoever finds the value stale
        marks it fresh first and does the (single) LIST; everyone else keeps
        using the previous epoch meanwhile.
        """
        if not self.cfg.snapshot_pin:
            return None
        now = time.monotonic()
        with self._epoch_lock:
            if now - self._epoch_checked < self.cfg.snapshot_poll:
                return self._epoch
            self._epoch_checked = now
            previous = self._epoch
        try:
            fresh = newest_catalog_snapshot(self.session.api, self.cfg.snapshot_prefix)
        except Exception as e:
            log.warning("catalog snapshot listing failed (%s); keeping epoch %r", e, previous)
            return previous
        if fresh != previous:
            log.info("catalog snapshot epoch: %r -> %r", previous, fresh)
        with self._epoch_lock:
            self._epoch = fresh
        return fresh

    def peek_epoch(self) -> str | None:
        """Last-resolved epoch without any network I/O (for health reporting
        even when the cluster is too busy to answer a probe)."""
        with self._epoch_lock:
            return self._epoch

    def _drop_epoch(self, epoch: str) -> None:
        """Forget a bad epoch so the next current_epoch() re-resolves."""
        with self._epoch_lock:
            if self._epoch == epoch:
                self._epoch = None
                self._epoch_checked = float("-inf")

    def _catalog(self, tx, epoch: str | None):
        """Catalog table pinned to the epoch snapshot, or live when unpinned.

        A pinned lookup that fails (snapshot aged out between polls) drops
        the epoch and falls back to the live table rather than erroring the
        request; the cached result is then at most one TTL stale.
        """
        if epoch:
            from vastdb.bucket import Bucket

            try:
                snap = Bucket(name=f"{CATALOG_BUCKET}/.snapshot/{epoch}", tx=tx)
                return tx.catalog(snapshot=snap)
            except Exception as e:
                log.warning("catalog snapshot %s unusable (%s); querying live", epoch, e)
                self._drop_epoch(epoch)
        return tx.catalog()

    def list_dir(
        self,
        path: str,
        columns: list[str],
        element_type: str | None = None,
        name_contains: str | None = None,
        epoch: str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Yield RecordBatches for one directory (equality on parent_path).

        The name filter is pushed into the predicate when the SDK supports
        it; otherwise it is applied client-side per batch (either way a
        filtered listing can escape the truncation cap).
        """
        from ibis import _

        base = _.parent_path == path
        if element_type == "OTHER":
            base = base & ~_.element_type.isin(("FILE", "DIR"))
        elif element_type:
            base = base & (_.element_type == element_type)
        if not name_contains:
            yield from self._run(base, columns, None, epoch)
            return
        # Decide pushdown-vs-client-side on the FIRST batch, so a fallback
        # can never duplicate rows already yielded.
        gen = self._run(base & _.name.contains(name_contains), columns, None, epoch)
        try:
            first = next(gen)
        except StopIteration:
            return
        except (NotImplementedError, ValueError, TypeError):
            gen.close()
            yield from self._run(base, columns, name_contains, epoch)
            return
        yield first
        yield from gen

    def _run(
        self, predicate, columns: list[str], client_name_filter: str | None, epoch: str | None
    ) -> Iterator[pa.RecordBatch]:
        with self.session.transaction() as tx:
            reader = self._catalog(tx, epoch).select(
                columns=columns, predicate=predicate, config=self._query_config()
            )
            for batch in reader:
                if client_name_filter and batch.num_rows:
                    mask = pc.match_substring(batch.column("name"), client_name_filter)
                    batch = batch.filter(mask)
                if batch.num_rows:
                    yield batch

    def scan_subtree_files(
        self, prefix: str, columns: list[str], epoch: str | None = None
    ) -> Iterator[pa.RecordBatch]:
        """Yield RecordBatches of every FILE row under prefix (recursive)."""
        from ibis import _

        with self.session.transaction() as tx:
            reader = self._catalog(tx, epoch).select(
                columns=columns,
                predicate=(_.element_type == "FILE") & (_.parent_path.startswith(prefix)),
                config=self._query_config(),
            )
            yield from reader

    def rollup_groups(
        self,
        prefix: str,
        depth: int,
        rows_cb: Callable[[int], None] | None = None,
        epoch: str | None = None,
    ) -> tuple[dict[str, FolderStats], int]:
        """Pipelined rollup: one select() stream (the SDK fans fetching out
        across the duplicated endpoint list), consumed through a bounded set
        of aggregation futures so transport and per-batch group_by overlap.
        pyarrow releases the GIL for aggregation. A reader or worker failure
        cancels the remaining bounded work instead of stranding queue workers.

        Returns (groups, total_rows).
        """
        from ibis import _

        nthreads = self.cfg.rollup_threads
        max_pending = nthreads * 2
        max_groups = self.cfg.rollup_max_groups
        pool = ThreadPoolExecutor(max_workers=nthreads, thread_name_prefix="rollup-agg")
        pending = set()
        groups, total = {}, 0

        def aggregate_batch(batch):
            partial = {}
            rows = aggregate_reader([batch], prefix, depth, partial, max_groups=max_groups)
            return rows, partial

        def collect(done):
            nonlocal total
            for future in done:
                rows, partial = future.result()
                merge_groups(groups, partial, max_groups=max_groups)
                total += rows

        try:
            with self.session.transaction() as tx:
                reader = self._catalog(tx, epoch).select(
                    columns=ROLLUP_COLUMNS,
                    predicate=(_.element_type == "FILE") & (_.parent_path.startswith(prefix)),
                    config=self._query_config(),
                )
                fed = 0
                for batch in reader:
                    if batch.num_rows == 0:
                        continue
                    pending.add(pool.submit(aggregate_batch, batch))
                    fed += batch.num_rows
                    if rows_cb:
                        rows_cb(fed)
                    if len(pending) >= max_pending:
                        done, pending = wait(pending, return_when=FIRST_COMPLETED)
                        collect(done)
                while pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    collect(done)
        finally:
            for future in pending:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
        return groups, total

    def list_child_dirs(
        self, path: str, limit: int | None = None, epoch: str | None = None
    ) -> list[str]:
        from ibis import _

        with self.session.transaction() as tx:
            dirs = (
                self._catalog(tx, epoch)
                .select(
                    columns=["name"],
                    predicate=(_.element_type == "DIR") & (_.parent_path == path),
                    config=self._query_config(),
                    limit_rows=limit,
                )
                .read_all()
            )
        return dirs.column("name").to_pylist()

    def health(self) -> dict:
        from ibis import _

        epoch = self.peek_epoch()
        try:
            with self.session.transaction() as tx:
                tx.catalog().select(
                    columns=["name"], predicate=(_.parent_path == "/"), limit_rows=1
                ).read_all()
            return {
                "vastdb": "ok",
                "catalog_reachable": True,
                "mode": "vastdb",
                "catalog_snapshot": epoch,
            }
        except Exception as e:  # surfaced in /api/health, never raised to UI
            return {
                "vastdb": f"error: {e}",
                "catalog_reachable": False,
                "mode": "vastdb",
                "catalog_snapshot": epoch,
            }


def make_backend(cfg: Config):
    if cfg.mock:
        from .mock import MockBackend

        return MockBackend()
    return VastBackend(cfg)


# ---- listing service: cache + sort + paginate -------------------------------


class ListingService:
    """Runs directory listings once, caches the Arrow table, serves pages
    as memory slices. Sorting re-sorts the cached table (never re-queries);
    the most recent sorted variants are memoized per listing."""

    def __init__(self, backend, cfg: Config, query_gate=None):
        self.backend = backend
        self.cfg = cfg
        self._query_gate = query_gate
        self.cache = TTLCache(cfg.cache_ttl, cfg.cache_max_bytes)
        # A small scheduling pool keeps speculative work out of anyio's HTTP
        # workers; query_gate still enforces the process-wide backend budget.
        self._prefetch_pool = (
            ThreadPoolExecutor(max_workers=2, thread_name_prefix="prefetch")
            if cfg.prefetch_children > 0
            else None
        )
        self._prefetch_futures: dict[str, Future] = {}
        self._prefetch_lock = threading.Lock()

    def close(self) -> None:
        if self._prefetch_pool:
            self._prefetch_pool.shutdown(wait=True, cancel_futures=True)

    def clamp_page_size(self, page_size: int | None) -> int:
        requested = self.cfg.page_default if page_size is None else int(page_size)
        return max(20, min(self.cfg.page_max, requested))

    def _query_context(self):
        return self._query_gate if self._query_gate is not None else contextlib.nullcontext()

    def _base_listing(
        self, path: str, type_filter: str, name_filter: str, _join_prefetch: bool = True
    ) -> tuple[dict, bool]:
        """Cached (table, truncated) for one directory + filters, plus
        whether it was served from cache.

        Keys include the catalog snapshot epoch: a new snapshot means new
        keys, so every entry is invalidated at once and a cached listing
        always describes exactly one catalog state. Old-epoch entries are
        never revisited and age out via TTL/LRU."""
        epoch = self.backend.current_epoch()
        key = (epoch, path, type_filter or "", name_filter or "")

        # If a speculative prefetch of this directory is mid-flight, wait for
        # it rather than racing it with a duplicate scan (the two would
        # contend for the same cluster). Prefetch workers themselves skip
        # this or they would wait on their own future.
        if _join_prefetch:
            with self._prefetch_lock:
                fut = self._prefetch_futures.get(path)
            if fut is not None:
                if fut.cancel():
                    # Still queued: run the query ourselves right now rather
                    # than waiting behind other prefetches for a pool slot.
                    with self._prefetch_lock:
                        self._prefetch_futures.pop(path, None)
                else:
                    # On prefetch failure, fall through to a normal query.
                    try:
                        fut.result(timeout=_PREFETCH_JOIN_TIMEOUT_S)
                    except FutureTimeoutError:
                        log.warning(
                            "prefetch of %s still running after %.0fs; "
                            "querying it directly instead of waiting",
                            path,
                            _PREFETCH_JOIN_TIMEOUT_S,
                        )
                    except Exception:
                        pass

        def compute():
            # A filtered listing is a subset of the unfiltered one: derive it
            # in memory when the unfiltered table is cached and complete
            # (a truncated cache may be missing matching rows -- re-query).
            if type_filter or name_filter:
                full = self.cache.get((epoch, path, "", ""))
                if full is not None and not full["truncated"]:
                    table = full["table"]
                    if type_filter:
                        table = _filter_listing_type(table, type_filter)
                    if name_filter:
                        table = table.filter(pc.match_substring(table.column("name"), name_filter))
                    derived = _listing_entry(table, False)
                    derived["elapsed_s"] = 0.0  # derived in memory, no query
                    return derived, _listing_cost(table)

            return self._query_listing(path, type_filter, name_filter, epoch)

        return self.cache.get_or_compute(key, compute)

    def _query_listing(
        self, path: str, type_filter: str, name_filter: str, epoch: str | None = None
    ) -> tuple[dict, int]:
        """Run the backend query for one listing; returns (entry, nbytes).

        Logs one timing line per query, splitting gate wait (queued behind
        other catalog work) from query time (cluster latency) -- the split
        that tells you whether to tune concurrency or blame the cluster.
        """
        element_type = {"file": "FILE", "dir": "DIR", "other": "OTHER"}.get(type_filter)
        batches, rows, truncated = [], 0, False
        t0 = time.monotonic()
        with self._query_context():
            gate_wait = time.monotonic() - t0
            it = self.backend.list_dir(
                path,
                LISTING_COLUMNS,
                element_type=element_type,
                name_contains=name_filter or None,
                epoch=epoch,
            )
            try:
                for batch in it:
                    if rows + batch.num_rows > self.cfg.listing_cap:
                        batches.append(batch.slice(0, self.cfg.listing_cap - rows))
                        rows = self.cfg.listing_cap
                        truncated = True
                        break
                    batches.append(batch)
                    rows += batch.num_rows
            finally:
                if hasattr(it, "close"):
                    it.close()
        elapsed = time.monotonic() - t0
        filters = (f" type={type_filter}" if type_filter else "") + (
            f" name~{name_filter}" if name_filter else ""
        )
        log.info(
            "listing %s%s: %d rows%s in %.2fs (gate wait %.2fs, epoch %s)",
            path,
            filters,
            rows,
            " TRUNCATED" if truncated else "",
            elapsed,
            gate_wait,
            epoch,
        )
        if batches:
            table = pa.Table.from_batches(batches, schema=batches[0].schema)
        else:
            table = _empty_listing_table()
        entry = _listing_entry(table, truncated)
        entry["elapsed_s"] = round(elapsed, 3)
        return entry, _listing_cost(table)

    def warm_listing(self, path: str, min_ttl: float = 0.0) -> list[str]:
        """Ensure the unfiltered listing of path is cached with at least
        min_ttl seconds of life left; re-query it otherwise. Returns the
        child directory names (the BFS frontier for the cache warmer).

        Bypasses get_or_compute (a fresh-enough entry must be re-queried,
        not returned), so it may race an interactive request computing the
        same key -- harmless, last write wins.
        """
        path = normalize_prefix(path)
        epoch = self.backend.current_epoch()
        key = (epoch, path, "", "")
        entry = None
        remaining = self.cache.expires_in(key)
        if remaining is not None and remaining >= min_ttl:
            entry = self.cache.get(key)
        if entry is None:
            entry, nbytes = self._query_listing(path, "", "", epoch)
            self.cache.put(key, entry, nbytes)
        table = entry["table"]
        dirs = table.filter(pc.equal(table.column("element_type"), "DIR"))
        return dirs.column("name").to_pylist()

    def _sorted_table(self, entry: dict, sort: str, order: str) -> pa.Table:
        skey = (sort, order)
        with entry["sort_lock"]:
            cached = entry["sorted"].get(skey)
            if cached is not None:
                return cached
            table = entry["table"]
            work = table.append_column("_isfile", pc.not_equal(table.column("element_type"), "DIR"))
            if sort == "name":
                keys = [("_isfile", "ascending"), ("name", order)]
            else:
                keys = [(sort, order), ("_isfile", "ascending"), ("name", "ascending")]
            out = work.sort_by(keys).drop_columns(["_isfile"])
            entry["sorted"][skey] = out
            while len(entry["sorted"]) > _MAX_SORT_VARIANTS:
                entry["sorted"].pop(next(iter(entry["sorted"])))
            return out

    def list_page(
        self,
        path: str,
        page: int = 1,
        page_size: int | None = None,
        sort: str = "name",
        order: str = "asc",
        type_filter: str = "all",
        name_filter: str = "",
    ) -> dict:
        path = normalize_prefix(path)
        if sort not in SORT_KEYS:
            sort = "name"
        order = "descending" if order in ("desc", "descending") else "ascending"
        page_size = self.clamp_page_size(page_size)

        entry, from_cache = self._base_listing(
            path, type_filter if type_filter != "all" else "", name_filter.strip()
        )
        table = entry["table"]
        counts = _type_counts(table)
        total = table.num_rows
        pages = max(1, -(-total // page_size))
        page = max(1, min(int(page or 1), pages))

        sorted_tbl = self._sorted_table(entry, sort, order)
        window = sorted_tbl.slice((page - 1) * page_size, page_size)
        self.prefetch_children(path, entry)
        return {
            "path": path,
            "snapshot_hint": SNAPSHOT_HINT,
            "catalog_snapshot": self.backend.current_epoch(),
            "from_cache": from_cache,
            "query_elapsed_s": entry.get("elapsed_s"),
            "total_rows": total,
            "total_dirs": counts.get("DIR", 0),
            "total_files": counts.get("FILE", 0),
            "total_other": total - counts.get("DIR", 0) - counts.get("FILE", 0),
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "truncated": entry["truncated"],
            "entries": _entries_json(window),
        }

    def prefetch_children(self, path: str, entry: dict) -> list[Future]:
        """Warm the listing cache for this directory's first child dirs.

        Fire-and-forget: a click on a child then serves from cache. Cached
        and in-flight children are skipped, and prefetched listings never
        trigger prefetch of grandchildren (only list_page does that).
        Returns the submitted futures (tests drain them).
        """
        if not self._prefetch_pool:
            return []
        epoch = self.backend.current_epoch()
        table = entry["table"]
        dirs = table.filter(pc.equal(table.column("element_type"), "DIR"))
        names = sorted(dirs.column("name").to_pylist())
        futures = []
        for name in names[: self.cfg.prefetch_children]:
            child = f"{path}{name}/"
            if self.cache.get((epoch, child, "", "")) is not None:
                continue
            with self._prefetch_lock:
                if child in self._prefetch_futures:
                    continue
                fut = self._prefetch_pool.submit(self._prefetch_one, child)
                self._prefetch_futures[child] = fut
            fut.add_done_callback(lambda _future, path=child: self._forget_prefetch(path, _future))
            futures.append(fut)
        return futures

    def _prefetch_one(self, child: str) -> None:
        # Speculative work is intentionally silent; an interactive request
        # retries the query and reports any failure.
        with contextlib.suppress(Exception):
            self._base_listing(child, "", "", _join_prefetch=False)

    def _forget_prefetch(self, child: str, future: Future) -> None:
        with self._prefetch_lock:
            if self._prefetch_futures.get(child) is future:
                self._prefetch_futures.pop(child, None)

    def listing_table(
        self,
        path: str,
        sort: str = "name",
        order: str = "asc",
        type_filter: str = "all",
        name_filter: str = "",
    ) -> tuple[pa.Table, bool]:
        """Full sorted table for CSV export (shares the listing cache)."""
        path = normalize_prefix(path)
        entry, _from_cache = self._base_listing(
            path, type_filter if type_filter != "all" else "", name_filter.strip()
        )
        order = "descending" if order in ("desc", "descending") else "ascending"
        return self._sorted_table(entry, sort if sort in SORT_KEYS else "name", order), entry[
            "truncated"
        ]


def _empty_listing_table() -> pa.Table:
    return pa.table({f.name: pa.array([], f.type) for f in SCHEMA if f.name in LISTING_COLUMNS})


def _listing_entry(table: pa.Table, truncated: bool) -> dict:
    return {"table": table, "truncated": truncated, "sorted": {}, "sort_lock": threading.Lock()}


def _listing_cost(table: pa.Table) -> int:
    # Base table plus the maximum number of independently allocated sorts.
    return max(1024, table.nbytes * (1 + _MAX_SORT_VARIANTS))


def _filter_listing_type(table: pa.Table, type_filter: str) -> pa.Table:
    element_type = table.column("element_type")
    if type_filter == "other":
        return table.filter(pc.invert(pc.is_in(element_type, value_set=pa.array(["FILE", "DIR"]))))
    want = {"file": "FILE", "dir": "DIR"}[type_filter]
    return table.filter(pc.equal(element_type, want))


def _type_counts(table) -> dict:
    if table.num_rows == 0:
        return {}
    vc = table.column("element_type").value_counts()
    return {r["values"]: r["counts"] for r in vc.to_pylist()}


def _entries_json(window: pa.Table) -> list[dict]:
    """Format one page of rows at the JSON boundary (ISO timestamps)."""
    mt = window.column("mtime").cast(pa.int64()).to_pylist()
    at = window.column("atime").cast(pa.int64()).to_pylist()
    out = []
    cols = {
        c: window.column(c).to_pylist()
        for c in ("name", "element_type", "size", "used", "owner_name", "extension", "nlinks")
    }
    for i in range(window.num_rows):
        out.append(
            {
                "name": cols["name"][i],
                "element_type": cols["element_type"][i],
                "size": cols["size"][i],
                "used": cols["used"][i],
                "mtime": ns_to_iso(mt[i]),
                "atime": ns_to_iso(at[i]),
                "owner_name": cols["owner_name"][i],
                "extension": cols["extension"][i],
                "nlinks": cols["nlinks"][i],
            }
        )
    return out


# ---- rollup service ---------------------------------------------------------


class RollupService:
    """Descendant aggregation grouped by depth-1 children, with caching.

    No time filters by design: a time-filtered rollup silently misreports
    last-modified/last-accessed (same refusal as the reference script).
    """

    def __init__(self, backend, cfg: Config, query_gate=None):
        self.backend = backend
        self.cfg = cfg
        self._query_gate = query_gate
        self.cache = TTLCache(cfg.cache_ttl, cfg.rollup_cache_max_bytes)

    def cached(self, path: str, depth: int = 1) -> dict | None:
        epoch = self.backend.current_epoch()
        return self.cache.get((epoch, normalize_prefix(path), depth))

    def compute(
        self, path: str, depth: int = 1, rows_cb: Callable[[int], None] | None = None
    ) -> dict:
        prefix = normalize_prefix(path)
        if depth < 1 or depth > self.cfg.rollup_max_depth:
            raise ValueError(f"rollup depth must be between 1 and {self.cfg.rollup_max_depth}")
        if prefix == "/" and not self.cfg.allow_root:
            raise PermissionError(
                "Refusing a rollup of the entire namespace ('/'); "
                "set CATWALK_ALLOW_ROOT=1 to enable."
            )
        if rows_cb:
            rows_cb(0)
        t0 = time.monotonic()
        # One epoch for the whole rollup: the subtree scan and the child-dir
        # seed then read the same catalog snapshot, and the result is cached
        # under the epoch it actually describes.
        epoch = self.backend.current_epoch()
        log.info("rollup start %s depth=%d (epoch %s)", prefix, depth, epoch)
        gate = self._query_gate if self._query_gate is not None else contextlib.nullcontext()
        with gate:
            if hasattr(self.backend, "rollup_groups"):
                groups, total_rows = self.backend.rollup_groups(
                    prefix, depth, rows_cb=rows_cb, epoch=epoch
                )
            else:
                reader = self.backend.scan_subtree_files(prefix, ROLLUP_COLUMNS, epoch=epoch)
                groups = {}
                try:
                    total_rows = aggregate_reader(
                        reader,
                        prefix,
                        depth,
                        groups,
                        rows_cb=rows_cb,
                        max_groups=self.cfg.rollup_max_groups,
                    )
                finally:
                    if hasattr(reader, "close"):
                        reader.close()
            # Seed immediate child directories, so empty folders still show up.
            dirs = self.backend.list_child_dirs(
                prefix, limit=self.cfg.rollup_max_groups + 1, epoch=epoch
            )
            for name in dirs:
                if name not in groups and len(groups) >= self.cfg.rollup_max_groups:
                    raise RollupTooWideError(
                        "rollup exceeds "
                        f"CATWALK_ROLLUP_MAX_GROUPS={self.cfg.rollup_max_groups}; "
                        "use a narrower path"
                    )
                groups.setdefault(name, FolderStats())
        if rows_cb:
            rows_cb(total_rows)
        elapsed = time.monotonic() - t0
        log.info(
            "rollup %s depth=%d: %d rows, %d groups in %.2fs (epoch %s)",
            prefix,
            depth,
            total_rows,
            len(groups),
            elapsed,
            epoch,
        )

        children = [
            {
                "name": key,
                "file_count": s.files,
                "total_bytes": s.bytes,
                "total_used": s.used,
                "last_modified": ns_to_iso(s.mtime_ns),
                "last_accessed": ns_to_iso(s.atime_ns),
            }
            for key, s in groups.items()
        ]
        children.sort(key=lambda c: c["total_bytes"], reverse=True)

        totals = _fold(groups)
        result = {
            "path": prefix,
            "catalog_snapshot": epoch,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_s": round(elapsed, 2),
            "rows_scanned": total_rows,
            "from_cache": False,
            "children": children,
            "totals": {
                "file_count": totals.files,
                "total_bytes": totals.bytes,
                "total_used": totals.used,
                "last_modified": ns_to_iso(totals.mtime_ns),
            },
        }
        self.cache.put((epoch, prefix, depth), result, nbytes=_deep_size(result))
        return result

    def public_result(self, result: dict, child_limit: int | None = None) -> dict:
        """Return an API-sized copy while retaining the full cached export."""
        limit = child_limit or self.cfg.rollup_response_children
        limit = min(limit, self.cfg.rollup_max_groups)
        children = result["children"]
        return {
            **result,
            "children": children[:limit],
            "total_children": len(children),
            "children_truncated": len(children) > limit,
        }


def _fold(groups) -> FolderStats:
    total = FolderStats()
    for s in groups.values():
        total.add(s.files, s.bytes, s.used, s.mtime_ns, s.atime_ns)
    return total


def _deep_size(value, seen=None) -> int:
    """Approximate retained Python memory without double-counting references."""
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(_deep_size(k, seen) + _deep_size(v, seen) for k, v in value.items())
    elif isinstance(value, (list, tuple, set, frozenset)):
        size += sum(_deep_size(item, seen) for item in value)
    return size
