"""Cache warmer: BFS-warm listings under configured roots at startup, then
keep them warm with periodic refresh passes.

Why this shape: a single catalog query has a ~2s latency floor regardless of
client fan-out (the catalog is server-bound), but queries for *different*
directories are independent -- so first-view responsiveness comes from having
listings already cached, not from more per-query concurrency. Warmed clicks
serve in milliseconds vs 2-8s cold.

Each pass walks breadth-first from the warm roots down to CATWALK_WARM_DEPTH
levels, re-querying only listings that would expire before the next pass
(entries recently refreshed by real browsing are skipped). The walk is capped
at CATWALK_WARM_MAX_DIRS listings per pass so a wide namespace cannot turn
warming into a namespace scan.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from .catalog import ListingService, normalize_prefix
from .config import Config

log = logging.getLogger("catwalk.warmer")

# Refresh margin: a pass re-queries entries expiring within interval plus
# this buffer, so an entry cannot go cold between two passes.
_PASS_MARGIN_SECS = 60.0


class CacheWarmer:
    """Owns a small worker pool + a scheduler thread. Disabled (no threads
    started) unless warm paths are configured."""

    def __init__(self, listings: ListingService, cfg: Config):
        self.listings = listings
        self.paths = [normalize_prefix(p) for p in (cfg.warm_paths or [])]
        self.depth = cfg.warm_depth
        self.max_dirs = cfg.warm_max_dirs
        self.threads = max(1, cfg.warm_threads)
        # Default interval: refresh comfortably inside the cache TTL.
        self.interval = cfg.warm_interval or cfg.cache_ttl * 0.75
        self.last_pass: dict | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pool: ThreadPoolExecutor | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.paths) and self.depth >= 0

    def start(self):
        if not self.enabled or self._thread is not None:
            return
        self._stop.clear()
        self._pool = ThreadPoolExecutor(max_workers=self.threads, thread_name_prefix="warmer")
        self._thread = threading.Thread(target=self._loop, name="warmer-loop", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=True)
        # A slow pass may have outlived the first join while its pool drained.
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        self._thread = None
        self._pool = None

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.run_pass()
            except Exception:
                log.exception("cache warm pass failed")
            self._stop.wait(self.interval)

    def run_pass(self, min_ttl: float | None = None) -> int:
        """One BFS warm pass. Returns the number of listings visited.

        Runnable standalone (tests, one-shot warms): builds a temporary pool
        when start() has not created one.
        """
        if min_ttl is None:
            min_ttl = self.interval + _PASS_MARGIN_SECS
        pool = self._pool
        own_pool = pool is None
        if own_pool:
            pool = ThreadPoolExecutor(max_workers=self.threads, thread_name_prefix="warmer")
        t0 = time.monotonic()
        visited = 0
        try:
            frontier = list(dict.fromkeys(self.paths))
            seen = set(frontier)
            for _level in range(self.depth + 1):
                if not frontier or self._stop.is_set():
                    break
                batch = frontier[: max(0, self.max_dirs - visited)]
                if len(batch) < len(frontier):
                    log.warning(
                        "warm cap CATWALK_WARM_MAX_DIRS=%d reached; %d dirs not warmed",
                        self.max_dirs,
                        len(frontier) - len(batch),
                    )
                if not batch:
                    break
                children_per_dir = list(pool.map(self._warm_one, batch, [min_ttl] * len(batch)))
                visited += len(batch)
                frontier = []
                for path, children in zip(batch, children_per_dir, strict=True):
                    for name in children:
                        child = f"{path}{name}/"
                        if child not in seen:
                            seen.add(child)
                            frontier.append(child)
        finally:
            if own_pool:
                pool.shutdown(wait=True)
        self.last_pass = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "listings": visited,
            "elapsed_s": round(time.monotonic() - t0, 1),
        }
        log.info("cache warm pass: %d listings in %ss", visited, self.last_pass["elapsed_s"])
        return visited

    def _warm_one(self, path: str, min_ttl: float) -> list[str]:
        try:
            return self.listings.warm_listing(path, min_ttl=min_ttl)
        except Exception as e:
            log.warning("warm failed for %s: %s", path, e)
            return []

    def stats(self) -> dict:
        return {
            "paths": self.paths,
            "depth": self.depth,
            "interval_s": round(self.interval, 1),
            "last_pass": self.last_pass,
        }
