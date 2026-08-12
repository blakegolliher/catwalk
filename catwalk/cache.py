"""TTL + LRU (by bytes) caches for listings, rollups, and views.

One instance per cache kind. Values are opaque; the caller supplies a byte
cost so Arrow tables can be accounted honestly. Thread-safe -- catalog work
runs in a thread pool.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Any


@dataclass
class _Entry:
    value: Any
    expires: float
    nbytes: int


class TTLCache:
    def __init__(self, ttl: float, max_bytes: int = 0):
        self.ttl = ttl
        self.max_bytes = max_bytes
        self._d: OrderedDict[Hashable, _Entry] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    def _prune_expired_locked(self, now: float):
        stale = [key for key, entry in self._d.items() if now >= entry.expires]
        for key in stale:
            self._bytes -= self._d.pop(key).nbytes

    def get(self, key: Hashable):
        with self._lock:
            e = self._d.get(key)
            if e is None:
                return None
            if time.monotonic() >= e.expires:
                del self._d[key]
                self._bytes -= e.nbytes
                return None
            self._d.move_to_end(key)
            return e.value

    def expires_in(self, key: Hashable) -> float | None:
        """Seconds of life left for key, or None if absent/expired.

        Does not refresh LRU position -- callers deciding whether to
        *replace* an entry should not also mark it recently used.
        """
        with self._lock:
            e = self._d.get(key)
            if e is None:
                return None
            remaining = e.expires - time.monotonic()
            if remaining <= 0:
                del self._d[key]
                self._bytes -= e.nbytes
                return None
            return remaining

    def put(self, key: Hashable, value: Any, nbytes: int = 0):
        with self._lock:
            self._prune_expired_locked(time.monotonic())
            old = self._d.pop(key, None)
            if old is not None:
                self._bytes -= old.nbytes
            # A single oversized value must not make a documented byte bound
            # permanently false. Return it to the caller, but do not cache it.
            if self.max_bytes and nbytes > self.max_bytes:
                return False
            self._d[key] = _Entry(value, time.monotonic() + self.ttl, nbytes)
            self._bytes += nbytes
            if self.max_bytes:
                while self._bytes > self.max_bytes:
                    _, evicted = self._d.popitem(last=False)
                    self._bytes -= evicted.nbytes
            return True

    def prune(self):
        """Remove every expired entry, including keys that are never revisited."""
        with self._lock:
            self._prune_expired_locked(time.monotonic())

    def invalidate(self, key: Hashable = None):
        with self._lock:
            if key is None:
                self._d.clear()
                self._bytes = 0
            else:
                e = self._d.pop(key, None)
                if e is not None:
                    self._bytes -= e.nbytes

    def get_or_compute(self, key: Hashable, compute: Callable[[], tuple[Any, int]]):
        """Return cached value, or compute() -> (value, nbytes) and cache it.

        compute runs outside the lock; concurrent misses may compute twice
        (harmless -- last write wins), which beats serializing slow scans.
        """
        v = self.get(key)
        if v is not None:
            return v, True
        value, nbytes = compute()
        self.put(key, value, nbytes)
        return value, False

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return self._bytes

    def __len__(self) -> int:
        with self._lock:
            return len(self._d)
