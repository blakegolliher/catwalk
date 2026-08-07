"""Cache warmer: BFS depth, dir cap, TTL-aware refresh."""

from catwalk.catalog import ListingService
from catwalk.config import Config
from catwalk.warmer import CacheWarmer
from mock_catalog import MockBackend


class CountingBackend(MockBackend):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def list_dir(self, *args, **kwargs):
        self.calls += 1
        yield from super().list_dir(*args, **kwargs)


def make(backend, **overrides):
    # Prefetch off: background warms would race the call-count assertions.
    overrides.setdefault("prefetch_children", 0)
    cfg = Config(mock=True, cache_ttl=300.0, **overrides)
    return ListingService(backend, cfg), cfg


def test_warm_pass_covers_depth_and_serves_from_cache():
    b = CountingBackend()
    svc, cfg = make(b, warm_paths=["/projects"], warm_depth=1)
    warmer = CacheWarmer(svc, cfg)
    visited = warmer.run_pass()
    # /projects/ + its 12 proj-* children
    assert visited == 13
    assert warmer.last_pass["listings"] == 13
    calls = b.calls
    page = svc.list_page("/projects/proj-05")
    assert page["total_rows"] > 0
    assert b.calls == calls, "warmed listing should serve from cache"


def test_warm_depth_zero_only_roots():
    b = CountingBackend()
    svc, cfg = make(b, warm_paths=["/projects", "/home"], warm_depth=0)
    assert CacheWarmer(svc, cfg).run_pass() == 2


def test_warm_max_dirs_caps_pass():
    b = CountingBackend()
    svc, cfg = make(b, warm_paths=["/projects"], warm_depth=3, warm_max_dirs=5)
    assert CacheWarmer(svc, cfg).run_pass() == 5


def test_refresh_requeries_only_entries_near_expiry():
    b = CountingBackend()
    svc, cfg = make(b, warm_paths=["/home"], warm_depth=0)
    warmer = CacheWarmer(svc, cfg)
    warmer.run_pass()
    calls = b.calls
    # Entry has ~300s left: a pass demanding less than that is a no-op...
    warmer.run_pass(min_ttl=1.0)
    assert b.calls == calls
    # ...but one demanding more than the TTL re-queries.
    warmer.run_pass(min_ttl=1000.0)
    assert b.calls == calls + 1


def test_interval_derived_from_cache_ttl():
    b = CountingBackend()
    svc, cfg = make(b, warm_paths=["/home"])
    assert CacheWarmer(svc, cfg).interval == 300.0 * 0.75


def test_disabled_without_paths():
    b = CountingBackend()
    svc, cfg = make(b)
    warmer = CacheWarmer(svc, cfg)
    assert not warmer.enabled
    warmer.start()  # must be a no-op, not a crash
    assert warmer._thread is None
