"""Snapshot-epoch cache keying: newest-snapshot resolution, epoch-keyed
listings/rollups, and the poll/backoff behavior of current_epoch."""

import threading
from types import SimpleNamespace

from mock_catalog import MockBackend

from catwalk.catalog import ListingService, RollupService, VastBackend, newest_catalog_snapshot
from catwalk.config import Config


class CountingBackend(MockBackend):
    def __init__(self):
        super().__init__()
        self.list_calls = 0
        self.scan_calls = 0

    def list_dir(self, *args, **kwargs):
        self.list_calls += 1
        yield from super().list_dir(*args, **kwargs)

    def scan_subtree_files(self, *args, **kwargs):
        self.scan_calls += 1
        yield from super().scan_subtree_files(*args, **kwargs)


def make_listings(backend, **overrides):
    overrides.setdefault("prefetch_children", 0)
    return ListingService(backend, Config(mock=True, **overrides))


# ---- newest_catalog_snapshot ------------------------------------------------


class StubApi:
    """Paged list_snapshots stub mirroring the SDK's (names, truncated, token)
    contract; names arrive as '.snapshot/<name>/' common prefixes."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = 0

    def list_snapshots(self, bucket, name_prefix="", next_token=None, max_keys=None):
        self.calls += 1
        i = next_token or 0
        page = self.pages[i] if i < len(self.pages) else []
        return page, i + 1 < len(self.pages), i + 1


def test_newest_snapshot_spans_pages():
    api = StubApi(
        [
            [".snapshot/big_catalog_2026-08-12_18_01_26_UTC/"],
            [
                ".snapshot/big_catalog_2026-08-12_18_16_25_UTC/",
                ".snapshot/big_catalog_2026-08-12_19_01_25_UTC/",
            ],
        ]
    )
    assert newest_catalog_snapshot(api, "big_catalog") == "big_catalog_2026-08-12_19_01_25_UTC"
    assert api.calls == 2


def test_newest_snapshot_none_when_cluster_has_no_snapshots():
    assert newest_catalog_snapshot(StubApi([[]]), "big_catalog") is None


# ---- VastBackend.current_epoch ---------------------------------------------


def make_vast_backend(api, snapshot_pin=True, snapshot_poll=60.0):
    backend = VastBackend.__new__(VastBackend)
    backend.cfg = SimpleNamespace(
        snapshot_pin=snapshot_pin, snapshot_poll=snapshot_poll, snapshot_prefix="big_catalog"
    )
    backend.session = SimpleNamespace(api=api)
    backend._epoch = None
    backend._epoch_checked = float("-inf")
    backend._epoch_lock = threading.Lock()
    return backend


def test_current_epoch_polls_once_per_interval():
    api = StubApi([[".snapshot/big_catalog_2026-08-12_19_01_25_UTC/"]])
    backend = make_vast_backend(api)
    assert backend.current_epoch() == "big_catalog_2026-08-12_19_01_25_UTC"
    assert backend.current_epoch() == "big_catalog_2026-08-12_19_01_25_UTC"
    assert api.calls == 1, "second call inside the poll interval must not LIST again"


def test_current_epoch_disabled_never_lists():
    api = StubApi([[".snapshot/big_catalog_2026-08-12_19_01_25_UTC/"]])
    backend = make_vast_backend(api, snapshot_pin=False)
    assert backend.current_epoch() is None
    assert api.calls == 0


def test_current_epoch_keeps_previous_value_when_listing_fails():
    class FailingApi:
        def list_snapshots(self, **kwargs):
            raise RuntimeError("VIP unreachable")

    backend = make_vast_backend(FailingApi(), snapshot_poll=0.000001)
    backend._epoch = "big_catalog_2026-08-12_18_46_25_UTC"
    assert backend.current_epoch() == "big_catalog_2026-08-12_18_46_25_UTC"


def test_drop_epoch_forces_reresolve():
    api = StubApi([[".snapshot/big_catalog_2026-08-12_19_01_25_UTC/"]])
    backend = make_vast_backend(api)
    epoch = backend.current_epoch()
    backend._drop_epoch(epoch)
    assert backend.current_epoch() == epoch  # re-resolved despite poll interval
    assert api.calls == 2


# ---- epoch-keyed caches -----------------------------------------------------


def test_listing_cache_invalidated_by_epoch_change():
    b = CountingBackend()
    b.epoch = "epoch-1"
    svc = make_listings(b)
    svc.list_page("/projects")
    svc.list_page("/projects")
    assert b.list_calls == 1, "same epoch: second request is a cache hit"
    b.epoch = "epoch-2"
    svc.list_page("/projects")
    assert b.list_calls == 2, "new epoch: cache key changes, listing re-queried"


def test_listing_epoch_none_still_caches():
    b = CountingBackend()
    svc = make_listings(b)
    svc.list_page("/projects")
    svc.list_page("/projects")
    assert b.list_calls == 1


def test_listing_reports_catalog_snapshot():
    b = CountingBackend()
    b.epoch = "epoch-1"
    svc = make_listings(b)
    assert svc.list_page("/projects")["catalog_snapshot"] == "epoch-1"


def test_rollup_cache_invalidated_by_epoch_change():
    b = CountingBackend()
    b.epoch = "epoch-1"
    svc = RollupService(b, Config(mock=True))
    result = svc.compute("/home/erin")
    assert result["catalog_snapshot"] == "epoch-1"
    assert svc.cached("/home/erin") is not None
    b.epoch = "epoch-2"
    assert svc.cached("/home/erin") is None, "new epoch: cached rollup no longer served"
