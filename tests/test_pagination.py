"""Listing service: slicing, clamping, truncation, cache behaviour."""

import pytest
from mock_catalog import MockBackend

from catwalk.catalog import ListingService
from catwalk.config import Config


class CountingBackend(MockBackend):
    """MockBackend that counts list_dir calls, to prove caching works."""

    def __init__(self):
        super().__init__()
        self.calls = 0

    def list_dir(self, *args, **kwargs):
        self.calls += 1
        yield from super().list_dir(*args, **kwargs)


@pytest.fixture(scope="module")
def backend():
    return CountingBackend()


def make_service(backend, **overrides):
    # Prefetch off by default: background warms would make the call-count
    # assertions racy. Tests that exercise prefetch enable it explicitly.
    overrides.setdefault("prefetch_children", 0)
    cfg = Config(mock=True, cache_ttl=300.0, **overrides)
    return ListingService(backend, cfg)


def test_page_size_clamped(backend):
    svc = make_service(backend)
    assert svc.clamp_page_size(None) == 20
    assert svc.clamp_page_size(5) == 20
    assert svc.clamp_page_size(50) == 50
    assert svc.clamp_page_size(5000) == 100


def test_pages_slice_without_overlap(backend):
    svc = make_service(backend)
    p1 = svc.list_page("/projects/proj-00/src", page=1, page_size=20)
    p2 = svc.list_page("/projects/proj-00/src", page=2, page_size=20)
    names1 = [e["name"] for e in p1["entries"]]
    names2 = [e["name"] for e in p2["entries"]]
    assert len(names1) == 20 and len(names2) == 20
    assert not set(names1) & set(names2)
    assert p1["pages"] == -(-p1["total_rows"] // 20)


def test_dirs_sort_before_files_on_name_sort(backend):
    svc = make_service(backend)
    page = svc.list_page("/bench-2b", page=1, page_size=20)
    types = [e["element_type"] for e in page["entries"]]
    # /bench-2b has 10 run-* dirs; they must all come before any file.
    assert types[:10] == ["DIR"] * 10


def test_out_of_range_page_clamps_to_last(backend):
    svc = make_service(backend)
    page = svc.list_page("/home/erin/notes", page=10**6, page_size=20)
    assert page["page"] == page["pages"]
    assert page["entries"]


def test_listing_cached_and_sort_does_not_requery(backend):
    svc = make_service(backend)
    before = backend.calls
    svc.list_page("/home/alice/code", page=1, page_size=20)
    after_first = backend.calls
    assert after_first == before + 1
    svc.list_page("/home/alice/code", page=2, page_size=20)
    svc.list_page("/home/alice/code", page=1, page_size=20, sort="size", order="desc")
    svc.list_page("/home/alice/code", page=1, page_size=100, sort="mtime")
    assert backend.calls == after_first  # pages + re-sorts are memory-only


def test_truncation_cap(backend):
    svc = make_service(backend, listing_cap=1000)
    page = svc.list_page("/bench-2b/run-000", page=1, page_size=20)
    assert page["truncated"] is True
    assert page["total_rows"] == 1000
    assert page["pages"] == 50


def test_name_filter_escapes_truncation(backend):
    svc = make_service(backend, listing_cap=1000)
    # run-000 has 100k files; a narrow filter matches far fewer than the cap.
    page = svc.list_page("/bench-2b/run-000", page=1, page_size=20, name_filter="000001.")
    assert page["truncated"] is False
    assert 0 < page["total_rows"] < 1000
    assert all("000001." in e["name"] for e in page["entries"])


def test_type_filter(backend):
    svc = make_service(backend)
    dirs = svc.list_page("/bench-2b", page=1, page_size=100, type_filter="dir")
    assert dirs["total_rows"] == 10
    assert all(e["element_type"] == "DIR" for e in dirs["entries"])
    files = svc.list_page("/bench-2b", page=1, page_size=100, type_filter="file")
    assert files["total_rows"] == 500


def test_other_type_filter_and_counts(backend):
    svc = make_service(backend)
    page = svc.list_page("/home/alice", page=1, page_size=100)
    assert page["total_other"] == 2
    other = svc.list_page("/home/alice", page=1, page_size=100, type_filter="other")
    assert other["total_rows"] == 2
    assert all(e["element_type"] == "SYMLINK" for e in other["entries"])


def test_unknown_path_is_empty_not_error(backend):
    svc = make_service(backend)
    page = svc.list_page("/no/such/dir", page=1)
    assert page["total_rows"] == 0
    assert page["entries"] == []
    assert page["pages"] == 1


def test_sort_orders(backend):
    svc = make_service(backend)
    page = svc.list_page(
        "/home/alice/code", page=1, page_size=100, sort="size", order="desc", type_filter="file"
    )
    sizes = [e["size"] for e in page["entries"]]
    assert sizes == sorted(sizes, reverse=True)


def test_filters_share_one_query_when_unfiltered_cached(backend):
    svc = make_service(backend)
    before = backend.calls
    svc.list_page("/home/bob/notes", page=1)
    svc.list_page("/home/bob/notes", page=1, type_filter="file")
    svc.list_page("/home/bob/notes", page=1, name_filter="data")
    # filtered listings derive from the cached unfiltered table
    assert backend.calls == before + 1


def test_filter_first_queries_backend_directly(backend):
    svc = make_service(backend)
    before = backend.calls
    page = svc.list_page("/home/dave/notes", page=1, type_filter="file")
    assert backend.calls == before + 1
    assert all(e["element_type"] == "FILE" for e in page["entries"])


def test_prefetch_warms_child_listings():
    from concurrent.futures import wait

    b = CountingBackend()
    svc = make_service(b, prefetch_children=4)
    entry = svc._base_listing("/projects/proj-01/", "", "")
    futures = svc.prefetch_children("/projects/proj-01/", entry)
    assert futures, "expected prefetch futures for child dirs"
    wait(futures, timeout=30)
    before = b.calls
    page = svc.list_page("/projects/proj-01/src", page=1)
    assert page["total_rows"] > 0
    assert b.calls == before, "child listing should be served from cache"
    svc.close()


def test_hung_prefetch_does_not_block_interactive_listing(monkeypatch):
    import time
    from concurrent.futures import Future

    monkeypatch.setattr("catwalk.catalog._PREFETCH_JOIN_TIMEOUT_S", 0.05)
    b = CountingBackend()
    svc = make_service(b, prefetch_children=4)
    hung = Future()
    hung.set_running_or_notify_cancel()  # "running": join cannot cancel it
    svc._prefetch_futures["/projects/proj-03/"] = hung
    start = time.monotonic()
    page = svc.list_page("/projects/proj-03", page=1)
    assert time.monotonic() - start < 5, "join must give up on a hung prefetch"
    assert page["total_rows"] > 0
    # The timed-out join fell through to a direct query and cached it.
    # (No call-count assertion: this listing's own child prefetches add calls.)
    assert svc.cache.get(("/projects/proj-03/", "", "")) is not None
    svc.close()


def test_prefetch_disabled_when_zero():
    b = CountingBackend()
    svc = make_service(b, prefetch_children=0)
    entry = svc._base_listing("/projects/proj-02/", "", "")
    assert svc.prefetch_children("/projects/proj-02/", entry) == []


def test_filtered_listing_derived_from_cached_unfiltered():
    b = CountingBackend()
    svc = make_service(b, prefetch_children=0)
    svc.list_page("/home/carol/code", page=1)
    before = b.calls
    dirs = svc.list_page("/home/carol/code", page=1, type_filter="dir")
    named = svc.list_page("/home/carol/code", page=1, name_filter="data")
    assert b.calls == before, "filters over a complete cached listing must not re-query"
    assert all(e["element_type"] == "DIR" for e in dirs["entries"])
    assert all("data" in e["name"] for e in named["entries"])


def test_filtered_listing_requeries_when_cache_truncated():
    b = CountingBackend()
    svc = make_service(b, prefetch_children=0, listing_cap=100)
    svc.list_page("/bench-2b/run-002", page=1)  # truncated at 100 rows
    before = b.calls
    svc.list_page("/bench-2b/run-002", page=1, name_filter="000200.")
    assert b.calls == before + 1, "truncated cache is incomplete -- must re-query"


def test_listing_cache_accounts_for_all_sort_variants():
    svc = make_service(MockBackend())
    entry = svc._base_listing("/home/alice/code/", "", "")
    assert svc.cache.total_bytes == entry["table"].nbytes * 4
