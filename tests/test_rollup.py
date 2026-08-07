"""aggregate_reader against synthetic Arrow batches, plus RollupService
semantics: descendant aggregation, empty-dir seeding, root guard."""

from types import SimpleNamespace

import pyarrow as pa
import pytest

from catwalk.catalog import (
    FolderStats,
    RollupService,
    RollupTooWide,
    VastBackend,
    aggregate_reader,
    merge_groups,
)
from catwalk.config import Config
from mock_catalog import MockBackend


def make_batch(rows):
    """rows: (parent_path, size, used, mtime_ns, atime_ns)"""
    pp, size, used, mt, at = zip(*rows, strict=True)
    return pa.record_batch(
        {
            "parent_path": pa.array(pp),
            "size": pa.array(size, pa.int64()),
            "used": pa.array(used, pa.int64()),
            "mtime": pa.array(mt, pa.int64()).cast(pa.timestamp("ns")),
            "atime": pa.array(at, pa.int64()).cast(pa.timestamp("ns")),
        }
    )


def test_aggregate_reader_groups_by_depth1_child():
    batches = [
        make_batch(
            [
                ("/admin/a/", 100, 96, 1_000, 2_000),
                ("/admin/a/deep/nested/", 50, 48, 9_000, 1_000),
                ("/admin/b/", 10, 8, 500, 600),
            ]
        ),
        make_batch(
            [
                ("/admin/a/", 200, 100, 3_000, 8_000),
                ("/admin/", 7, 4, 100, 200),  # file directly in the prefix
            ]
        ),
    ]
    groups = {}
    rows = aggregate_reader(iter(batches), "/admin/", 1, groups)
    assert rows == 5
    a = groups["a"]
    assert (a.files, a.bytes, a.used) == (3, 350, 244)
    assert a.mtime_ns == 9_000  # from the deep descendant
    assert a.atime_ns == 8_000
    assert groups["b"].files == 1
    assert groups["."].files == 1  # files at the prefix itself group as "."


def test_aggregate_reader_progress_callback():
    seen = []
    groups = {}
    aggregate_reader(
        iter([make_batch([("/x/a/", 1, 1, 1, 1)]), make_batch([("/x/a/", 1, 1, 1, 1)])]),
        "/x/",
        1,
        groups,
        rows_cb=seen.append,
    )
    assert seen == [1, 2]


def test_aggregate_reader_enforces_group_limit():
    batch = make_batch(
        [
            ("/x/a/", 1, 1, 1, 1),
            ("/x/b/", 1, 1, 1, 1),
        ]
    )
    with pytest.raises(RollupTooWide):
        aggregate_reader([batch], "/x/", 1, {}, max_groups=1)


def test_merge_groups():
    g1 = {"a": FolderStats()}
    g1["a"].add(2, 100, 50, 1_000, 1_000)
    g2 = {"a": FolderStats(), "b": FolderStats()}
    g2["a"].add(1, 10, 5, 5_000, 500)
    g2["b"].add(1, 1, 1, 1, 1)
    merge_groups(g1, g2)
    assert (g1["a"].files, g1["a"].bytes, g1["a"].mtime_ns) == (3, 110, 5_000)
    assert "b" in g1


def test_none_timestamps_do_not_crash():
    s = FolderStats()
    s.add(1, None, None, None, None)
    s.add(1, 5, 5, 42, 42)
    assert (s.bytes, s.mtime_ns) == (5, 42)


@pytest.fixture(scope="module")
def service():
    backend = MockBackend()
    return RollupService(backend, Config(mock=True, cache_ttl=300.0))


def test_rollup_matches_direct_scan(service):
    result = service.compute("/home/alice")
    # Oracle: recompute totals straight off the mock's file rows.
    expect_files = expect_bytes = 0
    for batch in service.backend.scan_subtree_files(
        "/home/alice/", ["parent_path", "size", "used", "atime", "mtime"]
    ):
        expect_files += batch.num_rows
        expect_bytes += sum(batch.column("size").to_pylist())
    assert result["totals"]["file_count"] == expect_files
    assert result["totals"]["total_bytes"] == expect_bytes
    assert {c["name"] for c in result["children"]} == {"code", "notes", "."}


def test_empty_dirs_still_appear(service):
    result = service.compute("/home/erin")
    empty = next(c for c in result["children"] if c["name"] == "empty")
    assert empty["file_count"] == 0
    assert empty["total_bytes"] == 0
    assert empty["last_modified"] == ""


def test_root_rollup_refused_without_allow_root(service):
    with pytest.raises(PermissionError):
        service.compute("/")


def test_root_rollup_allowed_with_flag():
    svc = RollupService(MockBackend(), Config(mock=True, cache_ttl=300.0, allow_root=True))
    result = svc.compute("/", depth=1)
    assert {c["name"] for c in result["children"]} >= {"bench-2b", "projects", "home"}


def test_rollup_cached(service):
    service.compute("/home/bob")
    assert service.cached("/home/bob") is not None
    assert service.cached("/home/bob/") is not None  # normalized key
    assert service.cached("/home/nobody") is None


def test_depth2_grouping(service):
    result = service.compute("/projects/proj-00", depth=2)
    names = {c["name"] for c in result["children"]}
    assert "src" in names and "data" in names


def test_public_result_limits_children_without_changing_totals(service):
    result = service.compute("/projects", depth=1)
    public = service.public_result(result, child_limit=3)
    assert len(public["children"]) == 3
    assert public["total_children"] == 12
    assert public["children_truncated"] is True
    assert public["totals"] == result["totals"]


def test_real_pipeline_propagates_stream_error_without_deadlock():
    class Reader:
        def __iter__(self):
            # Leave aggregation work in flight before the transport fails;
            # the old queue/sentinel pipeline stranded its workers here.
            yield make_batch([("/x/a/", 1, 1, 1, 1)])
            raise RuntimeError("stream failed")

    class Catalog:
        def select(self, **_kwargs):
            return Reader()

    class Transaction:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def catalog(self):
            return Catalog()

    class Session:
        def transaction(self):
            return Transaction()

    backend = VastBackend.__new__(VastBackend)
    backend.cfg = SimpleNamespace(rollup_threads=2, rollup_max_groups=100)
    backend.session = Session()
    backend._query_config = lambda: None
    with pytest.raises(RuntimeError, match="stream failed"):
        backend.rollup_groups("/x/", 1)
