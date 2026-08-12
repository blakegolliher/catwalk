"""Synthetic VAST Catalog for development, tests, and demos (CATWALK_MOCK=1).

Generates a deterministic namespace (seeded RNG, keyed per directory path) so
listings and subtree scans are reproducible run to run within a process:

  /bench-2b/            500 direct files + run-000..run-009 (100k files each,
                        ~1M files total -- exercises pagination + big rollups)
  /projects/proj-NN/    12 projects x {src,data,docs,results}, 200-2000 files
                        each (~50k elements total)
  /home/<user>/         small dirs, symlinks, and one empty dir

Implements the same primitives as the real backend (`list_dir`,
`scan_subtree_files`, `list_child_dirs`) so cache/pagination/rollup/UI code
runs unmodified.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Iterator
from zlib import crc32

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .catalog import SCHEMA

SEED = 0xCA7
BATCH_ROWS = 65_536
_TABLE_MEMO_MAX = 16

EXTS = ["", "txt", "log", "dat", "h5", "csv", "py", "bin", "parquet", "tar"]
STEMS = ["data", "frame", "chunk", "log", "img", "sample"]
OWNERS = ["alice", "bob", "carol", "dave", "root"]

MOCK_VIEWS = [
    {
        "name": "bench",
        "path": "/bench-2b",
        "protocols": ["NFS", "SMB"],
        "policy": "default",
        "tenant": "default",
    },
    {
        "name": "projects",
        "path": "/projects",
        "protocols": ["NFS"],
        "policy": "default",
        "tenant": "default",
    },
    {
        "name": "home",
        "path": "/home",
        "protocols": ["NFS", "S3"],
        "policy": "default",
        "tenant": "default",
    },
]


def _dir_seed(path: str) -> int:
    return crc32(path.encode()) ^ SEED


class MockCatalog:
    """Deterministic in-memory catalog with lazy per-directory generation."""

    def __init__(self):
        self.now_ns = int(time.time()) * 10**9
        self.tree: dict[str, dict] = {}
        self._memo: OrderedDict[str, pa.Table] = OrderedDict()
        self._memo_lock = threading.Lock()
        self._build_tree()

    # -- namespace layout ---------------------------------------------------

    def _add(self, path: str, files: int = 0, symlinks: int = 0):
        self.tree[path] = {"files": files, "symlinks": symlinks, "dirs": []}
        if path != "/":
            parent = path.rstrip("/").rsplit("/", 1)[0] + "/"
            name = path.rstrip("/").rsplit("/", 1)[1]
            self.tree[parent]["dirs"].append(name)

    def _build_tree(self):
        self._add("/")
        self._add("/bench-2b/", files=500)
        for i in range(10):
            self._add(f"/bench-2b/run-{i:03d}/", files=100_000)
        self._add("/projects/")
        for i in range(12):
            proj = f"/projects/proj-{i:02d}/"
            self._add(proj, files=5 + _dir_seed(proj) % 45)
            for sub in ("src", "data", "docs", "results"):
                p = f"{proj}{sub}/"
                self._add(p, files=200 + _dir_seed(p) % 1800)
        self._add("/home/")
        for user in ("alice", "bob", "carol", "dave", "erin"):
            h = f"/home/{user}/"
            self._add(h, files=10 + _dir_seed(h) % 30, symlinks=2)
            self._add(f"{h}code/", files=100 + _dir_seed(h + "c") % 400)
            self._add(f"{h}notes/", files=20 + _dir_seed(h + "n") % 80)
        self._add("/home/erin/empty/")

    # -- per-directory table generation -------------------------------------

    def _dir_table(self, path: str) -> pa.Table:
        with self._memo_lock:
            memo = self._memo.get(path)
            if memo is not None:
                self._memo.move_to_end(path)
                return memo
            table = self._generate(path)
            self._memo[path] = table
            while len(self._memo) > _TABLE_MEMO_MAX:
                self._memo.popitem(last=False)
            return table

    def _generate(self, path: str) -> pa.Table:
        spec = self.tree[path]
        rng = np.random.default_rng(_dir_seed(path))
        nfiles, nlinks_cnt = spec["files"], spec["symlinks"]
        stem = STEMS[_dir_seed(path + "s") % len(STEMS)]

        names, etypes, sizes, used, exts, nlinks = [], [], [], [], [], []
        for d in spec["dirs"]:
            names.append(d)
            etypes.append("DIR")
            sizes.append(4096)
            used.append(4096)
            exts.append("")
            nlinks.append(2 + len(self.tree[f"{path}{d}/"]["dirs"]))
        for i in range(nlinks_cnt):
            names.append(f"link-{i}")
            etypes.append("SYMLINK")
            sizes.append(24)
            used.append(0)
            exts.append("")
            nlinks.append(1)

        ext_idx = rng.integers(0, len(EXTS), nfiles)
        fsizes = np.clip(rng.lognormal(11.0, 2.4, nfiles), 0, 2**42).astype(np.int64)
        fused = (np.ceil(fsizes * rng.uniform(0.35, 1.0, nfiles) / 4096) * 4096).astype(np.int64)
        for i in range(nfiles):
            e = EXTS[ext_idx[i]]
            names.append(f"{stem}-{i:06d}.{e}" if e else f"{stem}-{i:06d}")
            exts.append(e)
        etypes.extend(["FILE"] * nfiles)
        sizes.extend(fsizes.tolist())
        used.extend(fused.tolist())
        nlinks.extend([1] * nfiles)

        n = len(names)
        two_years_ns = 730 * 86_400 * 10**9
        mtime = self.now_ns - rng.integers(0, two_years_ns, n, dtype=np.int64)
        atime = np.minimum(
            self.now_ns, mtime + rng.integers(0, 90 * 86_400 * 10**9, n, dtype=np.int64)
        )
        owners = [OWNERS[j] for j in rng.integers(0, len(OWNERS), n)]

        return pa.table(
            {
                "parent_path": pa.array([path] * n),
                "name": pa.array(names),
                "element_type": pa.array(etypes),
                "size": pa.array(sizes, pa.int64()),
                "used": pa.array(used, pa.int64()),
                "mtime": pa.array(mtime).cast(pa.timestamp("ns")),
                "atime": pa.array(atime).cast(pa.timestamp("ns")),
                "owner_name": pa.array(owners),
                "extension": pa.array(exts),
                "nlinks": pa.array(nlinks, pa.int64()),
            },
            schema=SCHEMA,
        )


class MockBackend:
    """Backend interface over MockCatalog -- same primitives as the real one."""

    def __init__(self):
        self.catalog = MockCatalog()
        # Settable in tests to exercise epoch-keyed caching; None = live
        # (matches a cluster without catalog snapshots).
        self.epoch: str | None = None

    def current_epoch(self) -> str | None:
        return self.epoch

    def peek_epoch(self) -> str | None:
        return self.epoch

    def list_dir(
        self,
        path: str,
        columns: list[str],
        element_type: str | None = None,
        name_contains: str | None = None,
        epoch: str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        if path not in self.catalog.tree:
            return
        tbl = self.catalog._dir_table(path)
        if element_type == "OTHER":
            tbl = tbl.filter(
                pc.invert(pc.is_in(tbl.column("element_type"), value_set=pa.array(["FILE", "DIR"])))
            )
        elif element_type:
            tbl = tbl.filter(pc.equal(tbl.column("element_type"), element_type))
        if name_contains:
            tbl = tbl.filter(pc.match_substring(tbl.column("name"), name_contains))
        yield from tbl.select(columns).to_batches(max_chunksize=BATCH_ROWS)

    def scan_subtree_files(
        self, prefix: str, columns: list[str], epoch: str | None = None
    ) -> Iterator[pa.RecordBatch]:
        for path in sorted(self.catalog.tree):
            if not path.startswith(prefix):
                continue
            tbl = self.catalog._dir_table(path)
            tbl = tbl.filter(pc.equal(tbl.column("element_type"), "FILE"))
            if tbl.num_rows:
                yield from tbl.select(columns).to_batches(max_chunksize=BATCH_ROWS)

    def list_child_dirs(
        self, path: str, limit: int | None = None, epoch: str | None = None
    ) -> list[str]:
        spec = self.catalog.tree.get(path)
        dirs = list(spec["dirs"]) if spec else []
        return dirs[:limit] if limit is not None else dirs

    def health(self) -> dict:
        return {"vastdb": "ok", "catalog_reachable": True, "mode": "mock"}

    def get_views(self) -> list[dict]:
        return [dict(v) for v in MOCK_VIEWS]
