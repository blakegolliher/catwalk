"""Fake catalog for tests and demos.

The implementation lives in catwalk.mock so `CATWALK_MOCK=1` works from an
installed package with no repo checkout; this module re-exports it for test
imports and documents the synthetic namespace in one place.

Namespace (deterministic, seeded RNG -- see catwalk/mock.py):
  /bench-2b/run-00N/   10 dirs x 100k files (~1M files: pagination, big rollups)
  /projects/proj-NN/   12 projects x 4 subdirs, 200-2000 files each (~50k)
  /home/<user>/        small dirs, symlinks, one empty dir (/home/erin/empty/)
"""

from catwalk.mock import (  # noqa: F401
    BATCH_ROWS,
    MOCK_VIEWS,
    SCHEMA,
    MockBackend,
    MockCatalog,
)
