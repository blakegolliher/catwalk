"""Slow or wedged catalog queries must not take down the rest of the app.

The query budget (CATWALK_QUERY_THREADS) bounds concurrent backend queries;
these tests pin that exhausting it with slow cold listings leaves /api/health
and cache-hit listings fast. Regression tests for the coupled-limiter
starvation where every endpoint shared one thread pool sized to the budget.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from catwalk.mock import MockBackend

SLOW_S = 2.0
FAST_S = 1.0  # generous: an unstarved request completes in milliseconds


@pytest.fixture
def slow_client(monkeypatch):
    monkeypatch.setenv("CATWALK_MOCK", "1")
    monkeypatch.setenv("CATWALK_QUERY_THREADS", "2")
    monkeypatch.setenv("CATWALK_PREFETCH_CHILDREN", "0")

    real_list_dir = MockBackend.list_dir

    def slow_list_dir(self, path, columns, **kw):
        time.sleep(SLOW_S)
        yield from real_list_dir(self, path, columns, **kw)

    monkeypatch.setattr(MockBackend, "list_dir", slow_list_dir)
    from catwalk.app import app

    with TestClient(app) as client:
        yield client


def test_health_and_cached_listing_survive_saturated_query_budget(slow_client):
    client = slow_client
    # Warm one directory so a later request is a guaranteed cache hit.
    client.get("/api/ls", params={"path": "/home/alice/"})

    with ThreadPoolExecutor(max_workers=4) as pool:
        cold = [
            pool.submit(client.get, "/api/ls", params={"path": p})
            for p in ("/projects/proj-00/", "/projects/proj-01/")
        ]
        time.sleep(0.3)  # let both cold listings occupy the query budget

        t0 = time.perf_counter()
        health = client.get("/api/health")
        health_elapsed = time.perf_counter() - t0

        t0 = time.perf_counter()
        cached = client.get("/api/ls", params={"path": "/home/alice/"})
        cached_elapsed = time.perf_counter() - t0

        for f in cold:
            assert f.result().status_code == 200

    assert health.status_code == 200
    assert health.json()["catalog_reachable"] is True
    assert health_elapsed < FAST_S, f"health starved behind slow queries ({health_elapsed:.2f}s)"
    assert cached.status_code == 200
    assert cached_elapsed < FAST_S, f"cache hit starved behind slow queries ({cached_elapsed:.2f}s)"


def test_health_reports_timeout_instead_of_hanging(slow_client, monkeypatch):
    from catwalk import app as appmod

    def wedged_health():
        time.sleep(30)

    monkeypatch.setattr(appmod._state["backend"], "health", wedged_health)
    t0 = time.perf_counter()
    body = slow_client.get("/api/health").json()
    elapsed = time.perf_counter() - t0
    assert elapsed < 7.0, f"health hung on a wedged backend probe ({elapsed:.2f}s)"
    # A timed-out probe proves slowness, not absence: unknown (None), not down.
    assert body["catalog_reachable"] is None
    assert "timed out" in body["vastdb"]


def _unconnected_vast_backend():
    import threading

    from catwalk.catalog import VastBackend

    backend = VastBackend.__new__(VastBackend)
    backend._epoch = None
    backend._epoch_checked = float("-inf")
    backend._epoch_lock = threading.Lock()
    backend._last_ok = float("-inf")

    class ExplodingSession:
        def transaction(self):
            raise RuntimeError("no cluster in tests")

    backend.session = ExplodingSession()
    return backend


def test_health_served_from_recent_query_success_without_probing():
    """Recent successful traffic proves reachability; health must not add a
    probe query on top of the load that traffic is creating."""
    backend = _unconnected_vast_backend()
    backend._last_ok = time.monotonic()
    body = backend.health()  # the session raises if health actually probes
    assert body["catalog_reachable"] is True


def test_health_probes_when_no_recent_success():
    backend = _unconnected_vast_backend()
    body = backend.health()
    assert body["catalog_reachable"] is False
    assert "no cluster in tests" in body["vastdb"]
