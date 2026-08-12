"""TTL/LRU cache bounds, expiry cleanup, single-flight, and counters."""

import threading
import time

import pytest

from catwalk.cache import TTLCache


def test_put_sweeps_expired_entries():
    cache = TTLCache(ttl=0.001)
    for key in range(20):
        cache.put(key, key, nbytes=1)
    time.sleep(0.01)
    cache.put("fresh", 1, nbytes=1)
    assert len(cache) == 1
    assert cache.total_bytes == 1


def test_oversized_value_is_not_cached():
    cache = TTLCache(ttl=60, max_bytes=10)
    assert cache.put("large", object(), nbytes=11) is False
    assert cache.get("large") is None
    assert cache.total_bytes == 0


def test_byte_bound_evicts_lru_entries():
    cache = TTLCache(ttl=60, max_bytes=10)
    cache.put("a", 1, nbytes=6)
    cache.put("b", 2, nbytes=6)
    assert cache.get("a") is None
    assert cache.get("b") == 2
    assert cache.total_bytes == 6


def test_stats_counters():
    cache = TTLCache(ttl=60, max_bytes=10)
    cache.put("a", 1, nbytes=6)
    assert cache.get("a") == 1  # hit
    assert cache.get("b") is None  # miss
    cache.put("b", 2, nbytes=6)  # evicts "a"
    s = cache.stats()
    assert s["hits"] == 1
    assert s["misses"] == 1
    assert s["evicted"] == 1
    assert s["entries"] == 1
    assert s["bytes"] == 6


def test_stats_counts_expiry():
    cache = TTLCache(ttl=0.001)
    cache.put("a", 1, nbytes=1)
    time.sleep(0.01)
    assert cache.get("a") is None
    s = cache.stats()
    assert s["expired"] == 1
    assert s["misses"] == 1


def _wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.005)


def test_concurrent_misses_coalesce_into_one_compute():
    cache = TTLCache(ttl=60)
    computing = threading.Event()
    release = threading.Event()
    calls = []

    def compute():
        calls.append(1)
        computing.set()
        assert release.wait(timeout=5)
        return "value", 1

    results = []
    threads = [
        threading.Thread(target=lambda: results.append(cache.get_or_compute("k", compute)))
        for _ in range(3)
    ]
    threads[0].start()
    computing.wait(timeout=5)
    for t in threads[1:]:
        t.start()
    # Both followers must be registered as waiters before the leader finishes.
    _wait_for(lambda: cache.stats()["coalesced"] == 2)
    release.set()
    for t in threads:
        t.join(timeout=5)
    assert len(calls) == 1
    assert [value for value, _hit in results] == ["value"] * 3


def test_failed_compute_propagates_to_waiters_then_clears_flight():
    cache = TTLCache(ttl=60)
    computing = threading.Event()
    release = threading.Event()

    def failing_compute():
        computing.set()
        assert release.wait(timeout=5)
        raise RuntimeError("scan failed")

    errors_seen = []

    def expect_failure():
        with pytest.raises(RuntimeError):
            cache.get_or_compute("k", failing_compute)
        errors_seen.append(True)

    leader = threading.Thread(target=expect_failure)
    leader.start()
    computing.wait(timeout=5)
    waiter = threading.Thread(target=expect_failure)
    waiter.start()
    _wait_for(lambda: cache.stats()["coalesced"] == 1)
    release.set()
    leader.join(timeout=5)
    waiter.join(timeout=5)
    assert errors_seen == [True, True]
    # The failed flight is gone: a fresh call computes again rather than
    # waiting on a dead future.
    value, hit = cache.get_or_compute("k", lambda: ("recovered", 1))
    assert (value, hit) == ("recovered", False)
