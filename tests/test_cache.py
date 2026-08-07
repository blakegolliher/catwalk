"""TTL/LRU cache bounds and expiry cleanup."""

import time

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
