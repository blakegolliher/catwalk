"""Configuration invariants that protect startup and resource limits."""

import pytest

from catwalk.config import Config


@pytest.mark.parametrize(
    "kwargs",
    [
        {"query_threads": 0},
        {"rollup_workers": 0},
        {"cache_ttl": 0},
        {"warm_interval": -1},
        {"page_default": 200, "page_max": 100},
        {"page_default": 10},
        {"rollup_response_children": 101, "rollup_max_groups": 100},
        {"endpoint": "pool-without-a-scheme"},
    ],
)
def test_invalid_config_is_rejected(kwargs):
    with pytest.raises(ValueError, match="invalid Catwalk configuration"):
        Config(mock=True, **kwargs).validate()


def test_valid_config_returns_itself():
    cfg = Config(mock=True)
    assert cfg.validate() is cfg


def test_query_concurrency_is_an_actual_ceiling():
    cfg = Config(
        endpoint="http://pool",
        data_endpoints=[f"http://vip-{i}" for i in range(6)],
        query_concurrency=2,
    )
    assert cfg.fanout_endpoints() == ["http://vip-0", "http://vip-1"]
