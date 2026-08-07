"""normalize_prefix / group_key edge cases -- the trailing-slash rules that
keep /admin from matching /administrator."""

from catwalk.catalog import group_key, normalize_prefix, ns_to_iso


def test_normalize_adds_trailing_slash():
    assert normalize_prefix("/admin") == "/admin/"
    assert normalize_prefix("admin") == "/admin/"
    assert normalize_prefix("/admin/") == "/admin/"


def test_normalize_root():
    assert normalize_prefix("/") == "/"
    assert normalize_prefix("") == "/"


def test_normalize_strips_duplicate_slashes_at_ends():
    assert normalize_prefix("//a/b//") == "/a/b/"


def test_prefix_cannot_match_sibling():
    # The whole point: /admin/ is not a prefix of /administrator/'s rows.
    p = normalize_prefix("/admin")
    assert not "/administrator/".startswith(p)


def test_group_key_depth1():
    assert group_key("/admin/proj/x/", "/admin/", 1) == "proj"
    assert group_key("/admin/proj/", "/admin/", 1) == "proj"


def test_group_key_files_directly_in_prefix():
    assert group_key("/admin/", "/admin/", 1) == "."


def test_group_key_depth2():
    assert group_key("/admin/proj/x/y/", "/admin/", 2) == "proj/x"
    assert group_key("/admin/proj/", "/admin/", 2) == "proj"


def test_ns_to_iso():
    assert ns_to_iso(0) == "1970-01-01T00:00:00.000000000"
    assert ns_to_iso(1_500_000_000_123_456_789) == "2017-07-14T02:40:00.123456789"
    assert ns_to_iso(None) == ""


def test_fanout_endpoints_cycles_vips():
    from catwalk.config import Config

    cfg = Config(
        endpoint="http://pool",
        data_endpoints=["http://a", "http://b", "http://c"],
        query_concurrency=8,
    )
    eps = cfg.fanout_endpoints()
    assert len(eps) == 8
    assert eps[:4] == ["http://a", "http://b", "http://c", "http://a"]


def test_fanout_endpoints_repeats_single_endpoint():
    from catwalk.config import Config

    cfg = Config(endpoint="http://pool", query_concurrency=6)
    assert cfg.fanout_endpoints() == ["http://pool"] * 6


def test_fanout_disabled_when_concurrency_zero():
    from catwalk.config import Config

    cfg = Config(endpoint="http://pool", data_endpoints=["http://a"], query_concurrency=0)
    assert cfg.fanout_endpoints() == ["http://a"]
