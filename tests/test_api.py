"""HTTP endpoints over the mock backend. TestClient runs the app lifespan,
so this also covers startup/shutdown wiring."""

import os

import pytest
from fastapi.testclient import TestClient

MOCK_ENV = {"CATWALK_MOCK": "1", "CATWALK_ROLLUP_SYNC_TIMEOUT": "30"}


@pytest.fixture(scope="module")
def client():
    saved = {k: os.environ.get(k) for k in MOCK_ENV}
    os.environ.update(MOCK_ENV)
    try:
        from catwalk.app import app

        with TestClient(app) as c:
            yield c
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None) if v is None else os.environ.update({k: v})


def test_health(client):
    body = client.get("/api/health").json()
    assert body["mode"] == "mock"
    assert body["catalog_reachable"] is True
    assert body["warmer"] is None  # no warm paths configured


def test_views_served_from_mock(client):
    body = client.get("/api/views").json()
    assert body["vms_unavailable"] is False
    assert {v["name"] for v in body["views"]} == {"bench", "projects", "home"}


def test_ls(client):
    body = client.get("/api/ls", params={"path": "/projects"}).json()
    assert body["total_dirs"] == 12
    assert body["entries"][0]["element_type"] == "DIR"


def test_ls_rejects_bad_type(client):
    assert client.get("/api/ls", params={"path": "/", "type": "bogus"}).status_code == 400


def test_ls_other_type(client):
    body = client.get("/api/ls", params={"path": "/home/alice", "type": "other"}).json()
    assert body["total_other"] == 2
    assert all(e["element_type"] == "SYMLINK" for e in body["entries"])


def test_ls_requires_path(client):
    assert client.get("/api/ls").status_code == 422


def test_root_rollup_refused(client):
    assert client.get("/api/rollup", params={"path": "/"}).status_code == 403


def test_rollup_rejects_excessive_depth(client):
    assert client.get("/api/rollup", params={"path": "/home", "depth": 999}).status_code == 400


def test_rollup_inline_then_cached(client):
    r1 = client.get("/api/rollup", params={"path": "/home/erin"})
    assert r1.status_code == 200
    assert r1.json()["from_cache"] is False
    r2 = client.get("/api/rollup", params={"path": "/home/erin"})
    assert r2.json()["from_cache"] is True
    assert {c["name"] for c in r2.json()["children"]} >= {"code", "notes", "empty"}


def test_rollup_status_unknown_job(client):
    assert client.get("/api/rollup/status", params={"job_id": "nope"}).status_code == 404


def test_rollup_cancel_requires_token(client):
    # Shared jobs may have other watchers; an anonymous cancel is refused.
    assert client.delete("/api/rollup/status", params={"job_id": "nope"}).status_code == 422
    r = client.delete("/api/rollup/status", params={"job_id": "nope", "cancel_token": "t"})
    assert r.status_code == 404


def test_export_listing_csv(client):
    r = client.get("/api/export/listing", params={"path": "/home/erin/notes"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    header, first = r.text.splitlines()[:2]
    assert header.split(",")[:2] == ['"name"', '"element_type"']
    assert first


def test_export_listing_rejects_bad_type(client):
    assert (
        client.get("/api/export/listing", params={"path": "/", "type": "bogus"}).status_code == 400
    )


def test_export_filename_supports_unicode_paths(client):
    r = client.get("/api/export/listing", params={"path": "/🐈"})
    assert r.status_code == 200
    disposition = r.headers["content-disposition"]
    assert "filename*=UTF-8''" in disposition
    assert "%F0%9F%90%88" in disposition
    assert "🐈" not in disposition


def test_spreadsheet_formula_strings_are_neutralized():
    import pyarrow as pa
    from catwalk.app import _spreadsheet_safe

    table = _spreadsheet_safe(pa.table({"name": ["=1+1", "+cmd", "safe"]}))
    assert table.column("name").to_pylist() == ["'=1+1", "'+cmd", "safe"]


def test_export_rollup_requires_prior_compute(client):
    assert client.get("/api/export/rollup", params={"path": "/home/dave"}).status_code == 404
    client.get("/api/rollup", params={"path": "/home/dave"})
    r = client.get("/api/export/rollup", params={"path": "/home/dave"})
    assert r.status_code == 200
    assert r.text.splitlines()[0].startswith('"folder"')


def test_capacity_degrades_without_vms(client):
    body = client.get("/api/capacity", params={"path": "/home"}).json()
    assert body["available"] is False


def test_static_index_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "Catwalk" in r.text
