"""A failure while building services (after the backend connects) must
degrade to 503s and a truthful /api/health, not KeyError 500s."""

from fastapi.testclient import TestClient


def test_partial_startup_failure_degrades_cleanly(monkeypatch):
    monkeypatch.setenv("CATWALK_MOCK", "1")
    import catwalk.app as appmod

    def boom(*_args, **_kwargs):
        raise RuntimeError("rollup service exploded")

    monkeypatch.setattr(appmod, "RollupService", boom)
    with TestClient(appmod.app) as client:
        r = client.get("/api/ls", params={"path": "/"})
        assert r.status_code == 503
        assert "rollup service exploded" in r.json()["detail"]
        assert client.get("/api/rollup", params={"path": "/home"}).status_code == 503

        health = client.get("/api/health").json()
        assert health["catalog_reachable"] is False
        assert "rollup service exploded" in health["vastdb"]
