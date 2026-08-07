"""Control-plane access via vastpy (VMS REST on :443): view enumeration and
best-effort capacity estimates.

Strictly optional -- when VMS credentials are absent or VMS is unreachable,
callers get an empty view list flagged vms_unavailable and the UI falls back
to free-form path entry. Catalog browsing never depends on this module.
"""

from __future__ import annotations

import threading

from .config import Config


class VMSService:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._client = None
        self._lock = threading.Lock()

    # -- session ------------------------------------------------------------

    def _connect(self):
        from vastpy import VASTClient  # deferred: mock/VMS-less installs

        return VASTClient(
            user=self.cfg.vms_user, password=self.cfg.vms_password, address=self.cfg.vms_address
        )

    def _get_client(self, fresh: bool = False):
        with self._lock:
            if self._client is None or fresh:
                self._client = self._connect()
            return self._client

    def _call(self, fn):
        """Run fn(client); on any failure, reconnect once and retry
        (covers expired VMS sessions without tracking auth state)."""
        try:
            return fn(self._get_client())
        except Exception:
            return fn(self._get_client(fresh=True))

    # -- API ----------------------------------------------------------------

    def get_views(self) -> dict:
        if not self.cfg.vms_configured:
            return {
                "views": [],
                "vms_unavailable": True,
                "reason": "VMS credentials not configured",
            }
        try:
            raw = self._call(lambda c: c.views.get())
        except Exception as e:
            return {"views": [], "vms_unavailable": True, "reason": str(e)}
        views = [
            {
                "name": v.get("name") or v.get("alias") or v.get("path"),
                "path": v.get("path"),
                "protocols": v.get("protocols") or [],
                "policy": v.get("policy_name") or v.get("policy"),
                "tenant": v.get("tenant_name") or v.get("tenant_id"),
            }
            for v in raw
            if v.get("path")
        ]
        views.sort(key=lambda v: v["path"] or "")
        return {"views": views, "vms_unavailable": False}

    def get_capacity(self, path: str) -> dict:
        """Sampled per-path capacity estimate from VMS. Best effort only."""
        if not self.cfg.vms_configured:
            return {"available": False, "reason": "VMS not configured"}
        try:
            raw = self._call(lambda c: c.capacity.get(path=path))
        except Exception as e:
            return {"available": False, "reason": str(e)}
        out = {"available": True, "path": path, "estimate": True, "raw": raw}
        if isinstance(raw, dict):
            details = raw.get("details", raw)
            if isinstance(details, dict):
                for k in ("logical", "unique", "usable", "percent", "average_atime"):
                    if k in details:
                        out[k] = details[k]
        return out

    def health(self) -> str:
        if not self.cfg.vms_configured:
            return "unconfigured"
        try:
            self._call(lambda c: c.views.get())
            return "ok"
        except Exception as e:
            return f"error: {e}"
