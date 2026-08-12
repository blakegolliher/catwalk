"""Catwalk configuration: environment variables first, CLI flags override.

Same convention and variable names as catalog_folder_report.py. The data
plane endpoint (VASTDB_ENDPOINT) must be a data VIP pool DNS name or VIP,
never the VMS address.
"""

from __future__ import annotations

import math
import os
import socket
import sys
from dataclasses import dataclass
from urllib.parse import urlparse


def _env(name: str, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _env_num(name: str, cast, default=None):
    v = os.environ.get(name)
    if v in (None, ""):
        return default
    try:
        return cast(v)
    except ValueError:
        sys.exit(f"error: environment variable {name}={v!r} is not a valid number")


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _env_bool_default(name: str, default: bool) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "on")


def _vms_host(value: str | None) -> str | None:
    """Reduce VMS_ADDRESS to host[:port]. vastpy builds https://{address}/...
    itself, so a pasted http(s):// URL would otherwise dial a host named
    'http'; accept both forms."""
    if not value:
        return value
    v = value.strip().rstrip("/")
    if "://" not in v:
        return v or None
    parsed = urlparse(v)
    host = parsed.hostname or ""
    if ":" in host:  # bare IPv6 literal needs brackets inside a URL
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return host or None


@dataclass
class Config:
    # Data plane
    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    data_endpoints: list[str] | None = None
    auto_endpoints: bool = False

    # Control plane (optional -- app degrades gracefully without it)
    vms_address: str | None = None
    vms_user: str | None = None
    vms_password: str | None = None

    # App
    host: str = "0.0.0.0"
    port: int = 8080
    page_default: int = 20
    page_max: int = 100
    cache_ttl: float = 900.0
    listing_cap: int = 500_000
    num_splits: int = 64
    cache_max_bytes: int = 2 * 1024**3
    mock: bool = False
    allow_root: bool = False
    rollup_sync_timeout: float = 5.0
    rollup_workers: int = 2
    rollup_threads: int = 8
    rollup_queue_max: int = 16
    rollup_cache_max_bytes: int = 256 * 1024**2
    rollup_max_groups: int = 10_000
    rollup_max_depth: int = 8
    rollup_response_children: int = 500
    query_threads: int = 8
    query_concurrency: int = 24
    query_timeout: float = 60.0  # per-read socket timeout; 0 disables
    # Snapshot pinning: key caches by the newest catalog snapshot and read
    # from it, so invalidation is exact (a new snapshot flips every cache
    # at once) instead of wall-clock TTL. Falls back to live queries when
    # the cluster has no catalog snapshots.
    snapshot_pin: bool = True
    snapshot_poll: float = 60.0
    snapshot_prefix: str = "big_catalog"
    prefetch_children: int = 8
    warm_paths: list[str] | None = None
    warm_depth: int = 2
    warm_threads: int = 4
    warm_max_dirs: int = 500
    warm_interval: float = 0.0  # 0 -> derived: cache_ttl * 0.75

    def validate(self) -> Config:
        """Reject settings that would crash startup or defeat resource bounds."""
        errors = []

        def integer(name, value, minimum=0, maximum=None):
            if value < minimum or (maximum is not None and value > maximum):
                bound = f"{minimum}..{maximum}" if maximum is not None else f">= {minimum}"
                errors.append(f"{name} must be {bound} (got {value!r})")

        def number(name, value, minimum=0.0, allow_zero=False):
            if not math.isfinite(value) or value < minimum or (value == 0 and not allow_zero):
                op = ">=" if allow_zero else ">"
                errors.append(f"{name} must be finite and {op} {minimum:g} (got {value!r})")

        def endpoint(name, value):
            if not value:
                return
            parsed = urlparse(value)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                errors.append(f"{name} must be an http(s) URL with a hostname (got {value!r})")

        integer("CATWALK_PORT", self.port, 0, 65_535)  # 0 = OS picks a free port
        integer("CATWALK_PAGE_DEFAULT", self.page_default, 20)
        integer("CATWALK_PAGE_MAX", self.page_max, 20)
        if self.page_default > self.page_max:
            errors.append("CATWALK_PAGE_DEFAULT must not exceed CATWALK_PAGE_MAX")
        number("CATWALK_CACHE_TTL", self.cache_ttl)
        integer("CATWALK_LISTING_CAP", self.listing_cap, 1)
        integer("CATWALK_NUM_SPLITS", self.num_splits, 1)
        integer("CATWALK_CACHE_MAX_BYTES", self.cache_max_bytes, 1)
        number("CATWALK_ROLLUP_SYNC_TIMEOUT", self.rollup_sync_timeout, allow_zero=True)
        integer("CATWALK_ROLLUP_WORKERS", self.rollup_workers, 1, 64)
        integer("CATWALK_ROLLUP_THREADS", self.rollup_threads, 1, 64)
        integer("CATWALK_ROLLUP_QUEUE_MAX", self.rollup_queue_max, 0)
        integer("CATWALK_ROLLUP_CACHE_MAX_BYTES", self.rollup_cache_max_bytes, 1)
        integer("CATWALK_ROLLUP_MAX_GROUPS", self.rollup_max_groups, 1)
        integer("CATWALK_ROLLUP_MAX_DEPTH", self.rollup_max_depth, 1, 64)
        integer("CATWALK_ROLLUP_RESPONSE_CHILDREN", self.rollup_response_children, 1)
        if self.rollup_response_children > self.rollup_max_groups:
            errors.append(
                "CATWALK_ROLLUP_RESPONSE_CHILDREN must not exceed CATWALK_ROLLUP_MAX_GROUPS"
            )
        integer("CATWALK_QUERY_THREADS", self.query_threads, 1, 128)
        integer("CATWALK_QUERY_CONCURRENCY", self.query_concurrency, 0, 256)
        number("CATWALK_QUERY_TIMEOUT", self.query_timeout, allow_zero=True)
        number("CATWALK_SNAPSHOT_POLL", self.snapshot_poll)
        integer("CATWALK_PREFETCH_CHILDREN", self.prefetch_children, 0)
        integer("CATWALK_WARM_DEPTH", self.warm_depth, 0)
        integer("CATWALK_WARM_THREADS", self.warm_threads, 1, 64)
        integer("CATWALK_WARM_MAX_DIRS", self.warm_max_dirs, 1)
        number("CATWALK_WARM_INTERVAL", self.warm_interval, allow_zero=True)
        endpoint("VASTDB_ENDPOINT", self.endpoint)
        for i, value in enumerate(self.data_endpoints or []):
            endpoint(f"VASTDB_DATA_ENDPOINTS[{i}]", value)

        if errors:
            raise ValueError("invalid Catwalk configuration:\n  - " + "\n  - ".join(errors))
        return self

    def resolved_data_endpoints(self) -> list[str] | None:
        """Explicit VIP list, or DNS A-record fan-out when auto_endpoints is set."""
        if self.data_endpoints:
            return self.data_endpoints
        if self.auto_endpoints and self.endpoint:
            u = urlparse(self.endpoint)
            host = u.hostname
            port = u.port or (443 if u.scheme == "https" else 80)
            suffix = f":{u.port}" if u.port else ""
            ips = sorted(
                {
                    ai[4][0]
                    for ai in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
                }
            )
            return [f"{u.scheme}://{ip}{suffix}" for ip in ips]
        return None

    def fanout_endpoints(self) -> list[str] | None:
        """Endpoint list sized to query_concurrency.

        The vastdb SDK runs one worker thread per data_endpoints entry and
        explicitly supports repeating an endpoint for more concurrency
        (vastdb/config.py). Cycle the VIP list (or the single endpoint) up
        to query_concurrency entries so scans are not capped at the VIP
        count.
        """
        if self.query_concurrency < 1:
            return self.resolved_data_endpoints()
        base = self.resolved_data_endpoints() or ([self.endpoint] if self.endpoint else None)
        if not base:
            return None
        n = self.query_concurrency
        return [base[i % len(base)] for i in range(n)]

    @property
    def vms_configured(self) -> bool:
        return bool(self.vms_address and self.vms_user and self.vms_password)


def load_config() -> Config:
    """Note on tenants: there is deliberately no tenant option. The catalog
    is scoped server-side to the tenant of the S3 identity in use -- to
    browse a tenant, point VASTDB_ENDPOINT at that tenant's VIP pool with
    that tenant's keys (its user needs an identity policy granting
    s3:Tabular*)."""
    eps = _env("VASTDB_DATA_ENDPOINTS")
    warm = _env("CATWALK_WARM_PATHS")
    return Config(
        endpoint=_env("VASTDB_ENDPOINT"),
        access_key=_env("VASTDB_ACCESS_KEY"),
        secret_key=_env("VASTDB_SECRET_KEY"),
        data_endpoints=[e.strip() for e in eps.split(",") if e.strip()] if eps else None,
        auto_endpoints=_env_bool("CATWALK_AUTO_ENDPOINTS"),
        vms_address=_vms_host(_env("VMS_ADDRESS")),
        vms_user=_env("VMS_USER"),
        vms_password=_env("VMS_PASSWORD"),
        host=_env("CATWALK_HOST", "0.0.0.0"),
        port=_env_num("CATWALK_PORT", int, 8080),
        page_default=_env_num("CATWALK_PAGE_DEFAULT", int, 20),
        page_max=_env_num("CATWALK_PAGE_MAX", int, 100),
        cache_ttl=_env_num("CATWALK_CACHE_TTL", float, 900.0),
        listing_cap=_env_num("CATWALK_LISTING_CAP", int, 500_000),
        num_splits=_env_num("CATWALK_NUM_SPLITS", int, 64),
        cache_max_bytes=_env_num("CATWALK_CACHE_MAX_BYTES", int, 2 * 1024**3),
        mock=_env_bool("CATWALK_MOCK"),
        allow_root=_env_bool("CATWALK_ALLOW_ROOT"),
        rollup_sync_timeout=_env_num("CATWALK_ROLLUP_SYNC_TIMEOUT", float, 5.0),
        rollup_workers=_env_num("CATWALK_ROLLUP_WORKERS", int, 2),
        rollup_threads=_env_num("CATWALK_ROLLUP_THREADS", int, 8),
        rollup_queue_max=_env_num("CATWALK_ROLLUP_QUEUE_MAX", int, 16),
        rollup_cache_max_bytes=_env_num("CATWALK_ROLLUP_CACHE_MAX_BYTES", int, 256 * 1024**2),
        rollup_max_groups=_env_num("CATWALK_ROLLUP_MAX_GROUPS", int, 10_000),
        rollup_max_depth=_env_num("CATWALK_ROLLUP_MAX_DEPTH", int, 8),
        rollup_response_children=_env_num("CATWALK_ROLLUP_RESPONSE_CHILDREN", int, 500),
        query_threads=_env_num("CATWALK_QUERY_THREADS", int, 8),
        query_concurrency=_env_num("CATWALK_QUERY_CONCURRENCY", int, 24),
        query_timeout=_env_num("CATWALK_QUERY_TIMEOUT", float, 60.0),
        snapshot_pin=_env_bool_default("CATWALK_SNAPSHOT_PIN", True),
        snapshot_poll=_env_num("CATWALK_SNAPSHOT_POLL", float, 60.0),
        snapshot_prefix=_env("CATWALK_SNAPSHOT_PREFIX", "big_catalog"),
        prefetch_children=_env_num("CATWALK_PREFETCH_CHILDREN", int, 8),
        warm_paths=[p.strip() for p in warm.split(",") if p.strip()] if warm else None,
        warm_depth=_env_num("CATWALK_WARM_DEPTH", int, 2),
        warm_threads=_env_num("CATWALK_WARM_THREADS", int, 4),
        warm_max_dirs=_env_num("CATWALK_WARM_MAX_DIRS", int, 500),
        warm_interval=_env_num("CATWALK_WARM_INTERVAL", float, 0.0),
    ).validate()


def main() -> int:
    """Deprecated shim: console scripts generated by older installs point at
    catwalk.config:main. The CLI now lives in catwalk.cli."""
    from .cli import main as cli_main

    return cli_main()


if __name__ == "__main__":
    raise SystemExit(main())
