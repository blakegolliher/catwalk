"""Report the addresses Catwalk is actually listening on, after startup.

uvicorn binds its sockets *after* the ASGI lifespan startup completes, so the
report runs in a short-lived background thread: it polls /proc for this
process's LISTEN sockets (authoritative no matter how uvicorn was launched or
which --port flag was used), expands a wildcard bind (0.0.0.0 / ::) to every
IP assigned on the system, and prints one URL per address. If /proc is
unavailable the report falls back to the configured host/port.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import threading
import time

_LISTEN = "0A"  # st column value for TCP LISTEN in /proc/net/tcp
_V4_WILD = "0.0.0.0"
_V6_WILD = "::"


def _hex_ipv4(h: str) -> str:
    """/proc/net/tcp address hex (little-endian u32) -> dotted quad."""
    return socket.inet_ntop(socket.AF_INET, struct.pack("<I", int(h, 16)))


def _hex_ipv6(h: str) -> str:
    """/proc/net/tcp6 address hex (4 little-endian u32 words) -> IPv6 text."""
    packed = b"".join(struct.pack("<I", int(h[i : i + 8], 16)) for i in range(0, 32, 8))
    return socket.inet_ntop(socket.AF_INET6, packed)


def _parse_proc_tcp(text: str, family: int) -> list[tuple]:
    """LISTEN rows of a /proc/net/tcp{,6} dump -> (family, addr, port, inode)."""
    rows = []
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 10 or parts[3] != _LISTEN:
            continue
        addr_hex, port_hex = parts[1].rsplit(":", 1)
        addr = _hex_ipv4(addr_hex) if family == socket.AF_INET else _hex_ipv6(addr_hex)
        rows.append((family, addr, int(port_hex, 16), int(parts[9])))
    return rows


def _own_socket_inodes(pid: int | str = "self") -> set[int]:
    inodes = set()
    try:
        fds = os.listdir(f"/proc/{pid}/fd")
    except OSError:
        return inodes
    for fd in fds:
        try:
            target = os.readlink(f"/proc/{pid}/fd/{fd}")
        except OSError:
            continue
        if target.startswith("socket:["):
            inodes.add(int(target[8:-1]))
    return inodes


def listening_sockets(pid: int | str = "self") -> list[tuple]:
    """(family, addr, port) for every TCP socket the process LISTENs on.

    Defaults to this process; `catwalk start` passes the spawned server's pid
    (readable because it is our own child) to discover the bound port.
    """
    rows = []
    for path, fam in (("/proc/net/tcp", socket.AF_INET), ("/proc/net/tcp6", socket.AF_INET6)):
        try:
            with open(path) as f:
                rows += _parse_proc_tcp(f.read(), fam)
        except OSError:
            pass
    own = _own_socket_inodes(pid)
    return sorted({(fam, addr, port) for fam, addr, port, inode in rows if inode in own})


def system_ips() -> dict[int, list[str]]:
    """Every IP assigned on the system, keyed by address family.

    Primary source is `ip -j addr` (Linux); fallback resolves the hostname,
    which typically yields only the primary address.
    """
    ips: dict[int, list[str]] = {socket.AF_INET: [], socket.AF_INET6: []}
    try:
        proc = subprocess.run(
            ["ip", "-j", "addr"], capture_output=True, text=True, timeout=5, check=True
        )
        for iface in json.loads(proc.stdout):
            for a in iface.get("addr_info", []):
                fam = {"inet": socket.AF_INET, "inet6": socket.AF_INET6}.get(a.get("family"))
                if fam is not None and a["local"] not in ips[fam]:
                    ips[fam].append(a["local"])
    except Exception:
        try:
            for ai in socket.getaddrinfo(socket.gethostname(), None):
                if ai[0] in ips and ai[4][0] not in ips[ai[0]]:
                    ips[ai[0]].append(ai[4][0])
        except OSError:
            pass
        if "127.0.0.1" not in ips[socket.AF_INET]:
            ips[socket.AF_INET].insert(0, "127.0.0.1")
    return ips


def listening_urls(sockets: list[tuple]) -> list[str]:
    """URLs for the given LISTEN sockets, wildcards expanded to real IPs."""
    ips = None
    urls = []
    for fam, addr, port in sockets:
        if addr in (_V4_WILD, _V6_WILD):
            ips = ips or system_ips()
            urls += [f"http://{ip}:{port}" for ip in ips[socket.AF_INET]]
            if addr == _V6_WILD:  # dual-stack: v6 wildcard covers v4 too
                urls += [f"http://[{ip}]:{port}" for ip in ips[socket.AF_INET6]]
        elif fam == socket.AF_INET6:
            urls.append(f"http://[{addr}]:{port}")
        else:
            urls.append(f"http://{addr}:{port}")
    return list(dict.fromkeys(urls))


def _print_now(msg: str):
    print(msg, flush=True)  # stdout is block-buffered when piped to a file


def report_listening(
    fallback_host: str, fallback_port: int, deadline: float = 15.0, out=_print_now
) -> threading.Thread:
    """Print the listening URLs once the server socket appears (background)."""

    def run():
        t0 = time.monotonic()
        while time.monotonic() - t0 < deadline:
            socks = listening_sockets()
            if socks:
                lines = "\n".join(f"  {u}" for u in listening_urls(socks))
                msg = f"Catwalk is listening on:\n{lines}"
                if not any(a in (_V4_WILD, _V6_WILD) for _f, a, _p in socks):
                    msg += (
                        "\n  (bound to a specific address; set "
                        "CATWALK_HOST=0.0.0.0 to listen on every IP)"
                    )
                out(msg)
                return
            time.sleep(0.2)
        host = fallback_host or _V4_WILD
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        socks = [(family, host, fallback_port)]
        lines = "\n".join(f"  {u}" for u in listening_urls(socks))
        out(f"Catwalk could not verify its sockets via /proc; configured to listen on:\n{lines}")

    t = threading.Thread(target=run, name="netinfo", daemon=True)
    t.start()
    return t
