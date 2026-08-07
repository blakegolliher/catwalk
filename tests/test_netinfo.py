"""Listening-address report: /proc parsing, wildcard expansion, live report."""

import socket

from catwalk import netinfo

# One header + one LISTEN row (127.0.0.1:8080, inode 99) + one ESTABLISHED row.
PROC_TCP = """\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 99 1 0 100 0 0 10 0
   1: 0100007F:D431 0100007F:1F90 01 00000000:00000000 00:00000000 00000000  1000        0 12 1 0 20 4 30 10 -1
"""


def test_parse_proc_tcp_listen_rows_only():
    rows = netinfo._parse_proc_tcp(PROC_TCP, socket.AF_INET)
    assert rows == [(socket.AF_INET, "127.0.0.1", 8080, 99)]


def test_hex_address_decoding():
    assert netinfo._hex_ipv4("0100007F") == "127.0.0.1"
    assert netinfo._hex_ipv4("00000000") == "0.0.0.0"
    assert netinfo._hex_ipv6("0" * 32) == "::"


def test_listening_sockets_sees_own_bind():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        assert (socket.AF_INET, "127.0.0.1", port) in netinfo.listening_sockets()
    assert (socket.AF_INET, "127.0.0.1", port) not in netinfo.listening_sockets()


def test_listening_sockets_degrades_when_proc_fd_is_unavailable(monkeypatch):
    original = netinfo.os.listdir

    def unavailable(path):
        if path == "/proc/self/fd":
            raise FileNotFoundError(path)
        return original(path)

    monkeypatch.setattr(netinfo.os, "listdir", unavailable)
    assert netinfo._own_socket_inodes() == set()


def test_wildcard_expands_to_system_ips(monkeypatch):
    monkeypatch.setattr(
        netinfo,
        "system_ips",
        lambda: {socket.AF_INET: ["127.0.0.1", "10.0.0.5"], socket.AF_INET6: ["::1"]},
    )
    urls = netinfo.listening_urls([(socket.AF_INET, "0.0.0.0", 8080)])
    assert urls == ["http://127.0.0.1:8080", "http://10.0.0.5:8080"]
    urls6 = netinfo.listening_urls([(socket.AF_INET6, "::", 8080)])
    assert urls6 == ["http://127.0.0.1:8080", "http://10.0.0.5:8080", "http://[::1]:8080"]


def test_specific_bind_not_expanded():
    urls = netinfo.listening_urls([(socket.AF_INET, "192.168.1.5", 9090)])
    assert urls == ["http://192.168.1.5:9090"]


def test_system_ips_includes_loopback():
    assert "127.0.0.1" in netinfo.system_ips()[socket.AF_INET]


def test_report_waits_for_bind_then_prints():
    msgs = []
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        t = netinfo.report_listening("127.0.0.1", port, deadline=5.0, out=msgs.append)
        t.join(timeout=10.0)
    assert msgs and f"http://127.0.0.1:{port}" in msgs[0]
    # bound to a specific address -> the wildcard hint must appear
    assert "CATWALK_HOST=0.0.0.0" in msgs[0]


def test_report_fallback_formats_ipv6(monkeypatch):
    monkeypatch.setattr(netinfo, "listening_sockets", lambda: [])
    msgs = []
    thread = netinfo.report_listening("::1", 8080, deadline=0, out=msgs.append)
    thread.join(timeout=1)
    assert "http://[::1]:8080" in msgs[0]
