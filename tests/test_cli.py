"""`catwalk start/status/stop` process management, against the mock backend.

Each test points CATWALK_STATE_DIR at a tmp dir; `start` binds port 0 so
parallel test runs never collide, and the CLI must discover the real port
from the child's listen socket.
"""

import json
import os
import subprocess
import time
import urllib.request

import pytest

from catwalk import cli


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CATWALK_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("CATWALK_MOCK", "1")
    monkeypatch.setenv("CATWALK_PORT", "0")
    monkeypatch.setenv("CATWALK_PREFETCH_CHILDREN", "0")
    monkeypatch.delenv("CATWALK_WARM_PATHS", raising=False)
    return tmp_path


def _wait_dead(pid: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not cli.process_alive(pid):
            return True
        time.sleep(0.1)
    return False


def test_start_status_stop_roundtrip(clean_env, capsys):
    assert cli.main(["start"]) == 0
    st = json.loads((clean_env / "catwalk.json").read_text())
    pid = st["pid"]
    try:
        assert st["port"] > 0, "start must discover the real port behind --port 0"
        with urllib.request.urlopen(f"http://127.0.0.1:{st['port']}/api/health", timeout=5) as resp:
            assert json.loads(resp.read())["mode"] == "mock"

        assert cli.main(["status"]) == 0
        out = capsys.readouterr().out
        assert str(pid) in out
        assert "[mock]" in out

        # A second start against the same state dir must refuse, not respawn.
        with pytest.raises(SystemExit):
            cli.main(["start"])
    finally:
        stopped = cli.main(["stop"])
    assert stopped == 0
    assert not (clean_env / "catwalk.json").exists()
    assert _wait_dead(pid), "server process survived catwalk stop"


def test_status_when_not_running(clean_env, capsys):
    assert cli.main(["status"]) == 3
    assert "not running" in capsys.readouterr().out


def test_stop_is_idempotent(clean_env, capsys):
    assert cli.main(["stop"]) == 0
    assert "not running" in capsys.readouterr().out


def test_stale_state_is_cleaned(clean_env, capsys):
    proc = subprocess.Popen(["true"])
    proc.wait()  # a real pid that is certainly dead now
    (clean_env / "catwalk.json").write_text(json.dumps({"pid": proc.pid, "port": 1}))
    assert cli.main(["status"]) == 3
    assert not (clean_env / "catwalk.json").exists()


def test_env_file_parsing(tmp_path):
    f = tmp_path / "catwalk.env"
    f.write_text(
        "# demo settings\n"
        "export VASTDB_ENDPOINT=http://pool1\n"
        "VMS_USER = admin\n"
        'QUOTED="a b"\n'
        "\n"
        "EMPTY=\n"
    )
    assert cli.load_env_file(f) == {
        "VASTDB_ENDPOINT": "http://pool1",
        "VMS_USER": "admin",
        "QUOTED": "a b",
        "EMPTY": "",
    }


def test_env_file_rejects_malformed_lines(tmp_path):
    f = tmp_path / "catwalk.env"
    f.write_text("VASTDB_ENDPOINT=http://pool1\nthis is not a setting\n")
    with pytest.raises(SystemExit) as exc:
        cli.load_env_file(f)
    assert ":2:" in str(exc.value)


def test_explicit_env_file_must_exist(clean_env):
    with pytest.raises(SystemExit):
        cli.main(["start", "--env-file", str(clean_env / "nope.env")])


def test_shell_env_wins_over_env_file(clean_env, monkeypatch):
    (clean_env / "catwalk.env").write_text("CATWALK_PAGE_MAX=40\nCATWALK_NUM_SPLITS=32\n")
    monkeypatch.setenv("CATWALK_PAGE_MAX", "60")
    # Absent during the test, restored afterwards even though the loader
    # writes it into os.environ (setenv records the undo, delenv the state).
    monkeypatch.setenv("CATWALK_NUM_SPLITS", "sentinel")
    monkeypatch.delenv("CATWALK_NUM_SPLITS")
    cli._apply_env_file(None)
    assert os.environ["CATWALK_PAGE_MAX"] == "60"  # shell env kept
    assert os.environ["CATWALK_NUM_SPLITS"] == "32"  # file filled the gap


def test_start_reads_env_file_from_state_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("CATWALK_STATE_DIR", str(tmp_path))
    # Ensure CATWALK_MOCK is absent during the test but restored afterwards,
    # even though the loader writes it into os.environ.
    monkeypatch.setenv("CATWALK_MOCK", "1")
    monkeypatch.delenv("CATWALK_MOCK")
    (tmp_path / "catwalk.env").write_text(
        "CATWALK_MOCK=1\nCATWALK_PORT=0\nCATWALK_PREFETCH_CHILDREN=0\n"
    )
    assert cli.main(["start"]) == 0
    try:
        st = json.loads((tmp_path / "catwalk.json").read_text())
        assert st["mock"] is True, "mock setting must come from the env file"
        assert st["port"] > 0
    finally:
        assert cli.main(["stop"]) == 0
