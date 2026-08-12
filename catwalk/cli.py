"""Catwalk command line: `start` / `stop` / `status` process management plus
the foreground `run` mode.

`catwalk start` validates the configuration, spawns `python -m catwalk run`
detached from the terminal (its own session, stdout/stderr appended to the log
file), waits for the server socket to appear, and prints the listening URLs.
State lives in CATWALK_STATE_DIR (default ~/.catwalk):

    catwalk.env    optional KEY=VALUE settings (same names as the env vars),
                   loaded by `start`/`run`; shell env and flags override it
    catwalk.json   pid, bind address, resolved port, log path, start time
    catwalk.log    server output, appended across restarts

One state dir manages one instance; to run several (e.g. one per tenant, per
the README), point each at its own CATWALK_STATE_DIR.

Legacy invocations keep working: `catwalk --mock --port 8080` (no subcommand)
serves in the foreground exactly like `catwalk run --mock --port 8080`.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from .config import Config, load_config
from .netinfo import listening_sockets, listening_urls

COMMANDS = ("run", "start", "stop", "status")

_START_DEADLINE_S = 20.0
_STOP_DEADLINE_S = 10.0
_HELP = """\
usage: catwalk [command] [options]

commands:
  start    launch the server in the background and print its URLs
  stop     stop the background server (--force to SIGKILL)
  status   show whether the server runs and probe /api/health (exit 3 if down)
  run      serve in the foreground, logging to the terminal (the default)

Running with no command (`catwalk --mock --port 8080`) behaves like `run`.
State and logs live in CATWALK_STATE_DIR (default ~/.catwalk); settings can be
kept in <state-dir>/catwalk.env (KEY=VALUE, same names as the env vars) or a
file passed with --env-file — shell env and flags override the file.
`catwalk <command> --help` shows per-command options.
"""


# ---- state files ------------------------------------------------------------


def state_dir() -> Path:
    return Path(os.environ.get("CATWALK_STATE_DIR") or "~/.catwalk").expanduser()


def state_file() -> Path:
    return state_dir() / "catwalk.json"


def log_file() -> Path:
    return state_dir() / "catwalk.log"


def env_file() -> Path:
    return state_dir() / "catwalk.env"


def read_state() -> dict | None:
    try:
        return json.loads(state_file().read_text())
    except (OSError, ValueError):
        return None


def load_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=VALUE settings file: blank lines and # comments skipped,
    an optional `export ` prefix tolerated (so the file stays source-able),
    values may be wrapped in single or double quotes. Anything else is a
    config error and fails fast, like Config.validate()."""
    entries: dict[str, str] = {}
    for lineno, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            sys.exit(f"{path}:{lineno}: expected KEY=VALUE, got {raw.strip()!r}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        entries[key] = value
    return entries


def _apply_env_file(explicit: str | None) -> Path | None:
    """Load settings into os.environ as *defaults*: already-set shell env
    vars win, and flags (applied afterwards) win over both. Returns the
    loaded path, or None when no file exists."""
    path = Path(explicit).expanduser() if explicit else env_file()
    if not path.is_file():
        if explicit:
            sys.exit(f"env file not found: {path}")
        return None
    if path.stat().st_mode & 0o077:
        print(
            f"warning: {path} is readable by other users; consider: chmod 600 {path}",
            file=sys.stderr,
        )
    for key, value in load_env_file(path).items():
        os.environ.setdefault(key, value)
    return path


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # kill(0) succeeds on zombies, but a zombie is already dead — it lingers
    # only because a still-alive spawner has not reaped it (e.g. a process
    # driving cli.main in-process, like the tests). /proc state 'Z' tells.
    try:
        with open(f"/proc/{pid}/stat") as f:
            state = f.read().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return True
    return state != "Z"


def _is_catwalk_process(pid: int) -> bool:
    """Guard against pid reuse before signalling anything."""
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except OSError:
        return process_alive(pid)  # no /proc: trust the state file
    return any(b"catwalk" in part for part in cmdline)


def running_state() -> dict | None:
    """State of a live managed server; clears stale state as a side effect."""
    st = read_state()
    if st is None:
        return None
    pid = st.get("pid")
    if isinstance(pid, int) and process_alive(pid) and _is_catwalk_process(pid):
        return st
    state_file().unlink(missing_ok=True)
    return None


def _tail(path: Path, lines: int = 15) -> str:
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return "(log unreadable)"


# ---- argument parsing -------------------------------------------------------


def _serve_parser(command: str, description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=f"catwalk {command}", description=description)
    p.add_argument(
        "--env-file",
        help="KEY=VALUE settings file loaded first (default: <state-dir>/catwalk.env "
        "if present); shell env vars and flags override it",
    )
    p.add_argument("--host", help="bind address (env CATWALK_HOST)")
    p.add_argument("--port", type=int, help="bind port; 0 picks a free one (env CATWALK_PORT)")
    p.add_argument(
        "--mock",
        action="store_true",
        help="serve the synthetic demo namespace (env CATWALK_MOCK=1)",
    )
    p.add_argument("--endpoint", help="data VIP pool URL (env VASTDB_ENDPOINT)")
    p.add_argument("--access-key", help="env VASTDB_ACCESS_KEY")
    p.add_argument("--secret-key", help="env VASTDB_SECRET_KEY")
    p.add_argument("--data-endpoints", help="comma-separated VIP URLs (env VASTDB_DATA_ENDPOINTS)")
    p.add_argument(
        "--auto-endpoints",
        action="store_true",
        help="resolve endpoint A records, fan out across every VIP",
    )
    p.add_argument("--vms-address", help="env VMS_ADDRESS")
    p.add_argument("--vms-user", help="env VMS_USER")
    p.add_argument("--vms-password", help="env VMS_PASSWORD")
    return p


def _apply_flag_overrides(args: argparse.Namespace) -> None:
    """Flags override the environment; the server process reads only env."""
    overrides = {
        "CATWALK_HOST": args.host,
        "CATWALK_PORT": str(args.port) if args.port is not None else None,
        "CATWALK_MOCK": "1" if args.mock else None,
        "VASTDB_ENDPOINT": args.endpoint,
        "VASTDB_ACCESS_KEY": args.access_key,
        "VASTDB_SECRET_KEY": args.secret_key,
        "VASTDB_DATA_ENDPOINTS": args.data_endpoints,
        "CATWALK_AUTO_ENDPOINTS": "1" if args.auto_endpoints else None,
        "VMS_ADDRESS": args.vms_address,
        "VMS_USER": args.vms_user,
        "VMS_PASSWORD": args.vms_password,
    }
    for k, v in overrides.items():
        if v is not None:
            os.environ[k] = v


def _load_validated_config() -> Config:
    try:
        return load_config()
    except ValueError as e:
        sys.exit(str(e))


# ---- commands ---------------------------------------------------------------


def _note_unconfigured_backend(cfg: Config, env_path: Path | None) -> None:
    if not cfg.mock and not cfg.endpoint:
        where = env_path or env_file()
        print(
            "note: no VASTDB_ENDPOINT configured -- the UI will report "
            f"'catalog backend unavailable'. Set VASTDB_ENDPOINT / "
            f"VASTDB_ACCESS_KEY / VASTDB_SECRET_KEY in {where}, or pass --mock "
            "for the demo namespace.",
            file=sys.stderr,
        )


def cmd_run(argv: list[str]) -> int:
    args = _serve_parser("run", "Serve in the foreground (logs to the terminal).").parse_args(argv)
    env_path = _apply_env_file(args.env_file)
    _apply_flag_overrides(args)
    cfg = _load_validated_config()
    if env_path:
        print(f"using env file: {env_path}")
    _note_unconfigured_backend(cfg, env_path)

    import uvicorn

    uvicorn.run("catwalk.app:app", host=cfg.host, port=cfg.port)
    return 0


def cmd_start(argv: list[str]) -> int:
    args = _serve_parser(
        "start", "Launch the server in the background and print its URLs."
    ).parse_args(argv)
    env_path = _apply_env_file(args.env_file)
    _apply_flag_overrides(args)
    cfg = _load_validated_config()  # fail here, not in an unread log file
    if env_path:
        print(f"using env file: {env_path}")
    _note_unconfigured_backend(cfg, env_path)

    existing = running_state()
    if existing:
        sys.exit(
            f"catwalk is already running (pid {existing['pid']}, port "
            f"{existing.get('port', '?')}); run 'catwalk stop' first, or use a "
            "different CATWALK_STATE_DIR for a second instance"
        )

    state_dir().mkdir(parents=True, exist_ok=True)
    log = log_file()
    started = datetime.now(timezone.utc)
    with open(log, "ab") as lf:
        lf.write(f"---- catwalk start {started.isoformat(timespec='seconds')} ----\n".encode())
        lf.flush()
        child = subprocess.Popen(
            [sys.executable, "-m", "catwalk", "run"],
            stdout=lf,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,  # survives this terminal closing
        )

    # Wait for the listen socket (uvicorn binds after app startup) so we can
    # print real URLs -- and catch immediate deaths with the log tail instead
    # of leaving the user to discover a silently absent process.
    socks: list[tuple] = []
    deadline = time.monotonic() + _START_DEADLINE_S
    while time.monotonic() < deadline:
        if child.poll() is not None:
            sys.exit(
                f"catwalk exited during startup (code {child.returncode}); "
                f"last log lines from {log}:\n{_tail(log)}"
            )
        socks = listening_sockets(child.pid)
        if socks:
            break
        time.sleep(0.2)

    port = socks[0][2] if socks else cfg.port
    state_file().write_text(
        json.dumps(
            {
                "pid": child.pid,
                "host": cfg.host,
                "port": port,
                "mock": cfg.mock,
                "log": str(log),
                "started": started.isoformat(timespec="seconds"),
                "started_epoch": started.timestamp(),
            },
            indent=2,
        )
        + "\n"
    )

    print(f"catwalk started (pid {child.pid}), logging to {log}")
    if socks:
        print("listening on:")
        for url in listening_urls(socks):
            print(f"  {url}")
    else:
        print(
            f"no listening socket after {_START_DEADLINE_S:.0f}s -- still "
            f"starting; watch the log or run 'catwalk status'"
        )
    print("stop with: catwalk stop")
    return 0


def cmd_stop(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="catwalk stop", description="Stop the background server.")
    p.add_argument("--force", action="store_true", help="SIGKILL instead of graceful SIGTERM")
    args = p.parse_args(argv)

    st = read_state()
    if st is None:
        print(f"catwalk is not running (no state in {state_dir()})")
        return 0
    pid = st.get("pid")
    if not (isinstance(pid, int) and process_alive(pid) and _is_catwalk_process(pid)):
        state_file().unlink(missing_ok=True)
        print("catwalk is not running (cleaned up stale state)")
        return 0

    os.kill(pid, signal.SIGKILL if args.force else signal.SIGTERM)
    deadline = time.monotonic() + _STOP_DEADLINE_S
    while time.monotonic() < deadline:
        if not process_alive(pid):
            break
        time.sleep(0.2)
    if process_alive(pid):
        sys.exit(
            f"catwalk (pid {pid}) did not exit within {_STOP_DEADLINE_S:.0f}s; "
            "try 'catwalk stop --force'"
        )
    # If the server was spawned by this very process, reap the zombie.
    with contextlib.suppress(OSError):
        os.waitpid(pid, os.WNOHANG)
    state_file().unlink(missing_ok=True)
    print(f"catwalk stopped (pid {pid})")
    return 0


def cmd_status(argv: list[str]) -> int:
    argparse.ArgumentParser(
        prog="catwalk status",
        description="Show whether the server runs; exit 0 when up, 3 when down.",
    ).parse_args(argv)

    st = running_state()
    if st is None:
        print("catwalk is not running")
        return 3

    uptime = ""
    epoch = st.get("started_epoch")
    if isinstance(epoch, (int, float)):
        minutes = (time.time() - epoch) / 60
        uptime = f", up {minutes / 60:.1f}h" if minutes >= 90 else f", up {minutes:.0f}m"
    mode = " [mock]" if st.get("mock") else ""
    print(f"catwalk is running: pid {st['pid']}, port {st.get('port', '?')}{mode}{uptime}")
    print(f"log: {st.get('log', log_file())}")

    host = st.get("host") or "127.0.0.1"
    probe_host = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    if ":" in probe_host:
        probe_host = f"[{probe_host}]"
    url = f"http://{probe_host}:{st.get('port')}/api/health"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            health = json.loads(resp.read())
    except Exception as e:
        print(f"health: {url} not answering ({e}) -- may still be starting")
        return 0
    print(
        f"health: catalog_reachable={health.get('catalog_reachable')} "
        f"mode={health.get('mode')} vms={health.get('vms')}"
    )
    return 0


# ---- entry point ------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] in ("-h", "--help"):
        print(_HELP, end="")
        return 0
    if argv and not argv[0].startswith("-"):
        if argv[0] not in COMMANDS:
            sys.exit(f"unknown command {argv[0]!r}; expected one of: {', '.join(COMMANDS)}")
        command, rest = argv[0], argv[1:]
    else:
        command, rest = "run", argv  # legacy: bare flags serve in the foreground
    handler = {"run": cmd_run, "start": cmd_start, "stop": cmd_stop, "status": cmd_status}
    return handler[command](rest)


if __name__ == "__main__":
    raise SystemExit(main())
