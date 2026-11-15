"""Snakemake's command line, run from one server process that imports Snakemake once.

Each run is a fork of the server, given the caller's environment and folder. It starts as a
fresh ``snakemake`` process would, less starting Python and importing Snakemake, with one
difference: a module the server imported read the environment when the server started.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

_SERVER = """
import importlib, json, os, sys
import snakemake.cli

# What a dry run imports lazily, so each fork has it already.
for name in (
    "snakemake.api", "snakemake.assets", "snakemake.profiles", "snakemake.report",
    "snakemake.storage", "docutils", "jinja2", "numpy", "pygments", "requests", "smart_open",
    "yaml", "yte",
):
    importlib.import_module(name)
print("ready", flush=True)
for line in sys.stdin:
    argv, env, cwd, out, err = json.loads(line)
    pid = os.fork()
    if pid == 0:
        os.environ.clear()
        os.environ.update(env)
        os.chdir(cwd)
        for fd, path in ((1, out), (2, err)):
            os.dup2(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC), fd)
        code = 0
        try:
            snakemake.cli.main(argv)
        except SystemExit as stop:
            code = stop.code if isinstance(stop.code, int) else 1
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
        os._exit(code)
    _, status = os.waitpid(pid, 0)
    print(os.waitstatus_to_exitcode(status), flush=True)
"""

#: The ``snakemake`` fixture: :func:`run`.
Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]
#: Seconds :func:`stop` waits for the server to exit before it kills it.
STOP_WAIT = 5

_server: subprocess.Popen[str] | None = None
_ready = False


def start() -> None:
    """Start the server, if it has not started; :func:`ready` waits for its imports."""
    global _server, _ready
    if _server is None:
        # Its own process group, so a kill reaches a run it has forked too.
        _server = subprocess.Popen(
            [sys.executable, "-c", _SERVER],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        _ready = False


def ready() -> None:
    """Start the server, if it has not started, and wait until it has imported Snakemake."""
    global _ready
    start()
    if _ready:
        return
    assert _server is not None
    assert _server.stdout is not None
    try:
        line = _server.stdout.readline()
    except BaseException:
        _kill()
        raise
    if line != "ready\n":
        _kill()
        raise RuntimeError(f"the Snakemake server stopped before it was ready: {line!r}")
    _ready = True


def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    """Run ``snakemake argv``; return its exit code and output, as ``subprocess.run`` would.

    A run cut off part-way, by a timeout or any other error, kills the server: the next run
    starts a new one, and never reads the reply this one left unread.
    """
    ready()
    assert _server is not None
    assert _server.stdin is not None
    assert _server.stdout is not None
    with tempfile.TemporaryDirectory() as tmp:
        out, err = Path(tmp) / "stdout", Path(tmp) / "stderr"
        request = [argv, dict(os.environ), str(Path.cwd()), str(out), str(err)]
        try:
            _server.stdin.write(json.dumps(request) + "\n")
            _server.stdin.flush()
            code = int(_server.stdout.readline())
        except BaseException:
            _kill()
            raise
        return subprocess.CompletedProcess(
            ["snakemake", *argv], code, out.read_text(), err.read_text()
        )


def _kill() -> None:
    """Kill the server and any run it forked, and forget them."""
    if _server is not None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(_server.pid, signal.SIGKILL)
        _forget()


def _forget() -> None:
    global _server, _ready
    assert _server is not None
    _server.wait()
    for pipe in (_server.stdin, _server.stdout):
        if pipe is not None:
            with contextlib.suppress(OSError):
                pipe.close()
    _server, _ready = None, False


def stop() -> None:
    """Stop the server, if one started: close its input, wait a little, then kill it."""
    if _server is None:
        return
    assert _server.stdin is not None
    try:
        _server.stdin.close()
        _server.wait(STOP_WAIT)
    except (OSError, subprocess.TimeoutExpired):
        _kill()
        return
    _forget()
