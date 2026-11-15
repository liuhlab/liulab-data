"""Snakemake's command line, run from one server process that imports Snakemake once.

Each run is a fork of the server, with the caller's environment and folder, so it starts as a
fresh ``snakemake`` process would, less starting Python and importing Snakemake.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
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

_server: subprocess.Popen[str] | None = None


def start() -> subprocess.Popen[str]:
    """Start the server, if it has not started, and wait until it has imported Snakemake."""
    global _server
    if _server is None:
        _server = subprocess.Popen(
            [sys.executable, "-c", _SERVER],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert _server.stdout is not None
        assert _server.stdout.readline() == "ready\n"
    return _server


def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    """Run ``snakemake argv``; return its exit code and output, as ``subprocess.run`` would."""
    server = start()
    assert server.stdin is not None
    assert server.stdout is not None
    with tempfile.TemporaryDirectory() as tmp:
        out, err = Path(tmp) / "stdout", Path(tmp) / "stderr"
        request = [argv, dict(os.environ), str(Path.cwd()), str(out), str(err)]
        server.stdin.write(json.dumps(request) + "\n")
        server.stdin.flush()
        code = int(server.stdout.readline())
        return subprocess.CompletedProcess(
            ["snakemake", *argv], code, out.read_text(), err.read_text()
        )


def stop() -> None:
    """Stop the server, if one started."""
    global _server
    if _server is not None:
        assert _server.stdin is not None
        assert _server.stdout is not None
        _server.stdin.close()
        _server.wait()
        _server.stdout.close()
        _server = None
