"""The repo's scripts run in this process: each imported once, its ``main`` called here.

A run returns what ``subprocess.run`` would: the exit code, standard output and standard error.
"""

from __future__ import annotations

import functools
import importlib.util
import io
import json
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPTS = Path(__file__).parents[1] / "scripts"
COMPARE_BUILDS = SCRIPTS / "store" / "compare_builds.py"
BENCHMARK = SCRIPTS / "store" / "benchmark.py"

#: How the benchmark runs: as the script with a child process per query, or its ``main`` here
#: with each query run here too, at a canned cost of one second and one MiB.
BENCHMARK_MODES = ["here", pytest.param("processes", marks=pytest.mark.slow)]


@functools.cache
def load(script: Path) -> ModuleType:
    """Import a script as a module, once a session."""
    spec = importlib.util.spec_from_file_location(script.stem, script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_main(script: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    """Call the script's ``main(args)`` here, as running ``script args`` would."""
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = load(script).main(args)
    return subprocess.CompletedProcess([str(script), *args], code, out.getvalue(), err.getvalue())


def compare(old: Path, new: Path) -> subprocess.CompletedProcess[str]:
    """Run ``compare_builds.py old new`` here."""
    return run_main(COMPARE_BUILDS, [str(old), str(new)])


def benchmark(
    args: list[str], cwd: Path, mode: str, monkeypatch: pytest.MonkeyPatch
) -> subprocess.CompletedProcess[str]:
    """Run ``benchmark.py args`` in ``cwd``, in one of :data:`BENCHMARK_MODES`."""
    if mode == "processes":
        return subprocess.run(
            [sys.executable, str(BENCHMARK), *args],
            capture_output=True,
            text=True,
            check=False,
            cwd=cwd,
        )
    module = load(BENCHMARK)

    def measure(name: str, root: Path, samples_file: Path, tmp: Path) -> dict[str, Any]:
        rows = module.run_query(name, root, json.loads(samples_file.read_text()), tmp)
        return {"query": name, "peak_rss_mib": 1.0, "rows": rows, "wall_s": 1.0, "cpu_s": 1.0}

    monkeypatch.setattr(module, "measure", measure)
    monkeypatch.chdir(cwd)
    return run_main(BENCHMARK, args)
