"""The one driver of the full run and the refresh: its closing steps and its run lock."""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from labdata import _slurm
from labdata.cli import app
from labdata.store import lifecycle, pipeline, refresh
from tests._snakemake import Runner
from tests.test_store_refresh import _jobs

_DATA = Path(__file__).parent / "data" / "geo_slice"
_CLOSING = ("extract", "build", "switch", "prune", "copy")
#: The full run's round folder for the slice's snapshots.
_ROUND = "work/run/2026-10-14-all"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A store with the slice's snapshots, no raw files and no build."""
    root = tmp_path / "store"
    shutil.copytree(_DATA / "snapshots", root / "snapshots")
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return root


def _store(*args: str) -> str:
    result = CliRunner().invoke(app, ["store", *args])
    assert result.exit_code == 0, result.output
    return result.output


def test_the_full_run_and_the_refresh_close_with_the_same_steps_in_the_same_order(
    root: Path, monkeypatch: pytest.MonkeyPatch, snakemake: Runner
) -> None:
    planned: list[list[tuple[str, list[str], list[str], str]]] = []

    def fake(argv: list[str], *, verbose: bool) -> int:
        planned.append(_jobs(snakemake, argv))
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    _store("run", "--root", str(root), "--cores", "4")
    _store("refresh", "--root", str(root), "--cores", "4", "--since", "2026-10-14")

    full, refresh = (
        sorted(
            # The refresh's extracts also wait for its snapshot downloads.
            (rule, sorted(inputs) if rule != "extract" else [], outputs, command)
            for rule, inputs, outputs, command in jobs
            if rule in _CLOSING
        )
        for jobs in planned
    )
    assert full == refresh
    needs = {rule: inputs for rule, inputs, _, _ in full if rule != "extract"}
    assert needs == {
        "build": sorted(["pack.done", *(f"extract-{s}.done" for s in _EXTRACTS)]),
        "switch": ["build.done"],
        "prune": ["switch.done"],
        "copy": ["prune.done"],
    }


_EXTRACTS = ("biosample", "bioproject", "sra-accessions", "geo-series")

#: Slurm jobs by state; Slurm knows no other job, and gives no answer about ``_UNASKED``.
_JOBS = {"4242": "RUNNING", "4241": "CANCELLED"}
_UNASKED = "4240"


@pytest.fixture
def slurm(monkeypatch: pytest.MonkeyPatch) -> None:
    def show_job(job_id: str) -> str | None:
        if job_id == _UNASKED:
            return None
        state = _JOBS.get(job_id)
        return f"JobId={job_id} JobName=refresh\n   JobState={state} Reason=None\n" if state else ""

    monkeypatch.setattr(_slurm, "show_job", show_job)


def _owner(root: Path, *, job: str | None, host: str, pid: int) -> None:
    """Leave the run lock as a refresh's driver on ``host`` holds it."""
    lock = root / "work" / "run.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    owner = {"update": "refresh", "job": job, "host": host, "pid": pid, "started": _ago(0)}
    lock.write_text(json.dumps(owner), encoding="utf-8")


def _ago(days: float) -> str:
    return f"{dt.datetime.now(dt.UTC) - dt.timedelta(days=days):%Y-%m-%dT%H:%M:%SZ}"


def _unfinished(root: Path, days: float) -> str:
    """Record a refresh started ``days`` ago that stopped after pack; return its folder."""
    started = _ago(days)
    folder = f"work/refresh/{started.replace(':', '')}"
    (root / folder).mkdir(parents=True)
    (root / folder / "pack.done").touch()
    record = {
        "update": "refresh",
        "started": started,
        "ended": None,
        "outcome": "running",
        "folder": folder,
        "since": "2026-10-14",
        "dag": refresh.DAG,
    }
    lifecycle.write_status(root, record)
    return folder


def _folders(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace the Snakemake seam: each run records its folder, and stops as if killed."""
    seen: list[str] = []

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen.append(argv[argv.index("--directory") + 1])
        return 1

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    return seen


@pytest.mark.parametrize(
    ("command", "owner"),
    [
        ("refresh", {"job": "4242", "host": "node-b", "pid": 1}),
        ("run", {"job": None, "host": socket.gethostname(), "pid": os.getpid()}),
        ("build", {"job": "4242", "host": "node-b", "pid": 1}),
        ("prune", {"job": None, "host": socket.gethostname(), "pid": os.getpid()}),
        ("remove-sra", {"job": "4242", "host": "node-b", "pid": 1}),
        (
            "restore --from gcs:lab-bucket/geo-sra-store",
            {"job": "4242", "host": "node-b", "pid": 1},
        ),
    ],
)
def test_an_update_refuses_while_another_ones_owner_is_alive(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    slurm: None,
    command: str,
    owner: dict[str, Any],
) -> None:
    _unfinished(root, days=1)
    _owner(root, **owner)
    before = lifecycle.read_status(root)
    seen = _folders(monkeypatch)
    monkeypatch.setattr("labdata.cli._store_client", lambda: None)
    rclone: list[list[str]] = []
    monkeypatch.setattr("labdata._rclone.run", rclone.append)

    result = CliRunner().invoke(app, ["store", *command.split(), "--root", str(root)])

    assert result.exit_code == 1
    assert f"process {owner['pid']} on {owner['host']}" in result.output
    assert seen == rclone == []
    assert lifecycle.read_status(root) == before


def _built(root: Path) -> None:
    """Give the store a current build ``b1``, and a build ``b2`` it has not switched to."""
    for build in ("b1", "b2"):
        (root / "builds" / build).mkdir(parents=True)
    marker = root / "labdata-store.toml"
    marker.write_text(marker.read_text(encoding="utf-8") + 'build = "b1"\n', encoding="utf-8")


@pytest.mark.parametrize("days", [1, 8, None], ids=["recent", "old", "an SRA removal's"])
def test_a_refresh_whose_owner_is_gone_resumes_within_seven_days_and_is_abandoned_after(
    root: Path, monkeypatch: pytest.MonkeyPatch, slurm: None, days: int | None
) -> None:
    _built(root)
    if days is None:
        # The SRA removal stays running, with no run folder, until a build applies it.
        lifecycle.write_status(root, {"update": "remove-sra", "outcome": "running"})
        folder = None
    else:
        folder = _unfinished(root, days)
        lifecycle.write_status(root, lifecycle.read_status(root) | {"build": "b2"})
    _owner(root, job="4241", host="node-b", pid=1)
    seen = _folders(monkeypatch)

    result = CliRunner().invoke(
        app, ["store", "refresh", "--root", str(root), "--since", "2026-11-01"]
    )

    assert result.exit_code == 1, result.output
    status = lifecycle.read_status(root)
    resumed = days == 1
    assert (seen == [str(root / str(folder))]) is resumed
    assert status["update"] == "refresh"
    assert status["since"] == ("2026-10-14" if resumed else "2026-11-01")
    # The killed refresh's unswitched build, and its folder while it may resume, stay.
    _store("prune", "--root", str(root), "--grace", "0")
    assert (root / "builds" / "b2").is_dir() is resumed
    if folder:
        assert (root / folder).is_dir() is resumed
    assert Path(seen[-1]).is_dir()
    assert not (root / "work" / "run.lock").exists()


def test_a_full_run_replacing_a_switched_refresh_hands_on_its_copy_and_prune_removes_its_folder(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _built(root)
    folder = _unfinished(root, days=1)
    switched = {"build": "b1", "outcome": "built", "copy_pending": True}
    lifecycle.write_status(root, lifecycle.read_status(root) | switched)
    seen = _folders(monkeypatch)

    result = CliRunner().invoke(app, ["store", "run", "--root", str(root)])

    assert result.exit_code == 1, result.output
    assert seen == [str(root / _ROUND)]
    assert lifecycle.read_status(root)["copy_pending"] is True
    _store("prune", "--root", str(root))
    assert not (root / folder).exists()
    assert Path(seen[-1]).is_dir()


def test_rerunning_a_finished_round_leaves_the_status_record_and_says_the_round_is_done(
    root: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    folder = root / "work" / "run" / "2026-10-14-all"
    folder.mkdir(parents=True)
    (folder / "copy.done").touch()
    # A hand build after the round finished.
    lifecycle.write_status(root, {"update": "build", "outcome": "finalized", "build": "b1"})
    before = (root / "refresh-status.json").read_bytes()
    seen = _folders(monkeypatch)

    output = _store("run", "--root", str(root))

    assert seen == []
    assert (root / "refresh-status.json").read_bytes() == before
    assert f"{folder} is done" in caplog.text, output


def test_a_finalized_round_rerun_for_a_failed_lane_is_running_and_a_stop_fails_it_to_resume(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = root / _ROUND
    folder.mkdir(parents=True)
    for marker in ("copy.done", "geo.failed"):
        (folder / marker).touch()
    finalized = {"update": "run", "folder": _ROUND, "build": "b1", "started": _ago(2)}
    lifecycle.write_status(root, finalized | {"outcome": "finalized", "ended": _ago(1)})
    during: list[tuple[str, str, str]] = []

    def stopped(argv: list[str], *, verbose: bool) -> int:
        status = lifecycle.read_status(root)
        during.append((argv[argv.index("--directory") + 1], status["outcome"], status["started"]))
        return 1

    monkeypatch.setattr(pipeline, "_run_snakemake", stopped)
    again = ["store", "run", "--root", str(root)]

    for _ in range(2):
        assert CliRunner().invoke(app, again).exit_code == 1
        assert lifecycle.read_status(root)["outcome"] == "failed"

    assert during == [(str(folder), "running", finalized["started"])] * 2


def test_a_round_left_only_its_copy_copies_and_says_its_failed_lane_runs_on_the_next_run(
    root: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    (root / _ROUND).mkdir(parents=True)
    (root / _ROUND / "geo.failed").touch()
    switched = {"update": "run", "folder": _ROUND, "build": "b1", "outcome": "built"}
    lifecycle.write_status(root, switched | {"copy_pending": True})
    seen = _folders(monkeypatch)

    CliRunner().invoke(app, ["store", "run", "--root", str(root)])

    assert seen == []
    assert "the geo lane left items failed; it runs again on the next run" in caplog.text


@pytest.mark.parametrize("job", [None, _UNASKED], ids=["off Slurm", "no Slurm answer"])
def test_a_refresh_whose_owner_cannot_be_checked_refuses_unless_told_it_is_gone(
    root: Path, monkeypatch: pytest.MonkeyPatch, slurm: None, job: str | None
) -> None:
    folder = _unfinished(root, days=1)
    _owner(root, job=job, host="node-b", pid=1)
    seen = _folders(monkeypatch)
    again = ["store", "refresh", "--root", str(root)]

    refused = CliRunner().invoke(app, again)

    assert refused.exit_code == 1
    assert "cannot check" in refused.output
    assert "--owner-gone" in refused.output
    assert seen == []
    assert CliRunner().invoke(app, [*again, "--owner-gone"]).exit_code == 1
    assert seen == [str(root / folder)]


#: A refresh driver whose Snakemake run writes its folder to a file, then never ends.
_DRIVER = """
import sys, threading
from pathlib import Path
from labdata.cli import app
from labdata.store import pipeline

def hang(argv, *, verbose):
    Path(sys.argv[2]).write_text(argv[argv.index("--directory") + 1])
    threading.Event().wait()
    return 0

pipeline._run_snakemake = hang
app(["store", "refresh", "--root", sys.argv[1], "--since", "2026-10-14"])
"""


@pytest.mark.slow
def test_a_real_refresh_driver_killed_mid_run_is_resumed_by_the_next_start(
    root: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    running = tmp_path / "running"
    driver = subprocess.Popen([sys.executable, "-c", _DRIVER, str(root), str(running)])
    try:
        deadline = time.monotonic() + 60
        while not running.is_file() and driver.poll() is None and time.monotonic() < deadline:
            threading.Event().wait(0.1)
        assert running.is_file(), "the driver never reached its Snakemake run"
        folder = running.read_text()
        again = ["store", "refresh", "--root", str(root)]

        refused = CliRunner().invoke(app, again)

        assert refused.exit_code == 1
        assert f"process {driver.pid} on {socket.gethostname()}" in refused.output
    finally:
        driver.kill()
        driver.wait()
    assert lifecycle.read_status(root)["outcome"] == "running"
    seen = _folders(monkeypatch)

    assert CliRunner().invoke(app, again).exit_code == 1

    assert seen == [folder]
    assert lifecycle.read_status(root)["outcome"] == "failed"
