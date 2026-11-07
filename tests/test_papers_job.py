"""``labdata papers serve`` and ``labdata papers copy``: the paper job and its GCS copy."""

from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import init_paper_store, open_paper_store
from labdata.papers import job as job_mod
from labdata.papers.openalex import OpenAlex
from tests._paper_fakes import Article, FakeWeb, fixture_build, work

PMID = "41000001"
DEST = "gcs:bucket/datalake/paper-store"
JOB = "4242"
SCRIPT = "/repo/scripts/papers/serve.sbatch"


class Clock:
    """A UTC clock that moves only when the job sleeps."""

    def __init__(self, start: dt.datetime) -> None:
        self.now = start

    def __call__(self) -> dt.datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += dt.timedelta(seconds=seconds)


class Sbatch:
    """Stand-in for the successor submission: records each command, answers a job ID."""

    def __init__(self, returncode: int = 0, stderr: str = "") -> None:
        self.calls: list[list[str]] = []
        self.returncode, self.stderr = returncode, stderr

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        return subprocess.CompletedProcess(args, self.returncode, "4243\n", self.stderr)


class Rclone:
    """Stand-in for the rclone runner: records each command, answers with one result."""

    def __init__(self, returncode: int = 0, stderr: str = "") -> None:
        self.calls: list[list[str]] = []
        self.returncode, self.stderr = returncode, stderr

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        return subprocess.CompletedProcess(args, self.returncode, "", self.stderr)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    monkeypatch.delenv("OPENALEX_API_KEYS", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)


@pytest.fixture
def sbatch(monkeypatch: pytest.MonkeyPatch) -> Sbatch:
    fake = Sbatch()
    monkeypatch.setattr(job_mod, "_submit", fake)
    return fake


@pytest.fixture
def rclone(monkeypatch: pytest.MonkeyPatch) -> Rclone:
    fake = Rclone()
    monkeypatch.setattr("labdata.papers.copy._run_rclone", fake)
    return fake


def paper_store(tmp_path: Path) -> Path:
    store = fixture_build(tmp_path / "store", [("GSE1", f"PMID:{PMID}", "SOFT")])
    root = tmp_path / "papers"
    init_paper_store(root, store, copy_to=DEST)
    return root


def synced(root: Path) -> Path:
    """Record this month's PMC sync, so only the daily pass is due."""
    (root / "paper-status.json").write_text(json.dumps({"last_pmc_sync_success": "2026-11-03"}))
    return root


def serve(root: Path, clock: Clock, until: dt.datetime) -> list[str]:
    """Run the paper job from the clock's time until ``until``; return its log lines."""
    web = FakeWeb(
        articles={PMID: Article(PMID, "A paper", 2020)},
        works=[work("W1", pmid=PMID)],
    )
    lines: list[str] = []
    job_mod.serve(
        open_paper_store(root),
        client=EntrezClient(
            NcbiCredentials("t@t.org", "abc"), transport=web, limiter=RateLimiter(1000.0, 8)
        ),
        openalex=OpenAlex(transport=web, limiter=RateLimiter(1000.0, 8)),
        clock=clock,
        sleep=clock.sleep,
        until=until,
        job_id=JOB,
        successor=SCRIPT,
        log=lines.append,
    )
    return lines


def utc(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text).replace(tzinfo=dt.UTC)


def passes(lines: list[str]) -> list[str]:
    """The passes the log says ran, as ``<kind> <date> at <time>``."""
    return [line for line in lines if " pass " in line and line.endswith(": success")]


def status(root: Path) -> dict:
    return json.loads((root / "paper-status.json").read_text())


def test_one_daily_pass_a_day_after_0030_and_the_pmc_sync_on_the_3rd(
    tmp_path: Path, sbatch: Sbatch, rclone: Rclone
) -> None:
    root = paper_store(tmp_path)
    clock = Clock(utc("2026-11-01 23:00"))
    lines = serve(root, clock, until=utc("2026-11-04 02:00"))

    assert passes(lines) == [
        "2026-11-01T23:00Z daily pass 2026-11-01: success",
        "2026-11-02T00:30Z daily pass 2026-11-02: success",
        "2026-11-03T00:30Z daily pass 2026-11-03: success",
        "2026-11-03T00:30Z pmc-sync pass 2026-11-03: success",
        "2026-11-04T00:30Z daily pass 2026-11-04: success",
    ]
    assert status(root)["last_daily_success"] == "2026-11-04"
    assert status(root)["last_pmc_sync_success"] == "2026-11-03"
    # Every pass ends with a copy to GCS.
    assert len([c for c in rclone.calls if c[1] == "copy"]) == 5
    assert sbatch.calls == [
        ["sbatch", "--parsable", f"--dependency=afterany:{JOB}", SCRIPT],
    ]
    # Idle, the job prints a line a minute.
    idle = [line for line in lines if line.startswith("2026-11-02T01:") and " idle" in line]
    assert len(idle) == 60


def test_after_a_restart_only_what_is_due_runs_and_one_successor_is_submitted(
    tmp_path: Path, sbatch: Sbatch, rclone: Rclone
) -> None:
    root = paper_store(tmp_path)
    (root / "paper-status.json").write_text(
        json.dumps(
            {
                "last_daily_success": "2026-11-04",
                "last_pmc_sync_success": "2026-11-03",
                "last_pass": {"kind": "daily", "outcome": "failed"},
            }
        )
    )
    clock = Clock(utc("2026-11-05 07:00"))
    first = serve(root, clock, until=utc("2026-11-05 08:00"))
    # The same job restarted (requeued) the same day.
    second = serve(root, clock, until=utc("2026-11-05 09:00"))

    assert passes(first) == ["2026-11-05T07:00Z daily pass 2026-11-05: success"]
    assert passes(second) == []
    assert len(sbatch.calls) == 1


def test_a_failed_successor_submission_is_logged_and_the_job_goes_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rclone: Rclone
) -> None:
    root = synced(paper_store(tmp_path))
    monkeypatch.setattr(job_mod, "_submit", Sbatch(1, "sbatch: error: Access denied"))
    lines = serve(root, Clock(utc("2026-11-05 07:00")), until=utc("2026-11-05 07:10"))

    assert any("successor not submitted" in line and "Access denied" in line for line in lines)
    assert passes(lines) == ["2026-11-05T07:00Z daily pass 2026-11-05: success"]


def copy(root: Path) -> str:
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root)])
    assert result.exit_code == 0, result.output
    return result.output


def test_copy_sends_new_and_changed_files_but_no_lock_journal_or_temp_file(
    tmp_path: Path, rclone: Rclone
) -> None:
    root = paper_store(tmp_path)
    copy(root)

    [args] = rclone.calls
    assert args[:4] == ["rclone", "copy", str(root), DEST]
    assert "--exclude=.*" in args
    assert status(root)["copy_pending"] is False


def test_an_expired_login_sets_copy_pending_and_a_later_copy_clears_it(
    tmp_path: Path, rclone: Rclone
) -> None:
    root = paper_store(tmp_path)
    rclone.returncode, rclone.stderr = 1, "oauth2: cannot fetch token: invalid_grant"
    out = copy(root)
    assert "rclone config reconnect gcs:" in out
    assert status(root)["copy_pending"] is True

    rclone.returncode, rclone.stderr = 0, ""
    copy(root)
    assert status(root)["copy_pending"] is False


def test_an_expired_login_does_not_fail_the_pass(
    tmp_path: Path, sbatch: Sbatch, rclone: Rclone
) -> None:
    root = synced(paper_store(tmp_path))
    rclone.returncode, rclone.stderr = 1, "invalid_grant"
    lines = serve(root, Clock(utc("2026-11-05 07:00")), until=utc("2026-11-05 07:10"))

    assert passes(lines) == ["2026-11-05T07:00Z daily pass 2026-11-05: success"]
    assert status(root)["copy_pending"] is True
    assert status(root)["last_daily_success"] == "2026-11-05"


def test_serve_submits_its_successor_from_the_slurm_job_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sbatch: Sbatch
) -> None:
    root = paper_store(tmp_path)
    monkeypatch.setenv("SLURM_JOB_ID", JOB)
    monkeypatch.setattr("labdata.cli._store_client", lambda: None)
    monkeypatch.setattr("labdata.cli._openalex_client", lambda: None)
    result = CliRunner().invoke(
        app,
        ["papers", "serve", "--root", str(root), "--successor", SCRIPT, "--until", "2000-01-01"],
    )

    assert result.exit_code == 0, result.output
    assert sbatch.calls == [["sbatch", "--parsable", f"--dependency=afterany:{JOB}", SCRIPT]]
    assert f"successor job 4243 submitted, to start after job {JOB}" in result.output
