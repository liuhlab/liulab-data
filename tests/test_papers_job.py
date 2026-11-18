"""``labdata papers serve`` and ``labdata papers copy``: the paper job and its GCS copy."""

from __future__ import annotations

import datetime as dt
import json
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata import _slurm
from labdata._http import RateLimiter
from labdata.cli import app
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import job as job_mod
from labdata.papers import open_paper_store, tables
from labdata.papers.openalex import Account, OpenAlex, accounts_path, load_accounts, save_accounts
from tests._fake_gcs import FakeGcs
from tests._paper_fakes import Article, FakeWeb, fixture_build, init_papers, work

PMID = "41000001"
DEST = "gcs:bucket/datalake/paper-store"
JOB = "4242"
SCRIPT = "/repo/scripts/papers/serve.sbatch"
RUN = "/data/paper-run"
OUTPUT = f"--output={RUN}/labdata-papers.%j.log"


class Clock:
    """A UTC clock that moves only when the job sleeps."""

    def __init__(self, start: dt.datetime) -> None:
        self.now = start

    def __call__(self) -> dt.datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += dt.timedelta(seconds=seconds)


class Sbatch:
    """Stand-in for the successor submission: records each command, answers job IDs from ``first``."""

    def __init__(self, returncode: int = 0, stderr: str = "", first: int = 4243) -> None:
        self.calls: list[list[str]] = []
        self.returncode, self.stderr, self.next = returncode, stderr, first

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        self.next += 1
        return subprocess.CompletedProcess(args, self.returncode, f"{self.next - 1}\n", self.stderr)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    monkeypatch.delenv("OPENALEX_API_KEYS", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(_slurm, "show_job", lambda _id: show_job(f"{RUN}/labdata-papers.{JOB}.log"))


def show_job(stdout: str) -> str:
    """The part of ``scontrol show job`` output that names the job's log."""
    return f"JobId={JOB} JobName=labdata-papers\n   WorkDir=/repo\n   StdOut={stdout}\n"


@pytest.fixture
def sbatch(monkeypatch: pytest.MonkeyPatch) -> Sbatch:
    fake = Sbatch()
    monkeypatch.setattr(job_mod, "_submit", fake)
    return fake


@pytest.fixture
def gcs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeGcs:
    return FakeGcs.install(monkeypatch, tmp_path / "gcs", DEST)


def paper_store(tmp_path: Path) -> Path:
    store = fixture_build(tmp_path / "store", [("GSE1", f"PMID:{PMID}", "SOFT")])
    root = tmp_path / "papers"
    init_papers(root, store, copy_to=DEST)
    return root


def holding(root: Path) -> Path:
    """Give the store one paper file and its ``files`` row: a copy refuses an empty table."""
    (root / "papers" / "1").mkdir(parents=True)
    (root / "papers" / "1" / "1.pdf").write_bytes(b"%PDF")
    row = {"paper": "1", "kind": "pdf", "path": "papers/1/1.pdf", "size": 4, "md5": "a"}
    tables.write(root, tables.FILES, [row])
    return root


def synced(root: Path, build: str | None = None) -> Path:
    """Record this month's PMC sync, and the build it read, so only the daily pass is due."""
    record = {"last_pmc_sync_success": "2026-11-03", "last_pmc_sync_build": build}
    (root / "paper-status.json").write_text(json.dumps(record))
    return root


def refresh(store: Path, build: str = "b2") -> None:
    """Make a copy of build ``b1`` the GEO/SRA store's current build, as a refresh does."""
    shutil.copytree(store / "builds" / "b1", store / "builds" / build)
    (store / "labdata-store.toml").write_text(f'format_version = 1\nbuild = "{build}"\n')


def serve(
    root: Path, clock: Clock, until: dt.datetime, log: Callable[[str], None] | None = None
) -> list[str]:
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
        openalex=lambda: OpenAlex(load_accounts(), transport=web, limiter=RateLimiter(1000.0, 8)),
        transport=web,
        clock=clock,
        sleep=clock.sleep,
        until=until,
        job_id=JOB,
        successor=SCRIPT,
        log=log or lines.append,
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
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    root = holding(paper_store(tmp_path))
    full_copy(root)
    gcs.forget()
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
    # These passes change no paper file: their copies send only the top of the store.
    assert not any("/" in path for _, path in gcs.log)
    assert sbatch.calls == [
        ["sbatch", "--parsable", f"--dependency=afterany:{JOB}", OUTPUT, SCRIPT],
        ["sbatch", "--parsable", f"--dependency=afterany:{JOB}:4243", OUTPUT, SCRIPT],
    ]
    # Idle, the job prints a line a minute.
    idle = [line for line in lines if line.startswith("2026-11-02T01:") and " idle" in line]
    assert len(idle) == 60


def test_after_a_restart_only_what_is_due_runs_and_no_more_successors_are_submitted(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
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
    assert len(sbatch.calls) == 2


def test_a_new_build_makes_the_pmc_sync_due_within_the_month(tmp_path: Path) -> None:
    store = open_paper_store(synced(paper_store(tmp_path), build="b1"))
    mid_month = utc("2026-11-20 07:00")
    assert job_mod.due(store, mid_month) == ["daily"]

    refresh(tmp_path / "store")
    assert job_mod.due(store, mid_month) == ["daily", "pmc-sync"]

    # A sync that recorded no build: only the date rule applies.
    synced(store.root)
    assert job_mod.due(store, mid_month) == ["daily"]


@pytest.mark.parametrize(
    "breaks",
    [
        lambda store: (store / "labdata-store.toml").unlink(),
        lambda store: (store / "labdata-store.toml").write_text("not toml ["),
        lambda store: shutil.rmtree(store),
    ],
    ids=["no marker", "unreadable marker", "no store folder"],
)
def test_a_store_that_will_not_open_does_not_end_the_job(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs, breaks: Callable[[Path], object]
) -> None:
    root = synced(paper_store(tmp_path), build="b1")
    breaks(tmp_path / "store")
    lines = serve(root, Clock(utc("2026-11-05 07:00")), until=utc("2026-11-05 08:01"))

    # The daily pass, which reads the store's links, fails and is tried again an hour later.
    assert [line for line in lines if "daily pass 2026-11-05: failed" in line] == [
        "2026-11-05T07:00Z daily pass 2026-11-05: failed; tried again after 1:00:00",
        "2026-11-05T08:00Z daily pass 2026-11-05: failed; tried again after 1:00:00",
    ]
    # One line per check: before each of the two passes, and each idle minute.
    unreadable = [line for line in lines if "GEO/SRA store not readable" in line]
    assert len(unreadable) == len([line for line in lines if " idle;" in line]) + 2
    # On the 3rd the date rule still makes the PMC sync due.
    assert job_mod.due(open_paper_store(root), utc("2026-12-03 07:00")) == ["daily", "pmc-sync"]


def test_a_refresh_on_the_1st_starts_the_months_pmc_sync_and_none_follows_on_the_3rd(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    root = synced(paper_store(tmp_path), build="b1")
    refresh(tmp_path / "store")
    lines = serve(root, Clock(utc("2026-12-01 07:00")), until=utc("2026-12-03 01:00"))

    syncs = [line for line in passes(lines) if "pmc-sync" in line]
    assert syncs == ["2026-12-01T07:00Z pmc-sync pass 2026-12-01: success"]
    assert status(root)["last_pmc_sync_build"] == "b2"


def test_the_pmc_sync_logs_and_records_the_papers_it_moved_and_turned_away(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    unknown = ("GSE2", "DOI:10.1000/unknown", "SOFT Series_citation")
    store = fixture_build(tmp_path / "store", [("GSE1", f"PMID:{PMID}", "SOFT"), unknown])
    root = tmp_path / "papers"
    init_papers(root, store, copy_to=DEST)
    lines = serve(root, Clock(utc("2026-11-03 07:00")), until=utc("2026-11-03 07:10"))

    progress = [line for line in lines if line.startswith("looked up ")]
    assert progress[-1].endswith("0 failed, 0 re-keyed, 1 DOIs turned away")
    summary = [line for line in lines if line.startswith("2026-11-03T07:00Z pmc-sync pass")]
    assert summary[0].endswith("0 failed, 0 re-keyed, 1 DOIs turned away")
    assert summary[1].endswith(": success")
    last = status(root)["last_pass"]
    assert (last["kind"], last["rekeyed"], last["turned_away"]) == ("pmc-sync", 0, 1)


def test_a_failed_successor_submission_is_logged_and_the_job_goes_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, gcs: FakeGcs
) -> None:
    root = synced(paper_store(tmp_path))
    monkeypatch.setattr(job_mod, "_submit", Sbatch(1, "sbatch: error: Access denied"))
    lines = serve(root, Clock(utc("2026-11-05 07:00")), until=utc("2026-11-05 07:10"))

    assert any("successor not submitted" in line and "Access denied" in line for line in lines)
    assert passes(lines) == ["2026-11-05T07:00Z daily pass 2026-11-05: success"]


@pytest.mark.parametrize(
    ("shown", "output"),
    [
        (show_job("/repo with space/labdata-papers.4242.log"), "--output=/repo with space/"),
        ("", None),
    ],
    ids=["log folder", "no scontrol answer"],
)
def test_the_successor_logs_into_the_folder_of_the_job_before_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sbatch: Sbatch,
    gcs: FakeGcs,
    shown: str,
    output: str | None,
) -> None:
    root = synced(paper_store(tmp_path))
    monkeypatch.setattr(_slurm, "show_job", lambda _id: shown)
    serve(root, Clock(utc("2026-11-05 07:00")), until=utc("2026-11-05 07:01"))

    logs = [[a for a in args if a.startswith("--output=")] for args in sbatch.calls]
    assert logs == [[f"{output}labdata-papers.%j.log"] if output else []] * 2


def test_a_failed_log_write_does_not_stop_the_job(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    root = synced(paper_store(tmp_path))
    clock = Clock(utc("2026-11-05 00:00"))
    lines: list[str] = []

    def log(line: str) -> None:
        if clock.now < utc("2026-11-05 00:20"):
            raise OSError(28, "No space left on device")
        lines.append(line)

    serve(root, clock, until=utc("2026-11-05 01:00"), log=log)

    assert passes(lines) == ["2026-11-05T00:30Z daily pass 2026-11-05: success"]
    assert status(root)["last_daily_success"] == "2026-11-05"


def dependency_of(args: list[str]) -> str:
    return next(a for a in args if a.startswith("--dependency=")).removeprefix(
        "--dependency=afterany:"
    )


@pytest.mark.parametrize(
    ("record", "shown", "after", "chain"),
    [
        # Job 4241 ran; 4243, queued behind 4242, is still waiting.
        ("4241 4242 4243", {}, ["4242:4243"], "4242 4243 4300"),
        # Job 4240 ran, 4241 failed at once, so this job, the next one queued, took over.
        ("4240 4241 4242", {"4241": "FAILED"}, ["4242", "4242:4300"], "4242 4300 4301"),
        # The job queued behind this one was lost.
        ("4241 4242 4243", {"4243": "CANCELLED"}, ["4242", "4242:4300"], "4242 4300 4301"),
        # The same job, requeued, with both its successors still waiting.
        ("4242 4243 4244", {}, [], "4242 4243 4244"),
        # Requeued after its first successor was lost: the second one keeps its place.
        ("4242 4243 4244", {"4243": "CANCELLED"}, ["4242:4244"], "4242 4244 4300"),
    ],
    ids=["one queued", "successor failed", "successor lost", "requeued", "requeued, one lost"],
)
def test_two_jobs_stay_queued_behind_the_running_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gcs: FakeGcs,
    record: str,
    shown: dict[str, str],
    after: list[str],
    chain: str,
) -> None:
    root = synced(paper_store(tmp_path))
    (root / job_mod.SUCCESSOR).write_text(record + "\n")
    sbatch = Sbatch(first=4300)
    monkeypatch.setattr(job_mod, "_submit", sbatch)
    monkeypatch.setattr(
        _slurm,
        "show_job",
        lambda id_: (
            show_job(f"{RUN}/labdata-papers.{id_}.log")
            + f"   JobState={shown.get(id_, 'PENDING')} Reason=Dependency\n"
        ),
    )
    serve(root, Clock(utc("2026-11-05 07:00")), until=utc("2026-11-05 07:01"))

    assert [dependency_of(args) for args in sbatch.calls] == after
    assert (root / job_mod.SUCCESSOR).read_text().split() == chain.split()


class EditingClock(Clock):
    """A clock that runs ``edit`` once, the first time it sleeps past ``at``."""

    def __init__(self, start: dt.datetime, at: dt.datetime, edit: Callable[[], object]) -> None:
        super().__init__(start)
        self.at, self.edit = at, edit

    def sleep(self, seconds: float) -> None:
        super().sleep(seconds)
        if self.now >= self.at and self.edit:
            self.edit()
            self.edit = None


def test_each_daily_pass_spends_the_accounts_saved_before_it(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    root = synced(paper_store(tmp_path))
    save_accounts([Account("k1", "a@lab.org")])
    clock = EditingClock(
        utc("2026-11-05 07:00"),
        at=utc("2026-11-05 12:00"),
        edit=lambda: save_accounts([Account("k1", "a@lab.org"), Account("k2", "b@lab.org")]),
    )
    lines = serve(root, clock, until=utc("2026-11-06 02:00"))

    spending = [line for line in lines if "accounts with quota left" in line]
    assert [line.split(", ")[1] for line in spending] == [
        "1 accounts with quota left",
        "2 accounts with quota left",
    ]
    assert status(root)["quota"]["by_key"] == {"a@lab.org": 0, "b@lab.org": 0}


def test_an_unreadable_accounts_file_fails_that_pass_and_the_job_goes_on(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    root = synced(paper_store(tmp_path))
    accounts_path().parent.mkdir(parents=True, exist_ok=True)
    accounts_path().write_text("not toml [")
    clock = EditingClock(
        utc("2026-11-05 07:00"),
        at=utc("2026-11-05 07:30"),
        edit=lambda: save_accounts([Account("k1", "a@lab.org")]),
    )
    lines = serve(root, clock, until=utc("2026-11-05 09:00"))

    assert any("cannot read OpenAlex accounts" in line for line in lines)
    assert passes(lines) == ["2026-11-05T08:00Z daily pass 2026-11-05: success"]


def full_copy(root: Path) -> str:
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])
    assert result.exit_code == 0, result.output
    return result.output


def test_a_failed_copy_sets_copy_pending(tmp_path: Path, gcs: FakeGcs) -> None:
    root = holding(paper_store(tmp_path))
    gcs.error = "ERROR : bucket not found\n"
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])
    assert result.exit_code == 1
    assert "bucket not found" in result.output
    assert status(root)["copy_pending"] is True


def test_an_expired_login_sets_copy_pending_and_a_later_copy_clears_it(
    tmp_path: Path, gcs: FakeGcs
) -> None:
    root = holding(paper_store(tmp_path))
    gcs.expired = True
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])
    assert result.exit_code == 1
    assert "rclone config reconnect gcs:" in result.output
    assert status(root)["copy_pending"] is True

    gcs.expired = False
    full_copy(root)
    assert status(root)["copy_pending"] is False


def test_an_expired_login_leaves_the_copy_pending_and_an_idle_tick_tries_it_again_hourly(
    tmp_path: Path, sbatch: Sbatch, gcs: FakeGcs
) -> None:
    root = holding(synced(paper_store(tmp_path)))
    full_copy(root)
    # A pass stopped before its copy: the store holds a paper file GCS lacks.
    (root / "papers" / "2").mkdir()
    (root / "papers" / "2" / "2.pdf").write_bytes(b"%PDF 2")
    row = {"paper": "2", "kind": "pdf", "path": "papers/2/2.pdf", "size": 6, "md5": "b"}
    tables.write(root, tables.FILES, [*tables.read(root, tables.FILES), row])
    gcs.expired = True
    back = utc("2026-11-05 07:03")
    clock = EditingClock(
        utc("2026-11-05 07:00"), at=back, edit=lambda: gcs.__setattr__("expired", False)
    )
    lines = serve(root, clock, until=utc("2026-11-05 08:10"))

    # An expired login does not fail the pass.
    assert passes(lines) == ["2026-11-05T07:00Z daily pass 2026-11-05: success"]
    assert status(root)["last_daily_success"] == "2026-11-05"
    # The pass's copy, then one an hour later, though the login was back at 07:03.
    assert len([line for line in lines if "rclone config reconnect gcs:" in line]) == 1
    assert len([line for line in lines if line.startswith("copied the paper store")]) == 1
    idle = [line.endswith("copy pending: yes") for line in lines if " idle;" in line]
    assert idle == [True] * 60 + [False] * 10
    assert gcs.read("papers/2/2.pdf") == b"%PDF 2"
    assert (status(root)["finalized"], status(root)["copy_pending"]) == (True, False)


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
    assert sbatch.calls == [
        ["sbatch", "--parsable", f"--dependency=afterany:{JOB}", OUTPUT, SCRIPT],
        ["sbatch", "--parsable", f"--dependency=afterany:{JOB}:4243", OUTPUT, SCRIPT],
    ]
    assert f"successor job 4244 submitted, to start after job {JOB} and 4243" in result.output
