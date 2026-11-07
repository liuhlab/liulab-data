"""``labdata store refresh``: the DAG, a fixture refresh end to end, its status record and copy.

The Snakemake seam is replaced by :func:`_executor`, which asks Snakemake's dry run for the
jobs left to do and runs each step's ``labdata store`` command in this process, so the fake
NCBI transport answers every fetch. The snapshot step "downloads" by copying fixtures.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

import labdata.ncbi.entrez as entrez_mod
from labdata.cli import app
from labdata.exceptions import NotFound
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import geo_lane, init_store, open_store, pipeline, raw, refresh
from labdata.store.fetch import soft_url
from tests.test_store_refresh_fetch import Ncbi, _Reply

_DATA = Path(__file__).parent / "data" / "geo_slice"
_OLD = "2026-10-14"
_SEQ = "Expression profiling by high throughput sequencing"
_COMPARE = Path(__file__).parents[1] / "scripts" / "store" / "compare_builds.py"
_EXPIRED = "Failed to create file system: couldn't fetch token: invalid_grant: maybe it has expired"
_DEST = "gcs:lab-bucket/store"

#: Held and unchanged, held and changed by GEO, held and withdrawn, released since.
KEPT, CHANGED, WITHDRAWN, NEW = "GSE153830", "GSE233685", "GSE233931", "GSE19456"
_GONE_GDS = "<eSummaryResult><ERROR>UID={uid}: cannot get document summary</ERROR></eSummaryResult>"


class _Geo(Ncbi):
    """NCBI with ``esummary`` on ``gds``: a withdrawn Series' summary is a record-level error."""

    gone: set[str]

    def _answer(self, url: str, params: dict[str, str]) -> _Reply:
        if url.endswith("esummary.fcgi"):
            uid = params["id"]
            if f"GSE{int(uid) - 200_000_000}" in self.gone:
                return _Reply(_GONE_GDS.format(uid=uid).encode())
            return _Reply(
                f"<eSummaryResult><DocSum><Id>{uid}</Id></DocSum></eSummaryResult>".encode()
            )
        return super()._answer(url, params)


def _ncbi() -> _Geo:
    """GEO and SRA as at the last build: the four Series' SOFT and every package."""
    ncbi = _Geo()
    ncbi.gone = set()
    for gse in (KEPT, CHANGED, WITHDRAWN, NEW):
        ncbi.serve_series(gse, (_DATA / "soft" / f"{gse}.soft").read_text(encoding="utf-8"))
    ncbi.packages = {p.stem: p.read_text(encoding="utf-8") for p in (_DATA / "packages").iterdir()}
    return ncbi


def _change(ncbi: _Geo) -> None:
    """GEO a month on: one Series revised, one withdrawn, one released."""
    text = ncbi.pages[soft_url(CHANGED)]
    ncbi.pages[soft_url(CHANGED)] = re.sub(
        r"^(!Series_title = .*)$", r"\1 (revised)", text, count=1, flags=re.M
    )
    ncbi.updated = [CHANGED]
    ncbi.gone = {WITHDRAWN}


def _geo_series(folder: Path, targets: set[str]) -> None:
    """Write a ``geo-series`` snapshot: the fixture's Series, ``targets`` as sequencing ones."""
    lines = gzip.decompress(next((_DATA / "snapshots" / "geo-series").rglob("*.gz")).read_bytes())
    folder.mkdir(parents=True, exist_ok=True)
    with gzip.open(folder / "gds_gse_esummary.jsonl.gz", "wt", encoding="utf-8") as fh:
        for line in lines.decode().splitlines():
            row = json.loads(line)
            if row["accession"] == WITHDRAWN and WITHDRAWN not in targets:
                continue
            if row["accession"] in targets:
                row["gdstype"] = _SEQ
            fh.write(json.dumps(row) + "\n")


def _renew(root: Path, date: str) -> None:
    """The snapshot step: the other sources unchanged, ``geo-series`` without the withdrawn."""
    for source in ("bioproject", "biosample", "sra-accessions"):
        old = _DATA / "snapshots" / source / _OLD
        shutil.copytree(old, root / "snapshots" / source / date, dirs_exist_ok=True)
    _geo_series(root / "snapshots" / "geo-series" / date, {KEPT, CHANGED, NEW})


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(geo_lane, "_sleep", lambda _s: None)


def _use(monkeypatch: pytest.MonkeyPatch, ncbi: Ncbi) -> None:
    def client() -> EntrezClient:
        creds = NcbiCredentials("t@t.org", "abc")
        return EntrezClient(creds, transport=ncbi, limiter=RateLimiter(1000.0, 8))

    monkeypatch.setattr("labdata.cli._store_client", client)


def _cli(*args: str) -> str:
    result = CliRunner().invoke(app, ["store", *args])
    assert result.exit_code == 0, result.output
    return result.output


def _built(root: Path, monkeypatch: pytest.MonkeyPatch, ncbi: Ncbi) -> Path:
    """A store as the full run left it: KEPT, CHANGED and WITHDRAWN held and built as b1."""
    init_store(root)
    for source in ("bioproject", "biosample", "sra-accessions"):
        shutil.copytree(_DATA / "snapshots" / source / _OLD, root / "snapshots" / source / _OLD)
    _geo_series(root / "snapshots" / "geo-series" / _OLD, {KEPT, CHANGED, WITHDRAWN})
    _use(monkeypatch, ncbi)
    where = ["--root", str(root)]
    for source in ("biosample", "bioproject", "sra-accessions", "geo-series"):
        _cli("extract", "--source", source, *where)
    _cli("_geo", *where)
    _cli("_eutils", *where)
    _cli("pack", *where)
    _cli("build", "--build-id", "b1", *where)
    return root


def _jobs(argv: list[str]) -> list[tuple[str, list[str], list[str], str]]:
    """Return the jobs Snakemake's dry run plans: rule, inputs, outputs and shell command."""
    done = subprocess.run(
        ["snakemake", *argv, "--dry-run"], capture_output=True, text=True, check=True
    )
    jobs = []
    for block in done.stdout.split("\nrule ")[1:]:
        rule = block.split(":", 1)[0]
        fields = dict(re.findall(r"^    (input|output): (.*)$", block, re.M))
        command = re.search(r"^Shell command: (.*)$", block, re.M)
        jobs.append(
            (
                rule,
                fields.get("input", "").split(", ") if fields.get("input") else [],
                fields.get("output", "").split(", ") if fields.get("output") else [],
                command[1] if command else "None",
            )
        )
    return jobs


def _executor(
    monkeypatch: pytest.MonkeyPatch, *, stop_after: str | None = None
) -> tuple[Callable[..., int], list[str]]:
    """A stand-in for the Snakemake seam that runs the planned steps here, in DAG order.

    It stops with exit 1 after the ``stop_after`` rule, as a killed job would.
    """
    ran: list[str] = []

    def run(argv: list[str], *, verbose: bool) -> int:
        work = Path(argv[argv.index("--directory") + 1])
        config = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())
        todo = _jobs(argv)
        cwd = Path.cwd()
        os.chdir(work)
        try:
            while todo:
                job = next(j for j in todo if all(Path(i).exists() for i in j[1]))
                todo.remove(job)
                rule, _, outputs, command = job
                if rule == "snapshots":
                    _renew(Path(config["root"]), config["date"])
                elif " -m labdata store " in command:
                    words = shlex.split(command.split(" -m labdata ", 1)[1].split(" 2>&1")[0])
                    result = CliRunner().invoke(app, words)
                    if result.exit_code:
                        sys.stderr.write(result.output)
                        return 1
                for out in outputs:
                    Path(out).touch()
                ran.append(rule)
                if rule == stop_after:
                    return 1
        finally:
            os.chdir(cwd)
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", run)
    return run, ran


def _refresh(root: Path, *extra: str) -> str:
    return _cli("refresh", "--root", str(root), "--cores", "1", *extra)


def _held(root: Path, source: str) -> dict[str, str]:
    return {row.accession: row.raw_text for row in raw.read_rows(root, source)}


def _current(root: Path) -> Path:
    return root / "builds" / str(open_store(root).settings["build"])


def _same_build(one: Path, two: Path) -> None:
    done = subprocess.run(
        [sys.executable, str(_COMPARE), str(one), str(two)], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stdout + done.stderr


# ---------------------------------------------------------------- the DAG


def test_refresh_hands_snakemake_the_refresh_dag_in_step_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SLURM_MEM_PER_NODE", "100000")
    root = tmp_path / "store"
    init_store(root)
    seen: dict[str, object] = {}

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen["argv"] = argv
        seen["jobs"] = _jobs(argv)
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    _refresh(root, "--since", "2026-10-14")

    jobs = seen["jobs"]
    assert isinstance(jobs, list)
    needs = {rule: set(inputs) for rule, inputs, _, _ in jobs if rule != "extract"}
    assert needs == {
        "snapshots": set(),
        "geo_series": set(),
        "targets": {"snapshots.done", "geo-series.done"},
        "changes": {"targets.done"},
        "geo": {"refetch.txt"},
        "eutils_beside_geo": {"refetch.txt"},
        "eutils_after_geo": {"geo.done", "eutils-beside-geo.done"},
        "confirm": {"eutils.done"},
        "pack": {"confirm.done"},
        "build": {"pack.done", *(f"extract-{s}.done" for s in _EXTRACTS)},
        "status": {"build.done"},
        "prune": {"status.done"},
        "copy": {"prune.done"},
        "all": {"copy.done"},
    }
    extracts = [j for j in jobs if j[0] == "extract"]
    assert sorted(o for j in extracts for o in j[2]) == sorted(
        f"extract-{s}.done" for s in _EXTRACTS
    )
    assert all(set(j[1]) == {"snapshots.done", "geo-series.done"} for j in extracts)
    commands = {rule: command for rule, _, _, command in jobs}
    assert f"--date {dt.datetime.now(dt.UTC):%Y-%m-%d}" in commands["snapshots"]
    assert "download_ncbi_bulk.py" in commands["snapshots"]
    # Only the snapshots a build reads: the SRA Parquet snapshot is never asked for, and the
    # geo-series patch runs beside the FTP downloads as its own step.
    ftp = sorted(set(_EXTRACTS) - {"geo-series"})
    assert commands["snapshots"].split("--date ", 1)[1].split()[1:4] == ftp
    assert "sra-metadata-parquet" not in commands["snapshots"]
    assert "download_ncbi_bulk.py" in commands["geo_series"]
    assert commands["geo_series"].split(" 2>&1")[0].endswith(" geo-series")
    assert "_changes --since 2026-10-14 --out refetch.txt" in commands["changes"]
    assert "_geo --refetch refetch.txt" in commands["geo"]
    assert "--soft-only" in commands["eutils_after_geo"]
    argv = seen["argv"]
    assert isinstance(argv, list)
    assert argv[argv.index("--resources") + 1] == "ncbi=2"
    total = int(argv[argv.index("--resources") + 2].removeprefix("mem_mb="))
    assert total <= 100000
    config = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())
    plan = config["mem_mb"]
    for group in (("geo", "eutils", "extract"), ("pack", "extract"), ("build",)):
        assert sum(plan[s] for s in group) <= total, group
    assert refresh.read_status(root)["outcome"] == "succeeded"


_EXTRACTS = ("biosample", "bioproject", "sra-accessions", "geo-series")


def test_a_resumed_refresh_says_it_keeps_its_own_since_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "store"
    init_store(root)
    started = "2026-11-01T02:00:00Z"
    folder = "work/refresh/2026-11-01T020000Z"
    unfinished = {
        "started": started,
        "outcome": "failed",
        "folder": folder,
        "since": _OLD,
        "dag": refresh.DAG,
    }
    refresh.write_status(root, unfinished)
    seen: list[str] = []

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen.append(json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())["since"])
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    with caplog.at_level("WARNING"):
        _refresh(root, "--since", "2026-01-01")

    assert seen == [_OLD]
    assert f"--since 2026-01-01 is not used: the unfinished refresh keeps {_OLD}" in caplog.text


def test_a_refresh_an_older_dag_opened_is_left_and_a_new_one_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "store"
    init_store(root)
    folder = "work/refresh/2026-11-01T020000Z"
    (root / folder).mkdir(parents=True)
    for marker in ("snapshots", "targets", "eutils", "confirm", "reach", "pack"):
        (root / folder / f"{marker}.done").touch()
    old = {"started": "2026-11-01T02:00:00Z", "outcome": "failed", "folder": folder, "since": _OLD}
    refresh.write_status(root, old)
    seen: list[str] = []

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen.append(argv[argv.index("--directory") + 1])
        return 1

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    again = ["store", "refresh", "--root", str(root), "--cores", "1"]
    with caplog.at_level("WARNING"):
        assert CliRunner().invoke(app, again).exit_code == 1

    assert Path(seen[0]) != root / folder
    assert not list(Path(seen[0]).glob("*.done"))
    status = refresh.read_status(root)
    assert status["since"] == _OLD
    assert status["folder"] != folder
    assert f"left unfinished: {folder}" in caplog.text
    # The new refresh, cut off in turn, resumes in its own folder.
    assert CliRunner().invoke(app, again).exit_code == 1
    assert seen[1] == seen[0]


# ---------------------------------------------------------------- a fixture refresh


@pytest.fixture
def built(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, _Geo]:
    ncbi = _ncbi()
    return _built(tmp_path / "store", monkeypatch, ncbi), ncbi


def test_a_refresh_adds_replaces_removes_and_switches_while_an_open_store_keeps_its_build(
    built: tuple[Path, _Geo], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, ncbi = built
    before = open_store(root)
    assert "(revised)" not in before.raw([CHANGED])[CHANGED].text
    _change(ncbi)
    _, ran = _executor(monkeypatch)

    said = _refresh(root)

    assert "every step done" in said
    assert ran[-1] == "all"
    soft = _held(root, raw.GEO_SOFT)
    assert set(soft) == {KEPT, CHANGED, NEW}
    assert "(revised)" in soft[CHANGED]
    after = open_store(root)
    assert after.settings["build"] != "b1"
    assert "(revised)" in after.raw([CHANGED])[CHANGED].text
    after.open(NEW)
    with pytest.raises(NotFound):
        after.open(WITHDRAWN)
    # opened before the switch, it still reads b1
    before.open(WITHDRAWN)
    assert "(revised)" not in before.raw([CHANGED])[CHANGED].text
    status = refresh.read_status(root)
    today = dt.datetime.now(dt.UTC).date().isoformat()
    assert status["outcome"] == "succeeded"
    assert status["last_success"] == today
    assert status["since"] == _OLD
    assert status["ended"] >= status["started"]
    assert status["counts"][raw.GEO_SOFT] == {"added": 1, "replaced": 1, "removed": 1}
    assert status["copy_pending"] is False


def test_a_refresh_killed_after_pack_and_rerun_gives_the_uninterrupted_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    whole_ncbi, cut_ncbi = _ncbi(), _ncbi()
    whole = _built(tmp_path / "whole", monkeypatch, whole_ncbi)
    cut = _built(tmp_path / "cut", monkeypatch, cut_ncbi)
    for ncbi in (whole_ncbi, cut_ncbi):
        _change(ncbi)
    _use(monkeypatch, whole_ncbi)
    _executor(monkeypatch)
    _refresh(whole)

    _use(monkeypatch, cut_ncbi)
    _executor(monkeypatch, stop_after="pack")
    result = CliRunner().invoke(app, ["store", "refresh", "--root", str(cut), "--cores", "1"])
    assert result.exit_code == 1
    assert refresh.read_status(cut)["outcome"] == "failed"
    assert open_store(cut).settings["build"] == "b1"
    cut_ncbi.fetched.clear()
    cut_ncbi.terms.clear()
    _, ran = _executor(monkeypatch)
    _refresh(cut)

    assert ran == ["build", "status", "prune", "copy", "all"]
    assert cut_ncbi.fetched == []
    assert cut_ncbi.terms == []
    for source in (raw.GEO_SOFT, raw.SRA_XML, raw.GEO_FILELIST):
        assert _held(cut, source) == _held(whole, source), source
    _same_build(_current(whole), _current(cut))
    status = refresh.read_status(cut)
    assert status["outcome"] == "succeeded"
    assert status["counts"] == refresh.read_status(whole)["counts"]


def test_a_second_refresh_asks_only_for_the_overlap_window(
    built: tuple[Path, _Geo], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root, ncbi = built
    _change(ncbi)
    _executor(monkeypatch)
    _refresh(root)
    first = _current(root)
    ncbi.updated = []
    ncbi.fetched.clear()
    ncbi.terms.clear()

    _refresh(root)

    since = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=3)
    assert ncbi.terms == [f"gse[ETYP] AND {since:%Y/%m/%d}:3000[UDAT]"]
    assert [call for call in ncbi.fetched if "geo/series" in call] == []
    assert _current(root) != first
    _same_build(first, _current(root))


def test_an_expired_login_leaves_the_copy_pending_and_a_later_copy_clears_it(
    built: tuple[Path, _Geo], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, ncbi = built
    marker = root / "labdata-store.toml"
    marker.write_text(marker.read_text() + f'copy_to = "{_DEST}"\n', encoding="utf-8")
    calls: list[list[str]] = []

    def expired(args: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "", _EXPIRED)

    monkeypatch.setattr("labdata.store.gcs._run_rclone", expired)
    _change(ncbi)
    _executor(monkeypatch)

    _refresh(root)

    status = refresh.read_status(root)
    assert status["outcome"] == "succeeded"
    assert status["copy_pending"] is True
    assert calls == [["rclone", "lsjson", _DEST, "--max-depth", "1"]]

    monkeypatch.setattr(
        "labdata.store.gcs._run_rclone",
        lambda args: subprocess.CompletedProcess(args, 0, "", ""),
    )
    assert "Copied the store" in _cli("copy", "--root", str(root))
    assert refresh.read_status(root)["copy_pending"] is False


def test_an_accession_only_a_pending_removal_lists_is_fetched_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ncbi = _ncbi()
    root = tmp_path / "store"
    init_store(root)
    _geo_series(root / "snapshots" / "geo-series" / _OLD, {KEPT})
    _use(monkeypatch, ncbi)
    _cli("_geo", KEPT, "--root", str(root))
    _cli("pack", "--root", str(root))
    raw.write_removals(root, raw.GEO_SOFT, [KEPT])
    ncbi.fetched.clear()

    _cli("_geo", KEPT, "--root", str(root))
    _cli("pack", "--root", str(root))

    assert soft_url(KEPT) in ncbi.fetched
    assert set(_held(root, raw.GEO_SOFT)) == {KEPT}
