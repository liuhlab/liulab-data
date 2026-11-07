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

import pyarrow.parquet as pq
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
            return _Reply(f"<eSummaryResult><DocSum><Id>{uid}</Id></DocSum></eSummaryResult>".encode())
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
        shutil.copytree(
            _DATA / "snapshots" / source / _OLD, root / "snapshots" / source / _OLD
        )
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


def _same_build(one: Path, two: Path) -> None:
    builds = [r / "builds" / str(open_store(r).settings["build"]) for r in (one, two)]
    done = subprocess.run(
        [sys.executable, str(_COMPARE), *map(str, builds)], capture_output=True, text=True
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
        "targets": {"snapshots.done"},
        "changes": {"targets.done"},
        "geo": {"refetch.txt"},
        "eutils_beside_geo": {"refetch.txt"},
        "eutils_after_geo": {"geo.done", "eutils-beside-geo.done"},
        "confirm": {"eutils.done"},
        "reach": {"confirm.done"},
        "pack": {"reach.done"},
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
    assert all(j[1] == ["snapshots.done"] for j in extracts)
    commands = {rule: command for rule, _, _, command in jobs}
    assert f"--date {dt.datetime.now(dt.UTC):%Y-%m-%d}" in commands["snapshots"]
    assert "download_ncbi_bulk.py" in commands["snapshots"]
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
