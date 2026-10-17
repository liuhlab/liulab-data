"""One full run: the round's sample, its hidden steps and the DAG ``labdata store run`` drives."""

from __future__ import annotations

import gzip
import json
import re
import shutil
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata import cli
from labdata.cli import app
from labdata.store import pipeline, targets
from labdata.store.fetch import soft_url
from tests.test_store_geo_lane import Site, client, soft

_DATA = Path(__file__).parent / "data" / "geo_slice"
_DATE = "2026-10-14"
_SEQ = "Expression profiling by high throughput sequencing"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _snapshot(root: Path, n: int) -> None:
    path = root / "snapshots" / "geo-series" / _DATE / "gds_gse_esummary.jsonl.gz"
    path.parent.mkdir(parents=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for i in range(1, n + 1):
            fh.write(json.dumps({"accession": f"GSE{i}", "gdstype": _SEQ, "bioproject": ""}))
            fh.write("\n")


def _accessions(path: Path) -> list[str]:
    return pq.read_table(path, columns=["accession"]).column(0).to_pylist()


def test_round_samples_are_seeded_and_nested(tmp_path: Path) -> None:
    _snapshot(tmp_path, 5000)
    one = _accessions(targets.target_list(tmp_path, fraction=0.01))
    ten = _accessions(targets.target_list(tmp_path, fraction=0.1))
    every = _accessions(targets.target_list(tmp_path))
    assert set(one) < set(ten) < set(every)
    assert 25 <= len(one) <= 75
    assert 400 <= len(ten) <= 600
    assert len(every) == 5000
    other = tmp_path / "other"
    _snapshot(other, 5000)
    assert _accessions(targets.target_list(other, fraction=0.01)) == one


def _soft_paths(calls: list[str]) -> set[str]:
    return {path.rsplit("/", 1)[-1].split("_")[0] for path in calls if path.endswith(".soft.gz")}


def test_a_larger_round_fetches_none_of_a_smaller_rounds_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "store"
    _snapshot(root, 400)
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    site = Site({soft_url(f"GSE{i}"): [gzip.compress(soft(f"GSE{i}"))] for i in range(1, 401)})
    monkeypatch.setattr(cli, "_store_client", lambda: client(site))
    runner = CliRunner()

    def step(*args: str) -> str:
        result = runner.invoke(app, ["store", *args, "--root", str(root)])
        assert result.exit_code == 0, result.output
        return result.output

    rounds = {}
    for fraction in ("0.1", "0.3"):
        assert "target Series" in step("_targets", "--fraction", fraction)
        assert "target Series" in step("_targets", "--fraction", fraction)  # idempotent
        site.calls.clear()
        step("_geo", "--series", str(targets.list_path(root, _DATE, float(fraction))))
        rounds[fraction] = _soft_paths(site.calls)
    assert rounds["0.1"]
    assert rounds["0.3"]
    assert not rounds["0.1"] & rounds["0.3"]
    site.calls.clear()
    step("_geo", "--series", str(targets.list_path(root, _DATE, 0.3)))
    assert site.calls == []


@pytest.fixture
def store(tmp_path: Path) -> Path:
    """A store with the slice's snapshots and no raw files."""
    root = tmp_path / "store"
    shutil.copytree(_DATA / "snapshots", root / "snapshots")
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return root


def _dry_run(argv: list[str]) -> dict[str, int]:
    """Return the jobs per rule Snakemake plans for ``argv``, without running one."""
    done = subprocess.run(
        ["snakemake", *argv, "--dry-run", "--quiet", "rules"],
        capture_output=True,
        text=True,
        check=True,
    )
    jobs = {}
    for line in done.stdout.splitlines():
        cells = line.split()
        if len(cells) == 2 and cells[1].isdigit() and cells[0] != "total":
            jobs[cells[0]] = int(cells[1])
    return jobs


def test_run_hands_snakemake_the_rounds_dag_and_config(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, object] = {}

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen["argv"] = argv
        seen["config"] = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())
        seen["jobs"] = _dry_run(argv)
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = CliRunner().invoke(
        app, ["store", "run", "--root", str(store), "--fraction", "0.01", "--cores", "8"]
    )
    assert result.exit_code == 0, result.output
    work = store / "work" / "run" / f"{_DATE}-f0.01"
    argv = seen["argv"]
    assert isinstance(argv, list)
    assert argv[argv.index("--directory") + 1] == str(work)
    assert argv[argv.index("--cores") + 1] == "8"
    assert argv[argv.index("--resources") + 1] == "ncbi=2"
    config = seen["config"]
    assert isinstance(config, dict)
    assert config["fraction"] == "0.01"
    assert config["copy"] is False
    assert config["snapshots"] == dict.fromkeys(
        ["biosample", "bioproject", "sra-accessions", "geo-series"], _DATE
    )
    assert config["targets"] == str(
        store / "work" / "targets" / f"geo-series-{_DATE}-f0.01.parquet"
    )
    assert json.loads((work / "run.json").read_text())["snapshots"] == config["snapshots"]
    assert seen["jobs"] == {
        "all": 1,
        "targets": 1,
        "geo": 1,
        "eutils_beside_geo": 1,
        "eutils_after_geo": 1,
        "pack": 1,
        "extract": 4,
        "build": 1,
        "coverage": 1,
        "prune": 1,
        "copy": 1,
    }


#: Steps the DAG can run at the same time: a snapshot extract has no inputs, so it may run
#: beside any step before the build.
_TOGETHER = [
    ("targets", "extract"),
    ("geo", "eutils_beside_geo", "extract"),
    ("eutils_after_geo", "extract"),
    ("pack", "extract"),
]


def test_steps_that_can_run_together_fit_in_the_jobs_memory(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SLURM_MEM_PER_NODE", "100000")
    planned: dict[str, int] = {}
    seen: dict[str, str] = {}

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen["resources"] = argv[argv.index("--resources") + 2]
        done = subprocess.run(
            ["snakemake", *argv, "--dry-run"], capture_output=True, text=True, check=True
        )
        for block in done.stdout.split("\nrule ")[1:]:
            rule = block.split(":", 1)[0]
            if rule == "all":
                continue
            mem = int(re.search(r"\bmem_mb=(\d+)", block)[1])  # type: ignore[index]
            planned[rule] = max(planned.get(rule, 0), mem)
            if "-m labdata" in block:
                assert f"LABDATA_MEMORY_MB={mem} " in block  # the step is told its share
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = CliRunner().invoke(app, ["store", "run", "--root", str(store), "--cores", "8"])
    assert result.exit_code == 0, result.output
    total = int(seen["resources"].removeprefix("mem_mb="))
    assert total <= 100000
    assert set(planned) >= {"build", "coverage", *(s for group in _TOGETHER for s in group)}
    assert max(planned.values()) <= total
    for group in _TOGETHER:
        assert sum(planned[s] for s in group) <= total, group


def test_run_exits_with_snakemakes_code_when_a_step_fails(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pipeline, "_run_snakemake", lambda argv, *, verbose: 1)
    result = CliRunner().invoke(app, ["store", "run", "--root", str(store)])
    assert result.exit_code == 1
    assert (store / "work" / "run" / f"{_DATE}-all" / "config.json").is_file()


def test_run_refuses_a_fraction_outside_zero_to_one(store: Path) -> None:
    result = CliRunner().invoke(app, ["store", "run", "--root", str(store), "--fraction", "2"])
    assert result.exit_code == 2
    assert "fraction" in result.output


def test_a_lane_run_that_saves_every_item_clears_its_failed_file(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site = Site({soft_url("GSE1"): [TimeoutError("timed out")]})
    monkeypatch.setattr(cli, "_store_client", lambda: client(site))
    monkeypatch.setattr("labdata.store.geo_lane._sleep", lambda _s: None)
    work = store / "work" / "run" / f"{_DATE}-all"
    work.mkdir(parents=True)
    marker = work / "geo.failed"
    args = ["store", "_geo", "GSE1", "--failed-marker", str(marker), "--root", str(store)]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert marker.is_file()
    for name in ("geo", "eutils-beside-geo", "eutils"):
        (work / f"{name}.done").touch()
    site.pages[soft_url("GSE1")] = [gzip.compress(soft("GSE1"))]
    assert CliRunner().invoke(app, args).exit_code == 0
    assert not marker.exists()


_AFTER_EUTILS = {"eutils_after_geo": 1, "pack": 1, "build": 1, "coverage": 1, "prune": 1, "copy": 1}


@pytest.mark.parametrize(
    ("failed", "jobs"),
    [
        ("geo", {"all": 1, "geo": 1, **_AFTER_EUTILS}),
        ("eutils", {"all": 1, **_AFTER_EUTILS}),
        (None, {}),
    ],
)
def test_a_lane_that_left_items_failed_reruns_with_every_step_after_it(
    store: Path, monkeypatch: pytest.MonkeyPatch, failed: str | None, jobs: dict[str, int]
) -> None:
    work = store / "work" / "run" / f"{_DATE}-all"
    work.mkdir(parents=True)
    target_list = targets.list_path(store, _DATE, None)
    target_list.parent.mkdir(parents=True)
    target_list.touch()
    extracts = [f"extract-{s}" for s in ("biosample", "bioproject", "sra-accessions", "geo-series")]
    steps = ["geo", "eutils-beside-geo", "eutils", "pack", "build", "coverage", "prune", "copy"]
    for marker in [*extracts, *steps]:
        (work / f"{marker}.done").touch()
    if failed:
        (work / f"{failed}.failed").touch()
    seen: dict[str, dict[str, int]] = {}

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen["jobs"] = _dry_run(argv)
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = CliRunner().invoke(app, ["store", "run", "--root", str(store)])
    assert result.exit_code == 0, result.output
    assert seen["jobs"] == jobs


def test_prune_and_copy_wait_for_coverage_to_pass(
    store: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, dict[str, int]] = {}

    def fake(argv: list[str], *, verbose: bool) -> int:
        work = Path(argv[argv.index("--directory") + 1])
        target_list = Path(json.loads((work / "config.json").read_text())["targets"])
        target_list.parent.mkdir(parents=True, exist_ok=True)
        target_list.touch()
        extracts = [f"extract-{s}" for s in ("biosample", "bioproject", "sra-accessions")]
        for marker in [*extracts, "extract-geo-series", "geo", "eutils-beside-geo", "eutils"]:
            (work / f"{marker}.done").touch()
        (work / "pack.done").touch()
        (work / "build.done").touch()
        seen["jobs"] = _dry_run([*argv, "copy.done"])
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = CliRunner().invoke(app, ["store", "run", "--root", str(store)])
    assert result.exit_code == 0, result.output
    assert seen["jobs"] == {"coverage": 1, "prune": 1, "copy": 1}
