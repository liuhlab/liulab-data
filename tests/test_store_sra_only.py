"""One SRA-only record, PRJNA1027859, fetched, built and read back from the store.

Packages come from ``tests/data/sra_package/``. The snapshots under
``tests/data/prjna1027859/snapshots/`` are written by ``make_fixtures.py`` beside them.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import (
    AccessionError,
    EntrezError,
    LabdataError,
    NotFound,
    NotMirrored,
)
from labdata.geo import Experiment
from labdata.store import Store, open_store
from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import ExplodingEntrezClient, FakeEntrezClient

_DATA = Path(__file__).parent / "data"
_SNAPSHOTS = _DATA / "prjna1027859" / "snapshots"
_SRX = [f"SRX2428313{i}" for i in range(6)]
_SRR = [f"SRR2871655{i}" for i in range(3, 9)]
_SAMN = [*(f"SAMN4093561{i}" for i in range(6, 10)), "SAMN40935620", "SAMN40935621"]
_PROJECT = "PRJNA1027859"
_STUDY = "SRP502277"


def _package(accession: str) -> str:
    return (_DATA / "sra_package" / f"{accession}.xml").read_text()


def _client(**drops: int) -> FakeEntrezClient:
    return FakeEntrezClient(
        esearch={f"{_PROJECT}[si]": ["39637862"]},
        packages={srx: _package(srx) for srx in _SRX},
        drops=drops,
    )


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A store root holding the fixture snapshots and no build yet."""
    path = tmp_path / "store"
    shutil.copytree(_SNAPSHOTS, path / "snapshots")
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("network blocked")

    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def store(root: Path) -> Store:
    """The store after a fetch and a build, opened fresh."""
    fetch(root, [_PROJECT], client=_client(), today="2026-10-14")
    build(root, "b1")
    return open_store(root)


def _rows(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


# ---------------------------------------------------------------- fetch


def test_fetch_saves_one_package_per_experiment_listed_in_the_snapshot(root: Path) -> None:
    client = _client()
    report = fetch(root, [_PROJECT], client=client, today="2026-10-14")
    assert list(report.experiments) == _SRX
    rows = _rows(root / "raw" / "sra-xml" / "SRX2428nnnn.parquet")
    assert [r["accession"] for r in rows] == _SRX
    for row in rows:
        assert row["raw_text"] in _package(str(row["accession"]))
        assert row["hash"] == hashlib.sha256(str(row["raw_text"]).encode()).hexdigest()
        assert row["fetch_date"] == "2026-10-14"
    assert client.count("efetch") == 1


def test_fetch_saves_the_pubmed_si_results_an_empty_one_included(root: Path) -> None:
    fetch(root, [_PROJECT], client=_client(), today="2026-10-14")
    rows = {
        r["accession"]: r["raw_text"]
        for path in sorted((root / "raw" / "pubmed-si").glob("*.parquet"))
        for r in _rows(path)
    }
    assert rows == {_PROJECT: "39637862", _STUDY: ""}
    assert sorted(p.name for p in (root / "raw" / "pubmed-si").glob("*.parquet")) == [
        "PRJNA102nnnn.parquet",
        "SRP50nnnn.parquet",
    ]


def test_fetch_requests_dropped_packages_again(root: Path) -> None:
    client = _client(SRX24283133=1)
    report = fetch(root, [_PROJECT], client=client, today="2026-10-14")
    assert list(report.experiments) == _SRX
    assert client.count("efetch") == 2


def test_fetch_fails_when_a_package_never_comes_back(root: Path) -> None:
    with pytest.raises(EntrezError, match="SRX24283133"):
        fetch(root, [_PROJECT], client=_client(SRX24283133=99), today="2026-10-14")


def test_a_rerun_makes_no_call_and_keeps_the_saved_rows(root: Path) -> None:
    fetch(root, [_PROJECT], client=_client(), today="2026-10-13")
    report = fetch(root, [_PROJECT], client=ExplodingEntrezClient(), today="2026-10-14")
    assert list(report.experiments) == _SRX
    assert report.pubmed_si == {_PROJECT: ("39637862",), _STUDY: ()}
    rows = _rows(root / "raw" / "sra-xml" / "SRX2428nnnn.parquet")
    assert [r["fetch_date"] for r in rows] == ["2026-10-13"] * 6


def test_a_rerun_after_a_killed_run_fetches_only_the_rest(root: Path) -> None:
    fetch(root, ["SRX24283130"], client=_client(), today="2026-10-13")
    client = _client()
    fetch(root, [_PROJECT], client=client, today="2026-10-14")
    assert [c for c in client.calls if c[0] == "efetch"] == [
        ("efetch", ("sra", tuple(_SRX[1:]), "full"))
    ]
    rows = _rows(root / "raw" / "sra-xml" / "SRX2428nnnn.parquet")
    assert [r["accession"] for r in rows] == _SRX
    assert [r["fetch_date"] for r in rows] == ["2026-10-13"] + ["2026-10-14"] * 5


# ---------------------------------------------------------------- build


def test_build_writes_the_tables_and_switches_the_marker(store: Store) -> None:
    out = store.root / "builds" / "b1"
    assert store.settings == {"format_version": 1, "build": "b1"}
    assert sorted(p.name for p in (out / "records").iterdir()) == [
        "bioproject.parquet",
        "biosample.parquet",
        "sra-experiment.parquet",
        "sra-run.parquet",
        "sra-sample.parquet",
        "sra-study.parquet",
    ]
    holdings = [r["accession"] for r in _rows(out / "holdings.parquet")]
    assert holdings == sorted(holdings)
    assert len(holdings) == 1 + 1 + 6 * 4
    by_to = [r["to"] for r in _rows(out / "links_by_to.parquet")]
    assert by_to == sorted(by_to)


def test_a_rebuild_from_the_same_inputs_gives_the_same_tables(store: Store) -> None:
    build(store.root, "b2")
    for name in ("holdings.parquet", "links_by_from.parquet", "records/sra-study.parquet"):
        assert _rows(store.root / "builds" / "b1" / name) == _rows(
            store.root / "builds" / "b2" / name
        )


def test_sra_typed_columns_come_from_the_accessions_snapshot(store: Store) -> None:
    run = store.open("SRR28716553")
    assert (run.status, run.published) == ("live", "2024-05-28")
    assert run.snapshot_date == "2026-10-14"


def test_the_study_carries_the_submission_center_name(store: Store) -> None:
    study = store.open(_STUDY)
    assert study.type == "SRA study"
    assert study.field("SUBMISSION@center_name") == ["Princeton University"]
    assert study.field("STUDY/DESCRIPTOR/STUDY_TITLE")[0].startswith("A Single-Nucleus Atlas")


def test_bioproject_and_biosample_text_come_from_the_snapshots(store: Store) -> None:
    project = store.open(_PROJECT)
    assert (project.type, project.submitted) == ("BioProject", "2023-10-13")
    sample = store.open("SAMN40935616")
    assert (sample.type, sample.status) == ("BioSample", "live")
    assert ("Package/Project/Project/ProjectID/ArchiveID@accession", _PROJECT) in project.fields


def test_copies_of_a_study_that_differ_fail_the_build(root: Path) -> None:
    fetch(root, [_PROJECT], client=_client(), today="2026-10-14")
    from labdata.store import raw

    rows = raw.read_rows(root, raw.SRA_XML)
    changed = rows[-1].raw_text.replace("<STUDY_TITLE>A Single", "<STUDY_TITLE>One")
    raw.write_rows(root, raw.SRA_XML, [raw.RawRow(rows[-1].accession, changed, "2026-10-14")])
    with pytest.raises(LabdataError, match=_STUDY):
        build(root, "bad")


# ---------------------------------------------------------------- the acceptance checks


@pytest.mark.usefixtures("no_network")
def test_experiments_for_gives_six_experiments_without_network(store: Store) -> None:
    experiments = store.experiments_for(_PROJECT)
    assert [e.accession for e in experiments] == _SRX
    assert all(type(e) is Experiment for e in experiments)
    assert {e.study for e in experiments} == {_STUDY}
    assert sorted(r.accession for e in experiments for r in e.runs) == _SRR
    run = experiments[0].runs[0]
    assert run.read_structure
    assert run.total_spots
    with pytest.raises(LabdataError, match="no network call"):
        _ = run.files


@pytest.mark.usefixtures("no_network")
def test_the_paper_comes_from_pubmed_only(store: Store) -> None:
    assert store.follow(_PROJECT, "PubMed") == {_PROJECT: ["PMID:39637862"]}
    links = [k for k in store.open(_PROJECT).links if k.relation == "PubMed"]
    assert [(k.other, k.source) for k in links] == [("PMID:39637862", "PubMed")]
    assert store.follow(_STUDY, "PubMed") == {_STUDY: []}


@pytest.mark.usefixtures("no_network")
def test_each_experiment_reaches_its_biosample(store: Store) -> None:
    samples = store.follow(_SRX, "SRA sample")
    assert all(len(srs) == 1 for srs in samples.values())
    biosamples = store.follow([s for [s] in samples.values()], "BioSample")
    assert sorted(b for [b] in biosamples.values()) == _SAMN
    assert store.follow("SAMN40935616", "BioSample of") == {"SAMN40935616": ["SRS21047787"]}


@pytest.mark.usefixtures("no_network")
def test_each_content_hash_recomputes_from_raw_text_served_in_one_call(store: Store) -> None:
    held = [r["accession"] for r in _rows(store.root / "builds" / "b1" / "holdings.parquet")]
    raw = store.raw(held)
    assert store.backend.calls["raw_many"] == 1
    assert sorted(raw) == held
    for accession in held:
        text = raw[accession].text
        assert hashlib.sha256(text.encode()).hexdigest() == raw[accession].content_hash
        assert store.open(accession).content_hash == raw[accession].content_hash


def test_raw_text_is_byte_identical_to_the_fetched_source(store: Store) -> None:
    raw = store.raw([*_SRX, *_SRR, _STUDY, _PROJECT, *_SAMN])
    packages = "".join(_package(srx) for srx in _SRX)
    for accession in [*_SRX, *_SRR, _STUDY]:
        assert raw[accession].text in packages
    assert raw["SRX24283130"].text.startswith("<EXPERIMENT_PACKAGE>")
    bioproject = (_SNAPSHOTS / "bioproject" / "2026-10-14" / "bioproject.xml").read_text()
    assert raw[_PROJECT].text in bioproject
    import gzip

    biosamples = gzip.decompress(
        (_SNAPSHOTS / "biosample" / "2026-10-14" / "biosample_set.xml.gz").read_bytes()
    ).decode()
    assert all(raw[a].text in biosamples for a in _SAMN)


# ---------------------------------------------------------------- the API around them


def test_open_is_open_many_of_one(store: Store) -> None:
    record = store.open("srx24283130")
    assert record.accession == "SRX24283130"
    assert record.follow("study") == [_STUDY]
    assert record.follow("run") == ["SRR28716558"]
    assert store.backend.calls["get_many"] == 1
    assert store.backend.calls["links_many"] == 1


def test_open_many_returns_errors_in_place(store: Store) -> None:
    result = store.open_many(["SRX24283130", "nope", "SRX1", "PMID:39637862"])
    assert list(result) == ["SRX24283130", "NOPE", "SRX1", "PMID:39637862"]
    assert isinstance(result["NOPE"], AccessionError)
    assert isinstance(result["SRX1"], NotFound)
    assert isinstance(result["PMID:39637862"], AccessionError)


def test_backend_calls_do_not_scale_with_the_batch(store: Store) -> None:
    store.open_many(_SRX)
    store.follow(_SRX, "run")
    store.experiments_for_many([_PROJECT, _STUDY])
    assert store.backend.calls == {"get_many": 1, "links_many": 1 + 1 + 2, "held_many": 3,
                                   "raw_many": 1}  # fmt: skip


def test_a_walk_from_a_root_not_held_raises_not_mirrored(root: Path) -> None:
    client = _client()
    fetch(root, ["SRX24283130"], client=client, today="2026-10-14")
    build(root, "one")
    store = open_store(root)
    assert [e.accession for e in store.experiments_for(_PROJECT)] == ["SRX24283130"]
    with pytest.raises(NotMirrored, match="SRX24283131"):
        store.experiments_for("SRX24283131")


def test_a_store_without_a_build_says_so(root: Path) -> None:
    with pytest.raises(LabdataError, match="labdata store build"):
        open_store(root).open(_PROJECT)


def test_reading_writes_nothing_under_the_root(store: Store) -> None:
    before = sorted(p for p in store.root.rglob("*"))
    store.experiments_for(_PROJECT)
    store.raw([_PROJECT])
    assert sorted(p for p in store.root.rglob("*")) == before


def test_the_one_record_path_imports_neither_pandas_nor_pyarrow(store: Store) -> None:
    code = (
        "import sys; from labdata.store import open_store; "
        f"s = open_store({str(store.root)!r}); s.open('{_PROJECT}'); "
        f"s.follow('{_PROJECT}', 'BioProject of'); s.experiments_for('{_PROJECT}'); "
        "import json; print(json.dumps([m for m in ('pandas', 'pyarrow') if m in sys.modules]))"
    )
    env = {**os.environ, "XDG_CACHE_HOME": str(store.root.parent / "cache")}
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env
    )
    assert json.loads(out.stdout) == []


# ---------------------------------------------------------------- the commands


def test_the_fetch_and_build_commands(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("labdata.cli._store_client", _client)
    runner = CliRunner()
    fetched = runner.invoke(app, ["store", "fetch", _PROJECT, "--root", str(root)])
    assert fetched.exit_code == 0, fetched.output
    assert "Saved 6 SRA packages" in fetched.output
    assert f"{_PROJECT}[si]: 39637862" in fetched.output
    built = runner.invoke(app, ["store", "build", "--root", str(root), "--build-id", "cli"])
    assert built.exit_code == 0, built.output
    assert len(open_store(root).experiments_for(_PROJECT)) == 6
