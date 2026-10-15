"""Build, switch and rebuild offline; every store folder documents its own files."""

from __future__ import annotations

import hashlib
import json
import shutil
import socket
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import LabdataError
from labdata.store import init_store, open_store
from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import FakeEntrezClient

_DATA = Path(__file__).parent / "data"
_PROJECT = "PRJNA1027859"
_SRX = [f"SRX2428313{i}" for i in range(6)]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A store holding PRJNA1027859, fetched and built once as ``b1``."""
    root = tmp_path / "store"
    shutil.copytree(_DATA / "prjna1027859" / "snapshots", root / "snapshots")
    init_store(root)
    client = FakeEntrezClient(
        esearch={f"{_PROJECT}[si]": ["39637862"]},
        packages={srx: (_DATA / "sra_package" / f"{srx}.xml").read_text() for srx in _SRX},
    )
    fetch(root, [_PROJECT], client=client, today="2026-10-14")
    build(root, "b1")
    return root


def _tables(build_dir: Path) -> dict[str, object]:
    """Every table of a build, as Python data, keyed by its path in the build."""
    return {
        str(p.relative_to(build_dir)): pq.read_table(p).to_pylist()
        for p in sorted(build_dir.rglob("*.parquet"))
    }


def _no_network(*_args: object, **_kwargs: object) -> socket.socket:
    raise AssertionError("the build opened a network socket")


# ---------------------------------------------------------------- init


def test_init_writes_the_marker_and_refuses_a_second_time(tmp_path: Path) -> None:
    root = tmp_path / "new" / "store"
    init_store(root)
    assert (root / "labdata-store.toml").read_text() == "format_version = 1\n"
    assert open_store(root).settings == {"format_version": 1}
    with pytest.raises(LabdataError, match="already"):
        init_store(root)


def test_init_leaves_existing_snapshots_alone(tmp_path: Path) -> None:
    root = tmp_path / "store"
    snap = root / "snapshots" / "bioproject" / "2026-10-14"
    snap.mkdir(parents=True)
    (snap / "bioproject.xml").write_text("<x/>")
    result = CliRunner().invoke(app, ["store", "init", str(root)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in snap.iterdir()) == ["bioproject.xml"]
    assert CliRunner().invoke(app, ["store", "init", str(root)]).exit_code != 0


# ---------------------------------------------------------------- build, switch, rebuild


def test_a_rebuild_with_the_network_blocked_gives_the_same_tables(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(socket, "socket", _no_network)
    monkeypatch.setattr(socket, "create_connection", _no_network)
    build(root, "b2")
    assert open_store(root).settings["build"] == "b2"
    first, second = _tables(root / "builds" / "b1"), _tables(root / "builds" / "b2")
    assert "records/bioproject.parquet" in first
    assert first == second


def test_an_old_store_keeps_reading_its_build_after_the_switch(root: Path) -> None:
    old = open_store(root)
    assert old.open(_PROJECT).accession == _PROJECT
    build(root, "b2")
    shutil.rmtree(root / "raw")
    assert old.settings["build"] == "b1"
    assert old.open("SRX24283130").accession == "SRX24283130"
    assert open_store(root).settings["build"] == "b2"


def test_the_marker_keeps_a_build_id_toml_must_escape(root: Path) -> None:
    build_id = 'b2 "quoted" \\ \x7f \U0001f9ec'
    build(root, build_id)
    assert open_store(root).settings == {"format_version": 1, "build": build_id}


def test_a_build_killed_before_its_rename_leaves_no_build_and_reruns(root: Path) -> None:
    stale = root / "builds" / ".b2.99999999.tmp"
    (stale / "records").mkdir(parents=True)
    (stale / "records" / "half.parquet").write_text("half")
    build(root, "b2")
    assert not stale.exists()
    assert not (root / "builds" / "b2" / "records" / "half.parquet").exists()


# ---------------------------------------------------------------- provenance


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _documented(folder: Path) -> None:
    """Assert the folder's README and manifest name each of its data files, size and sha256."""
    manifest = json.loads((folder / "manifest.json").read_text())
    readme = (folder / "README.md").read_text()
    data = {
        str(p.relative_to(folder))
        for p in folder.rglob("*")
        if p.is_file() and p.name not in ("README.md", "manifest.json")
    }
    assert {f["name"] for f in manifest["files"]} == data
    for entry in manifest["files"]:
        path = folder / entry["name"]
        assert entry["bytes"] == path.stat().st_size
        assert entry["sha256"] == _sha256(path)
        assert entry["name"] in readme
        assert entry["sha256"] in readme
    produced = manifest["produced_by"]
    assert produced["command"] in readme
    assert "commit" in produced
    assert "How to refresh" in readme


def test_every_build_raw_and_snapshot_folder_has_a_matching_readme_and_manifest(
    root: Path,
) -> None:
    result = CliRunner().invoke(app, ["store", "verify", "--root", str(root), "--write-missing"])
    assert result.exit_code == 0, result.output
    folders = [
        root / "builds" / "b1",
        *sorted((root / "raw").iterdir()),
        *sorted((root / "snapshots").glob("*/*")),
    ]
    assert len(folders) == 1 + 2 + 4
    for folder in folders:
        _documented(folder)
    build_manifest = json.loads((root / "builds" / "b1" / "manifest.json").read_text())
    assert build_manifest["produced_by"]["command"] == "labdata store build"
    assert set(build_manifest["inputs"]["raw"]) == {"pubmed-si", "sra-xml"}


def test_write_missing_keeps_a_downloaders_readme_and_manifest(root: Path) -> None:
    snap = root / "snapshots" / "bioproject" / "2026-10-14"
    xml = snap / "bioproject.xml"
    theirs = {
        "url": "https://example.org/x",
        "files": [{"name": xml.name, "bytes": xml.stat().st_size, "sha256": _sha256(xml)}],
    }
    (snap / "manifest.json").write_text(json.dumps(theirs))
    (snap / "README.md").write_text("hand-written\n")
    result = CliRunner().invoke(app, ["store", "verify", "--root", str(root), "--write-missing"])
    assert result.exit_code == 0, result.output
    assert json.loads((snap / "manifest.json").read_text()) == theirs
    assert (snap / "README.md").read_text() == "hand-written\n"


def test_verify_reports_a_file_that_no_longer_matches_its_manifest(root: Path) -> None:
    runner = CliRunner()
    assert (
        runner.invoke(app, ["store", "verify", "--root", str(root), "--write-missing"]).exit_code
        == 0
    )
    tab = root / "snapshots" / "sra-accessions" / "2026-10-14" / "SRA_Accessions.tab"
    tab.write_text(tab.read_text() + "SRX0\n")
    result = runner.invoke(app, ["store", "verify", "--root", str(root)])
    assert result.exit_code == 1
    assert "SRA_Accessions.tab" in result.output


def test_a_geo_rebuild_offline_keeps_supplementary_files_and_documents_filelists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.test_store_suppl import _client as suppl_client

    root = tmp_path / "geo"
    shutil.copytree(_DATA / "geo_slice" / "snapshots", root / "snapshots")
    init_store(root)
    fetch(root, ["GSE15556"], client=suppl_client(), today="2026-10-14")
    build(root, "b1")
    monkeypatch.setattr(socket, "socket", _no_network)
    monkeypatch.setattr(socket, "create_connection", _no_network)
    build(root, "b2")
    first, second = _tables(root / "builds" / "b1"), _tables(root / "builds" / "b2")
    assert first["supplementary_files.parquet"]
    assert first == second
    result = CliRunner().invoke(app, ["store", "verify", "--root", str(root), "--write-missing"])
    assert result.exit_code == 0, result.output
    _documented(root / "raw" / "geo-filelist")
    _documented(root / "builds" / "b2")
