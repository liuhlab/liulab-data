"""Build, switch and rebuild offline; every store folder documents its own files."""

from __future__ import annotations

import hashlib
import json
import shutil
import socket
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import LabdataError, StoreBuildError
from labdata.store import build as build_module
from labdata.store import extract, init_store, open_store
from labdata.store.build import build, build_lock
from labdata.store.extract import duckdb_memory
from labdata.store.fetch import fetch
from tests._fakes import FakeEntrezClient
from tests._slices import copy_snapshots

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
    copy_snapshots(_DATA / "prjna1027859" / "snapshots", root)
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
    assert open_store(root).settings["build"] == build_id
    build(root, "b3")
    settings = open_store(root).settings
    assert settings["build"] == "b3"
    assert sorted(settings["switched_away"]) == ["b1", build_id]


def test_a_build_killed_before_its_rename_leaves_no_build_and_reruns(root: Path) -> None:
    stale = root / "builds" / ".b2.99999999.tmp"
    (stale / "records").mkdir(parents=True)
    (stale / "records" / "half.parquet").write_text("half")
    build(root, "b2")
    assert not stale.exists()
    assert not (root / "builds" / "b2" / "records" / "half.parquet").exists()


def test_a_rebuild_links_the_known_table_computed_once_per_snapshot_set(root: Path) -> None:
    build(root, "b2")
    cached = list((root / "work" / "known").glob("known-*.parquet"))
    assert len(cached) == 1
    inodes = {(root / "builds" / b / "known.parquet").stat().st_ino for b in ("b1", "b2")}
    assert inodes == {cached[0].stat().st_ino}


def test_a_second_build_while_one_runs_fails_with_a_clear_error(root: Path) -> None:
    with build_lock(root), pytest.raises(StoreBuildError, match="another build"):
        build(root, "b2")
    assert not (root / "builds" / "b2").exists()
    build(root, "b2")


def test_the_duckdb_memory_cap_defaults_from_the_slurm_allocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LABDATA_DUCKDB_MEMORY", raising=False)
    monkeypatch.delenv("LABDATA_MEMORY_MB", raising=False)
    monkeypatch.setenv("SLURM_MEM_PER_NODE", "64000")
    assert duckdb_memory() == "32000MB"
    monkeypatch.setenv("LABDATA_DUCKDB_MEMORY", "2GB")
    assert duckdb_memory() == "2GB"


def test_a_record_range_follows_the_duckdb_cap_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LABDATA_MEMORY_MB", "8000")
    sizes = []
    for cap in ("1GiB", "4GiB"):
        monkeypatch.setenv("LABDATA_DUCKDB_MEMORY", cap)
        with extract.connect(tmp_path) as con:
            sizes.append(build_module._range_bytes(con))
    assert sizes == [(1 << 30) // 16, (4 << 30) // 16]


class _ShownCap:
    """A connection whose ``memory_limit`` shows as a given string, or as no row."""

    def __init__(self, shown: str | None) -> None:
        self.shown = shown

    def execute(self, _sql: str) -> _ShownCap:
        return self

    def fetchone(self) -> tuple[str] | None:
        return None if self.shown is None else (self.shown,)


@pytest.mark.parametrize(
    ("shown", "cap"),
    [
        ("1.5 GiB", 3 << 29),
        ("2.0 TiB", 2 << 40),
        ("1.5 GB", 1_500_000_000),
        ("8 MB", 8_000_000),
        ("32000 bytes", 32_000),
        ("1 byte", 16),
    ],
)
def test_a_record_range_reads_every_unit_duckdb_shows_its_cap_in(shown: str, cap: int) -> None:
    con: Any = _ShownCap(shown)
    assert build_module._range_bytes(con) == max(cap // 16, 1)


@pytest.mark.parametrize("shown", ["unlimited", "12 parsecs", "", None])
def test_a_duckdb_cap_that_cannot_be_read_stops_the_build(shown: str | None) -> None:
    con: Any = _ShownCap(shown)
    with pytest.raises(StoreBuildError, match="memory_limit"):
        build_module._range_bytes(con)


def test_a_pipeline_steps_duckdb_and_ray_stay_inside_its_memory_and_threads(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("LABDATA_DUCKDB_MEMORY", raising=False)
    monkeypatch.setenv("SLURM_MEM_PER_NODE", "64000")
    monkeypatch.setenv("LABDATA_MEMORY_MB", "8000")
    monkeypatch.setenv("LABDATA_THREADS", "3")
    with extract.connect(root) as con:
        limit, threads = con.execute(
            "SELECT current_setting('memory_limit'), current_setting('threads')"
        ).fetchone()  # type: ignore[misc]
    assert threads == 3
    seen: dict[str, Any] = {}
    import ray

    monkeypatch.setattr(ray, "is_initialized", lambda: False)
    monkeypatch.setattr(ray, "init", lambda **kw: seen.update(kw))
    monkeypatch.setattr(ray, "shutdown", lambda: None)
    with extract.parallel(4):
        pass
    duckdb_mb = float(limit.split()[0]) * {"MiB": 1.048576, "GiB": 1073.741824}[limit.split()[1]]
    ray_mb = seen["object_store_memory"] / 1e6
    assert duckdb_mb + ray_mb <= 8000 * 0.75


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
    copy_snapshots(_DATA / "geo_slice" / "snapshots", root)
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
