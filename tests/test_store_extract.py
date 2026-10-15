"""The parallel snapshot extract: its parts equal the serial readers and a rerun resumes.

The oracle is ``labdata.store.snapshots``: the serial stream readers the build used before.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import StoreBuildError
from labdata.store import extract, snapshots
from labdata.store.build import build
from tests.conftest import Decompressor as _Decompressor

_DATA = Path(__file__).parent / "data"
_SLICES = ["prjna1027859", "geo_slice", "sra_slice"]
_BIOPROJECT = r'<ArchiveID\b[^>]*?\baccession="(PRJ[A-Z]+\d+)"'
_BIOSAMPLE = r'<BioSample\b[^>]*?\baccession="(SAM[A-Z]+\d+)"'


def _root(tmp_path: Path, slice_name: str = "prjna1027859") -> Path:
    root = tmp_path / "store"
    shutil.copytree(_DATA / slice_name / "snapshots", root / "snapshots")
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return root


def _rows(folder: Path) -> list[dict[str, object]]:
    return [
        row
        for part in sorted(folder.glob("part-*.parquet"))
        for row in pq.read_table(part).to_pylist()
    ]


def _serial(root: Path, source: str) -> list[dict[str, object]]:
    """What the serial readers give for one source, in the extract's row shape."""
    snap = snapshots.snapshot(root, source)
    if source == snapshots.SRA_ACCESSIONS:
        return [
            {
                "accession": r["Accession"],
                "status": r["Status"] or None,
                "submitted": r["Received"][:10] or None,
                "updated": r["Updated"][:10] or None,
                "published": r["Published"][:10] or None,
            }
            for r in snapshots.tab_rows(snap.path, lambda r: bool(r["Accession"]))
        ]
    if source == snapshots.GEO_SERIES:
        return [{"accession": a} for a in snapshots.accessions(snap, source)]

    tag, key = (
        ("Package", _BIOPROJECT) if source == snapshots.BIOPROJECT else ("BioSample", _BIOSAMPLE)
    )
    every = list(snapshots.accessions(snap, source))
    texts = snapshots.elements(snap.path, tag, every, re.compile(key))
    return [{"accession": a, "raw_text": texts[a]} for a in every]


@pytest.mark.parametrize("slice_name", _SLICES)
@pytest.mark.parametrize("source", extract.SOURCES)
def test_an_extract_equals_the_serial_readers(tmp_path: Path, slice_name: str, source: str) -> None:
    root = _root(tmp_path, slice_name)
    out = extract.extract(root, source, workers=1)
    assert out == root / "work" / "extract" / f"{source}-2026-10-14"
    assert (out / extract.DONE).is_file()
    assert _rows(out) == _serial(root, source)
    assert not list((root / "work" / "tmp").glob("*"))


@pytest.mark.parametrize("source", extract.SOURCES)
def test_tiny_byte_ranges_neither_drop_nor_repeat_a_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    root = _root(tmp_path, "sra_slice")
    monkeypatch.setattr(extract, "_CHUNK", 5)
    out = extract.extract(root, source, workers=1, part_bytes=7)
    assert len(list(out.glob("part-*.parquet"))) > 20
    assert _rows(out) == _serial(root, source)


def test_a_part_already_written_is_skipped_and_a_rerun_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    real = extract._write_part
    calls: list[int] = []

    def killed(task: extract.Task) -> None:
        if len(calls) == 2:
            raise KeyboardInterrupt
        calls.append(task.index)
        real(task)

    monkeypatch.setattr(extract, "_write_part", killed)
    with pytest.raises(KeyboardInterrupt):
        extract.extract(root, "sra-accessions", workers=1, part_bytes=500)
    out = root / "work" / "extract" / "sra-accessions-2026-10-14"
    assert sorted(p.name for p in out.glob("part-*")) == [
        "part-00000.parquet",
        "part-00001.parquet",
    ]
    assert not (out / extract.DONE).exists()

    calls.clear()
    monkeypatch.setattr(extract, "_write_part", lambda task: (calls.append(task.index), real(task)))
    extract.extract(root, "sra-accessions", workers=1, part_bytes=1)
    assert calls[0] == 2
    assert _rows(out) == _serial(root, "sra-accessions")

    calls.clear()
    extract.extract(root, "sra-accessions", workers=1)
    assert calls == []


# Ray leaves a log file handle open at shutdown.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_ray_with_two_workers_gives_the_same_parts(tmp_path: Path) -> None:
    root = _root(tmp_path)
    out = extract.extract(root, "biosample", workers=2, part_bytes=2000)
    assert len(list(out.glob("part-*.parquet"))) > 1
    assert _rows(out) == _serial(root, "biosample")


def test_a_build_without_an_extract_names_the_command_to_run(tmp_path: Path) -> None:
    root = _root(tmp_path)
    for source in ("sra-accessions", "bioproject", "geo-series"):
        extract.extract(root, source, workers=1)
    with pytest.raises(StoreBuildError, match="labdata store extract --source biosample"):
        build(root, "b1")


def test_the_extract_command_does_one_source(tmp_path: Path) -> None:
    root = _root(tmp_path)
    runner = CliRunner()
    done = runner.invoke(
        app, ["store", "extract", "--source", "bioproject", "--workers", "1", "--root", str(root)]
    )
    assert done.exit_code == 0, done.output
    assert [p.name for p in (root / "work" / "extract").iterdir()] == ["bioproject-2026-10-14"]
    bad = runner.invoke(app, ["store", "extract", "--source", "nope", "--root", str(root)])
    assert bad.exit_code != 0


def test_a_gzip_snapshot_streams_through_the_decompressor_seam(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    commands: list[list[str]] = []

    def failing(command: list[str]) -> _Decompressor:
        commands.append(command)
        return _Decompressor(command, code=1)

    monkeypatch.setattr(extract, "_popen", failing)
    with pytest.raises(StoreBuildError, match="failed on .*gds_gse_esummary.jsonl.gz"):
        extract.extract(root, snapshots.GEO_SERIES, workers=1)
    assert commands
    assert commands[0][-1].endswith("gds_gse_esummary.jsonl.gz")
