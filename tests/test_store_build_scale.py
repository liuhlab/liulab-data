"""The build at full scale: bounded memory, parallel parsing, and the stage 1 tables kept.

``tests/data/stage1_tables.json`` is the stage 1 builder's output on the thin slice: for
each slice and table, its row count, schema and the sha256 of its rows as sorted-key JSON.
It was made by building each slice of ``tests/_slices.py`` with the in-memory builder of
commit 263bac3 (the stage 2 branch before the bounded build) and reading every table back.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tracemalloc
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labdata.exceptions import StoreBuildError
from labdata.store import extract, init_store, open_store
from labdata.store.build import build
from tests._slices import DATA, SLICES, built_root, copy_snapshots

_STAGE1 = json.loads((DATA / "stage1_tables.json").read_text())
_RAW_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("raw_text", pa.string()),
        ("hash", pa.string()),
        ("fetch_date", pa.string()),
    ]
)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _digest(build_dir: Path) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for path in sorted(build_dir.rglob("*.parquet")):
        table = pq.read_table(path)
        rows = json.dumps(table.to_pylist(), sort_keys=True, default=str)
        out[str(path.relative_to(build_dir))] = {
            "rows": table.num_rows,
            "schema": [f"{f.name}: {f.type}" for f in table.schema],
            "sha256": hashlib.sha256(rows.encode()).hexdigest(),
        }
    return out


def _write_raw(root: Path, source: str, bucket: str, rows: list[tuple[str, str]]) -> None:
    """Write one raw bucket file as the pack step does: accession, text, hash, fetch date."""
    folder = root / "raw" / source
    folder.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows)
    table = pa.table(
        {
            "accession": [a for a, _ in ordered],
            "raw_text": [t for _, t in ordered],
            "hash": [hashlib.sha256(t.encode()).hexdigest() for _, t in ordered],
            "fetch_date": ["2026-10-14"] * len(ordered),
        },
        schema=_RAW_SCHEMA,
    )
    pq.write_table(table, folder / f"{bucket}.parquet")


@pytest.mark.parametrize("slice_name", sorted(SLICES))
def test_tables_equal_those_of_the_stage_1_builder(tmp_path: Path, slice_name: str) -> None:
    root = built_root(tmp_path / "store", slice_name)
    assert _digest(root / "builds" / "b1") == _STAGE1[slice_name]


# Ray leaves a log file handle open at shutdown.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_parsing_on_two_workers_gives_the_same_tables(tmp_path: Path) -> None:
    root = built_root(tmp_path / "store", "geo_slice")
    build(root, "b2", workers=2)
    assert _digest(root / "builds" / "b2") == _STAGE1["geo_slice"]


def _package(n: int, padding: int) -> str:
    """A small experiment package; every ten experiments share one study."""
    return (
        f'<EXPERIMENT_PACKAGE><EXPERIMENT accession="SRX{n}"><TITLE>{"x" * padding}</TITLE>'
        f'<STUDY_REF accession="SRP{n // 10}"/></EXPERIMENT>'
        '<SUBMISSION center_name="LAB"/>'
        f'<STUDY accession="SRP{n // 10}"><DESCRIPTOR><STUDY_TITLE>t</STUDY_TITLE></DESCRIPTOR>'
        f'</STUDY><RUN accession="SRR{n}"/></EXPERIMENT_PACKAGE>'
    )


def test_peak_memory_is_bounded_by_the_chunk_not_the_store(tmp_path: Path) -> None:
    root = tmp_path / "store"
    copy_snapshots(SLICES["prjna1027859"][0], root)
    init_store(root)
    chunk = 1 << 18
    experiments, padding, per_bucket = 4000, 4000, 1000
    for start in range(0, experiments, per_bucket):
        rows = [(f"SRX{n}", _package(n, padding)) for n in range(start + 1, start + per_bucket + 1)]
        _write_raw(root, "sra-xml", f"SRX{start}", rows)
    raw_bytes = experiments * padding
    assert raw_bytes > 50 * chunk

    tracemalloc.start()
    try:
        build(root, "big", workers=1, chunk_bytes=chunk)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 16 * chunk, f"peak {peak} bytes for a {chunk}-byte chunk"
    store = open_store(root)
    assert store.follow("SRX4000", "study") == {"SRX4000": ["SRP400"]}
    assert pq.read_metadata(root / "builds" / "big" / "records" / "sra-run.parquet").num_rows == (
        experiments
    )


def test_an_experiment_with_no_package_is_a_build_note(tmp_path: Path) -> None:
    root = built_root(tmp_path / "store", "prjna1027859")
    _write_raw(root, "sra-xml", "SRX9999nnnn", [("SRX99999999", "")])
    out = build(root, "b2")
    notes = pq.read_table(out / "notes.parquet").to_pylist()
    assert {
        "accession": "SRX99999999",
        "kind": "no SRA package",
        "detail": "SRA_Accessions.tab status: not listed",
    } in notes
    assert json.loads((out / "manifest.json").read_text())["notes"]["no SRA package"] == 1


def test_differing_copies_name_the_record_and_both_origins(tmp_path: Path) -> None:
    root = tmp_path / "store"
    copy_snapshots(SLICES["prjna1027859"][0], root)
    init_store(root)
    changed = _package(21, 5).replace("<STUDY_TITLE>t<", "<STUDY_TITLE>changed<")
    _write_raw(root, "sra-xml", "SRX0nnnn", [("SRX20", _package(20, 5)), ("SRX21", changed)])
    with pytest.raises(StoreBuildError, match=r"copies of SRP2 differ.*SRX21 differs"):
        build(root, "bad", workers=1)
    assert not (root / "builds" / "bad").exists()
    assert not list((root / "work" / "build").glob("*"))


class _Writes:
    """Record every file opened for writing while switched on (an audit hook stays for life)."""

    def __init__(self) -> None:
        self.on = False
        self.paths: list[str] = []
        sys.addaudithook(self._hook)

    def _hook(self, event: str, args: tuple[Any, ...]) -> None:
        if not self.on or event != "open":
            return
        path, mode = args[0], args[1] if len(args) > 1 else None
        if isinstance(path, str | Path) and isinstance(mode, str) and set(mode) & set("wax+"):
            self.paths.append(str(path))


_WRITES = _Writes()


def test_the_biosample_extract_writes_no_unpacked_copy(tmp_path: Path) -> None:
    root = tmp_path / "store"
    shutil.copytree(SLICES["prjna1027859"][0], root / "snapshots")
    gz = root / "snapshots" / "biosample" / "2026-10-14" / "biosample_set.xml.gz"
    assert gz.is_file()
    _WRITES.paths.clear()
    _WRITES.on = True
    try:
        out = extract.extract(root, "biosample", workers=1, part_bytes=2000)
    finally:
        _WRITES.on = False
    written = [Path(p) for p in _WRITES.paths]
    assert written
    assert all(p.parent == out for p in written), written
