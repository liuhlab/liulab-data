"""The target Series list, gaps, type notes and ``labdata store coverage``."""

from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

import labdata.ncbi.entrez as entrez_mod
from labdata.cli import app
from labdata.store import eutils, extract, open_store, pack, raw
from labdata.store.build import build
from labdata.store.gaps import FETCH_FAILED, SOFT_MISSING, gap_row
from labdata.store.model import Link
from tests.test_store_eutils import _client, _Ncbi

_DATA = Path(__file__).parent / "data" / "geo_slice"
_DATE = "2026-10-14"
_SEQ = "Expression profiling by high throughput sequencing"
_ARRAY = "Expression profiling by array"

#: The fixture snapshot: a sequencing Series, an array-only Series, a type-disagreement
#: Series (its SOFT says array) and two sequencing Series with no SOFT.
_SNAPSHOT = [
    {"accession": "GSE19456", "gdstype": _SEQ, "bioproject": "PRJNA120321"},
    {"accession": "GSE20301", "gdstype": _ARRAY, "bioproject": "PRJNA125337"},
    {"accession": "GSE153830", "gdstype": f"{_ARRAY}; {_SEQ}", "bioproject": "PRJNA644323"},
    {"accession": "GSE28771", "gdstype": _SEQ, "bioproject": "PRJNA63453"},
    {"accession": "GSE28772", "gdstype": _SEQ, "bioproject": ""},
]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _soft(gse: str) -> str:
    return (_DATA / "soft" / f"{gse}.soft").read_text(encoding="utf-8")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A store with the fixture snapshot and the SOFT of the three Series that have one."""
    root = tmp_path / "store"
    shutil.copytree(_DATA / "snapshots", root / "snapshots")
    path = root / "snapshots" / "geo-series" / _DATE / "gds_gse_esummary.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.writelines(json.dumps(record) + "\n" for record in _SNAPSHOT)
    for source in extract.SOURCES:
        extract.extract(root, source, workers=1)
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    disagrees = _soft("GSE153830").replace(f"!Series_type = {_SEQ}", f"!Series_type = {_ARRAY}")
    pack.write_rows(
        root,
        raw.GEO_SOFT,
        [
            raw.RawRow("GSE19456", _soft("GSE19456"), _DATE),
            raw.RawRow("GSE20301", _soft("GSE20301"), _DATE),
            raw.RawRow("GSE153830", disagrees, _DATE),
        ],
    )
    pack.write_rows(root, raw.GEO_GAPS, [gap_row("GSE28771", SOFT_MISSING, _DATE)])
    return root


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def test_build_writes_gaps_and_type_notes_for_the_target_list(root: Path) -> None:
    out = build(root, "b1")
    assert _rows(out / "gaps.parquet") == [
        {"accession": "GSE28771", "reason": "soft-missing", "detail": None},
        {"accession": "GSE28772", "reason": "unexplained", "detail": None},
    ]
    notes = _rows(out / "notes.parquet")
    assert notes == [
        {
            "accession": "GSE153830",
            "kind": "series type disagrees",
            "detail": f"snapshot: {_ARRAY}; {_SEQ} | SOFT: {_ARRAY}",
        }
    ]
    coverage = json.loads((out / "manifest.json").read_text())["target_series"]
    assert coverage == {
        "snapshot_date": _DATE,
        "target": 4,
        "held": 2,
        "gaps": {"soft-missing": 1, "fetch-failed": 0, "unexplained": 1},
    }


def _coverage(root: Path) -> tuple[int, str]:
    result = CliRunner().invoke(app, ["store", "coverage", "--root", str(root)])
    return result.exit_code, result.output


def test_coverage_fails_and_names_an_unexplained_gap(root: Path) -> None:
    build(root, "b1")
    code, output = _coverage(root)
    assert code == 1
    assert "Target Series: 4" in output
    assert "Held: 2" in output
    assert "soft-missing: 1" in output
    assert "unexplained: 1" in output
    assert "GSE28772: unexplained" in output


def test_coverage_passes_when_every_target_series_is_held_or_explained(root: Path) -> None:
    pack.write_rows(root, raw.GEO_GAPS, [gap_row("GSE28772", FETCH_FAILED, _DATE, "timed out")])
    build(root, "b1")
    code, output = _coverage(root)
    assert code == 0, output
    assert "fetch-failed: 1" in output
    assert "unexplained: 0" in output


def test_a_snapshot_bioproject_is_a_geo_summary_link_from_both_ends(root: Path) -> None:
    build(root, "b1")
    store = open_store(root)
    series = store.links("GSE153830", "BioProject")["GSE153830"]
    assert Link("BioProject", "PRJNA644323", "GEO summary", True) in series
    assert Link("BioProject", "PRJNA644323", "SOFT", True) in series
    project = store.links("PRJNA644323", "BioProject of")["PRJNA644323"]
    assert Link("BioProject of", "GSE153830", "GEO summary", False) in project


def test_a_rounds_build_and_coverage_cover_only_its_sample(root: Path) -> None:
    # At 0.7 the seeded sample holds GSE153830 (saved) and GSE28772 (no SOFT, no gap row).
    out = build(root, "b1", fraction=0.7)
    assert _rows(out / "gaps.parquet") == [
        {"accession": "GSE28772", "reason": "unexplained", "detail": None},
    ]
    code, output = _coverage(root)
    assert code == 1
    assert "Target Series: 2" in output
    assert "Held: 1" in output
    assert "soft-missing: 0" in output


def test_experiments_whose_package_fetch_failed_are_noted_and_counted(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    pack.write_rows(root, raw.GEO_GAPS, [gap_row("GSE28772", FETCH_FAILED, _DATE)])
    listed = eutils.experiment_list(root)
    down = _Ncbi(known=set(), broken_calls=1 << 30)
    assert eutils.fetch_packages(root, listed, client=_client(down), date=_DATE).failed == 9
    out = build(root, "b1")
    failed = [n["accession"] for n in _rows(out / "notes.parquet") if n["kind"] == FETCH_FAILED]
    assert len(failed) == 9
    assert "SRX8670456" in failed
    code, output = _coverage(root)
    assert code == 0, output
    assert "fetch-failed: 1\n" in output  # the Series gap only
    assert "SRA experiments fetch-failed: 9" in output
