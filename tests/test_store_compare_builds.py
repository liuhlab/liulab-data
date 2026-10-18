"""``scripts/store/compare_builds.py``: two builds hold the same content, cell for cell."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from labdata.store import layout
from tests._slices import built_root

SCRIPT = Path(__file__).parents[1] / "scripts" / "store" / "compare_builds.py"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module")
def _built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp("compare") / "store", "geo_slice") / "builds" / "b1"


@pytest.fixture
def builds(_built: Path, tmp_path: Path) -> tuple[Path, Path]:
    """Two copies of one geo slice build."""
    old, new = tmp_path / "old", tmp_path / "new"
    shutil.copytree(_built, old)
    shutil.copytree(_built, new)
    return old, new


def _compare(old: Path, new: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(old), str(new)],
        capture_output=True,
        text=True,
        check=False,
    )


def _rewrite(path: Path, select: str) -> None:
    """Rewrite one table file from a DuckDB select over its rows, called ``t``."""
    kept = path.with_suffix(".old")
    path.rename(kept)
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT {select} FROM read_parquet('{kept}') t) TO '{path}' (FORMAT parquet)"
    )
    kept.unlink()


def test_identical_builds_match(builds: tuple[Path, Path]) -> None:
    done = _compare(*builds)
    assert done.returncode == 0, done.stdout + done.stderr


def test_a_build_with_text_dates_matches_its_typed_twin(builds: tuple[Path, Path]) -> None:
    old, new = builds
    for path in (old / "records").glob("*.parquet"):
        dates = ("submitted", "updated", "published", "snapshot_date")
        _rewrite(
            path, "* REPLACE (" + ", ".join(f"CAST({d} AS VARCHAR) AS {d}" for d in dates) + ")"
        )
    _rewrite(
        layout.SUPPLEMENTARY_FILES.path(old),
        "* REPLACE (strftime(date, '%Y-%m-%dT%H:%M:%S') AS date)",
    )
    assert str(pq.read_schema(layout.records_path(old, "GEO Series")).field("published").type) == (
        "string"
    )
    done = _compare(old, new)
    assert done.returncode == 0, done.stdout + done.stderr


def test_a_one_cell_change_fails_naming_the_cell(builds: tuple[Path, Path]) -> None:
    old, new = builds
    path = layout.records_path(new, "GEO Sample")
    accession = pq.read_table(path, columns=["accession"]).column(0)[3].as_py()
    _rewrite(
        path,
        f"* REPLACE (CASE WHEN accession = '{accession}' THEN raw_text || 'x' "
        "ELSE raw_text END AS raw_text)",
    )
    done = _compare(old, new)
    assert done.returncode == 1
    assert "records/geo-sample.parquet" in done.stdout
    assert "raw_text" in done.stdout
    assert accession in done.stdout
    assert "1 row" in done.stdout
