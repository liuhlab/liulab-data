"""The table layout: every built table matches its declaration, whatever the build's memory."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labdata.store import build as build_module
from labdata.store import extract, layout
from labdata.store.build import build
from tests._slices import built_root


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _layout(build_dir: Path) -> dict[str, tuple[object, ...]]:
    """Each table file's schema, codecs and rows per row group."""
    out: dict[str, tuple[object, ...]] = {}
    for path in sorted(build_dir.rglob("*.parquet")):
        meta = pq.read_metadata(path)
        groups = [meta.row_group(i) for i in range(meta.num_row_groups)]
        out[str(path.relative_to(build_dir))] = (
            str(meta.schema.to_arrow_schema().remove_metadata()),
            sorted({g.column(c).compression for g in groups for c in range(g.num_columns)}),
            [g.num_rows for g in groups],
        )
    return out


def test_every_built_table_matches_its_declaration(tmp_path: Path) -> None:
    build_dir = built_root(tmp_path / "store", "geo_slice") / "builds" / "b1"
    assert sorted(p.name for p in build_dir.glob("records/*.parquet"))
    assert layout.check(build_dir) == []


# Ray leaves a log file handle open at shutdown.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_two_builds_under_different_memory_have_the_same_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = built_root(tmp_path / "store", "geo_slice")
    monkeypatch.setenv("LABDATA_DUCKDB_MEMORY", "256MB")
    monkeypatch.setattr(build_module, "_range_bytes", lambda con: 1 << 10)
    build(root, "small", workers=2)
    assert _layout(root / "builds" / "small") == _layout(root / "builds" / "b1")


def test_a_table_that_differs_names_the_table_and_the_setting(tmp_path: Path) -> None:
    build_dir = tmp_path / "build"
    (build_dir / "records").mkdir(parents=True)
    with extract.connect(tmp_path) as con:
        for table in layout.TABLES[1:]:
            layout.write(con, table, table.path(build_dir), _empty(table))
    pq.write_table(
        pa.table({"accession": ["B", "A"], "type": ["t", "t"]}),
        layout.records_path(build_dir, "GEO Series"),
        compression="snappy",
    )
    columns, codec, order = layout.check(build_dir)
    assert columns.startswith(
        "records/geo-series.parquet: columns are (accession: string, type: string), "
        "declared (accession: string, type: string, status: string,"
    )
    assert codec == "records/geo-series.parquet: compression is SNAPPY, declared ZSTD"
    assert order == "records/geo-series.parquet: rows not sorted by accession"


def test_rows_go_out_sorted_in_row_groups_of_the_declared_size(tmp_path: Path) -> None:
    table = layout.Table(
        "t.parquet", (("accession", layout.STRING),), ("accession",), row_group_rows=3
    )
    out = tmp_path / "t.parquet"
    ranges = (f"SELECT 'A' || (7 - i + {lo}) AS accession FROM range(4) r(i)" for lo in (-4, 0))
    with extract.connect(tmp_path) as con:
        layout.write(con, table, out, ranges)
    meta = pq.read_metadata(out)
    assert [meta.row_group(i).num_rows for i in range(meta.num_row_groups)] == [3, 3, 2]
    assert pq.read_table(out).column(0).to_pylist() == [f"A{i}" for i in range(8)]


def _empty(table: layout.Table) -> str:
    columns = ", ".join(
        f'NULL::{"BIGINT" if kind == layout.INT64 else "VARCHAR"} AS "{name}"'
        for name, kind in table.columns
    )
    return f"SELECT {columns} WHERE false"
