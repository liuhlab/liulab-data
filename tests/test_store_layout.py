"""The table layout: every built table matches its declaration, whatever the build's memory."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labdata.exceptions import StoreBuildError
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


def test_the_layout_is_the_approved_declaration() -> None:
    settings = {
        t.name: (t.compression, t.compression_level, t.row_group_rows) for t in layout.TABLES
    }
    assert settings.pop("records") == ("zstd", 9, 2048)
    assert set(settings.values()) == {("zstd", 3, 122_880)}


def test_dates_are_written_typed(tmp_path: Path) -> None:
    build_dir = built_root(tmp_path / "store", "geo_slice") / "builds" / "b1"
    types = {
        path.name: {f.name: str(f.type) for f in pq.read_schema(path)}
        for path in (
            layout.records_path(build_dir, "GEO Series"),
            layout.KNOWN.path(build_dir),
            layout.SUPPLEMENTARY_FILES.path(build_dir),
        )
    }
    record_dates = ("submitted", "updated", "published", "snapshot_date")
    assert {c: types["geo-series.parquet"][c] for c in record_dates} == dict.fromkeys(
        record_dates, "date32[day]"
    )
    assert types["known.parquet"]["snapshot_date"] == "date32[day]"
    assert types["supplementary_files.parquet"]["date"] == "timestamp[us]"


#: Rows per row group in the memory test: small enough that every slice table spans several.
_SMALL_GROUP = 3


# Ray leaves a log file handle open at shutdown.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_two_builds_under_different_memory_have_the_same_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = built_root(tmp_path / "store", "geo_slice")
    write = layout.write

    def small_groups(
        con: object, table: layout.Table, out: Path, queries: object, metadata: object = None
    ) -> None:
        write(con, replace(table, row_group_rows=_SMALL_GROUP), out, queries, metadata)  # type: ignore[arg-type]

    monkeypatch.setattr(layout, "write", small_groups)
    build(root, "roomy", workers=2)
    monkeypatch.setenv("LABDATA_DUCKDB_MEMORY", "256MB")
    monkeypatch.setattr(build_module, "_range_bytes", lambda con: 1 << 10)
    build(root, "small", workers=2)
    roomy = _layout(root / "builds" / "roomy")
    assert max(len(groups) for _, _, groups in roomy.values()) > 1  # type: ignore[arg-type]
    assert _layout(root / "builds" / "small") == roomy


def _build_but_records(tmp_path: Path) -> Path:
    """A build folder with every table but records written as declared, each empty."""
    build_dir = tmp_path / "build"
    (build_dir / "records").mkdir(parents=True)
    with extract.connect(tmp_path) as con:
        for table in layout.TABLES[1:]:
            layout.write(con, table, table.path(build_dir), _empty(table))
    return build_dir


def _write_records(tmp_path: Path, build_dir: Path, records: layout.Table) -> None:
    """Write 5,000 records, accession set and every other column NULL, as ``records`` says."""
    rows = (
        f"SELECT e.* REPLACE (printf('GSE%05d', r.i) AS accession) "
        f"FROM range(5000) r(i) LEFT JOIN ({_empty(layout.RECORDS)}) e ON true"
    )
    with extract.connect(tmp_path) as con:
        layout.write(con, records, layout.records_path(build_dir, "GEO Series"), rows)


def test_row_groups_of_another_size_name_the_sizes(tmp_path: Path) -> None:
    build_dir = _build_but_records(tmp_path)
    _write_records(tmp_path, build_dir, replace(layout.RECORDS, row_group_rows=3000))
    assert layout.check(build_dir) == [
        "records/geo-series.parquet: row groups hold [3000, 2000] rows, declared 2048"
    ]


def test_the_compression_level_is_not_recorded_so_only_the_codec_is_checked(
    tmp_path: Path,
) -> None:
    # Parquet column metadata records the codec but not its level, so a file written at
    # another level matches the declaration.
    build_dir = _build_but_records(tmp_path)
    _write_records(tmp_path, build_dir, replace(layout.RECORDS, compression_level=1))
    assert layout.check(build_dir) == []


def test_a_table_that_differs_names_the_table_and_the_setting(tmp_path: Path) -> None:
    build_dir = _build_but_records(tmp_path)
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
    """A query of no rows with the table's columns, dates as text as a writer hands them."""
    types = {layout.INT64: "BIGINT", layout.FIELDS: "STRUCT(name VARCHAR, value VARCHAR)[]"}
    columns = ", ".join(
        f'NULL::{types.get(kind, "VARCHAR")} AS "{name}"' for name, kind in table.columns
    )
    return f"SELECT {columns} WHERE false"


_DATED = layout.Table(
    "dated.parquet",
    (("accession", layout.STRING), ("day", layout.DATE), ("stamp", layout.TIMESTAMP)),
    ("accession",),
)


def _write_dated(tmp_path: Path, rows: str) -> list[dict[str, object]]:
    out = tmp_path / "dated.parquet"
    with extract.connect(tmp_path) as con:
        layout.write(con, _DATED, out, f"SELECT * FROM (VALUES {rows}) v(accession, day, stamp)")
    return pq.read_table(out).to_pylist()


def test_text_dates_are_typed_and_only_empty_ones_become_null(tmp_path: Path) -> None:
    rows = _write_dated(
        tmp_path,
        "('A', '2019-05-01', '2013-01-17T14:38:58'), ('B', NULL, NULL), ('C', '', '')",
    )
    assert [(r["day"], r["stamp"]) for r in rows] == [
        (dt.date(2019, 5, 1), dt.datetime(2013, 1, 17, 14, 38, 58)),
        (None, None),
        (None, None),
    ]


@pytest.mark.parametrize(
    ("day", "stamp", "column", "value"),
    [
        ("2019-05", "2013-01-17T14:38:58", "day", "2019-05"),
        ("May 2019", "2013-01-17T14:38:58", "day", "May 2019"),
        ("2019-05-01", "2013-13-17T14:38:58", "stamp", "2013-13-17T14:38:58"),
    ],
)
def test_a_date_that_does_not_parse_fails_the_write_naming_it(
    tmp_path: Path, day: str, stamp: str, column: str, value: str
) -> None:
    with pytest.raises(StoreBuildError) as caught:
        _write_dated(tmp_path, f"('A', '{day}', '{stamp}')")
    message = str(caught.value)
    assert "dated.parquet" in message
    assert column in message
    assert value in message
