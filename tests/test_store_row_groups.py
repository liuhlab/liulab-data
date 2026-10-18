"""Lookups read only the row groups that hold their accessions.

The fixture build is the GEO slice with a few rows per row group, so every record and link
file has several. DuckDB's Parquet reader logs every row group it reads, with the columns it
reads from it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from labdata.store import Store, open_store
from labdata.store import build as build_module
from labdata.store import reader as reader_module
from tests._slices import built_root

#: Rows per row group of the fixture build's record and link files.
_ROWS = 4

#: A read: the file inside the build, the row group and one column read from it.
Read = tuple[str, int, str]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module")
def root(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    with pytest.MonkeyPatch.context() as patch:
        for name in ("RECORDS", "LINKS_BY_FROM", "LINKS_BY_TO"):
            patch.setattr(
                build_module, name, replace(getattr(build_module, name), row_group_rows=_ROWS)
            )
        yield built_root(tmp_path_factory.mktemp("groups") / "store", "geo_slice")


def _build(store: Store) -> Path:
    return store.root / "builds" / str(store.settings["build"])


def _groups(path: Path, column: str) -> dict[str, set[int]]:
    """Map each value of ``column`` to the row groups of ``path`` that hold it."""
    parquet = pq.ParquetFile(path)
    out: dict[str, set[int]] = {}
    for group in range(parquet.num_row_groups):
        for value in parquet.read_row_group(group, [column]).column(0).to_pylist():
            out.setdefault(value, set()).add(group)
    return out


def _reads(store: Store, lookup: Callable[[Store], object]) -> set[Read]:
    """Run ``lookup`` on ``store`` and return every row group its Parquet scans read.

    DuckDB's Parquet reader logs each row group it reads or skips by statistics.
    """
    con = store.reader.connection
    con.execute("CALL enable_logging('PhysicalOperator', level = 'trace')")
    lookup(store)
    build = _build(store).resolve()
    rows = con.execute(
        "SELECT info['file'], info['row_group_id'], parameters['Projections'] "
        "FROM duckdb_logs_parsed('PhysicalOperator') "
        "WHERE class = 'ParquetReader' AND event = 'ReadRowGroup'"
    ).fetchall()
    return {
        (str(Path(file).resolve().relative_to(build)), int(group), column)
        for file, group, projections in rows
        for column in projections.split()
    }


#: Well-formed accessions the fixture lacks, sorting before every key it holds. A lookup of
#: more keys than DuckDB turns into an ``IN`` filter for a join reads every row group between
#: the smallest and the largest key.
_ABSENT = [f"DRX{n}" for n in range(1, 61)]


def _held_groups(reads: set[Read], file: str) -> set[int]:
    return {group for name, group, _ in reads if name == file}


def test_a_lookup_reads_only_the_row_groups_that_hold_its_accessions(root: Path) -> None:
    store = open_store(root)
    samples = _groups(_build(store) / "records" / "geo-sample.parquet", "accession")
    first, last = sorted(samples)[0], sorted(samples)[-1]
    assert (
        len(samples[first] | samples[last])
        == 2
        < pq.ParquetFile(_build(store) / "records" / "geo-sample.parquet").num_row_groups - 1
    )
    reads = _reads(store, lambda s: s.open_many([first, last, *_ABSENT]))
    assert {name for name, _, _ in reads if name.startswith("records/")} == {
        "records/geo-sample.parquet"
    }
    assert _held_groups(reads, "records/geo-sample.parquet") == samples[first] | samples[last]


def test_a_links_lookup_reads_only_the_row_groups_of_its_ends(root: Path) -> None:
    store = open_store(root)
    build = _build(store)
    by_from = _groups(build / "links_by_from.parquet", "from")
    by_to = _groups(build / "links_by_to.parquet", "to")
    sample = sorted(a for a in by_from if a.startswith("GSM") and a in by_to)[0]
    reads = _reads(store, lambda s: s.links([sample, *_ABSENT]))
    assert _held_groups(reads, "links_by_from.parquet") == by_from[sample]
    assert _held_groups(reads, "links_by_to.parquet") == by_to[sample]


def test_an_ordinary_lookup_reads_no_raw_text(root: Path) -> None:
    store = open_store(root)
    held = pq.read_table(_build(store) / "holdings.parquet").column("accession").to_pylist()
    opened = _reads(store, lambda s: s.open_many(held))
    assert opened
    assert not {r for r in opened if r[2] == "raw_text"}
    raw = _reads(open_store(root), lambda s: s.raw(held[:1]))
    assert {r for r in raw if r[2] == "raw_text"}


def test_a_lookup_split_into_many_batches_returns_the_same(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    held = pq.read_table(_build(open_store(root)) / "holdings.parquet").column("accession")
    accessions = [*held.to_pylist(), "GSM999999999", "SRX99999999", "nope", "PMID:1"]
    wanted = held.to_pylist()

    def everything(store: Store) -> tuple[object, ...]:
        opened = {
            a: (type(r).__name__, str(r)) if isinstance(r, Exception) else r
            for a, r in store.open_many(accessions).items()
        }
        return (
            opened,
            store.links(wanted),
            store.raw(wanted),
            store.experiments_for_many([a for a in wanted if a.startswith("GSE")], partial=True),
        )

    whole = everything(open_store(root))
    monkeypatch.setattr(reader_module, "_BATCH", 3)
    assert everything(open_store(root)) == whole
