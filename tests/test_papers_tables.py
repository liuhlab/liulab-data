"""The paper store's four tables: written whole, through a temp file and a rename."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from labdata.papers import tables

PAPERS = [
    {
        "paper": "39637862",
        "pmid": 39637862,
        "pmcid": "PMC11700000",
        "doi": "10.1038/s41586-024-08350-8",
        "status": "fetched",
        "xml_tier": "pmc",
        "published": dt.date(2024, 12, 4),
    },
    {"paper": "10.1101/2024.01.15.575678", "doi": "10.1101/2024.01.15.575678", "status": "manual"},
]


def test_same_rows_give_the_same_file(tmp_path: Path) -> None:
    first = tables.write(tmp_path, tables.PAPERS, PAPERS)
    one = first.read_bytes()
    tables.write(tmp_path, tables.PAPERS, list(reversed(PAPERS)))
    assert first.read_bytes() == one
    assert [r["paper"] for r in tables.read(tmp_path, tables.PAPERS)] == [
        "10.1101/2024.01.15.575678",
        "39637862",
    ]
    assert tables.read(tmp_path, tables.PAPERS)[1]["published"] == dt.date(2024, 12, 4)


def test_a_write_cut_off_before_the_rename_keeps_the_old_table(tmp_path: Path) -> None:
    path = tables.write(tmp_path, tables.PAPERS, PAPERS)
    old = path.read_bytes()

    def rows() -> Iterator[dict[str, object]]:
        yield {"paper": "1", "status": "manual"}
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        tables.write(tmp_path, tables.PAPERS, rows())
    assert path.read_bytes() == old
    assert sorted(p.name for p in tmp_path.iterdir()) == ["papers.parquet"]


def test_a_missing_table_reads_empty_and_a_bad_row_is_refused(tmp_path: Path) -> None:
    assert tables.read(tmp_path, tables.FILES) == []
    with pytest.raises(ValueError, match="nope"):
        tables.write(tmp_path, tables.ATTEMPTS, [{"paper": "1", "nope": 1}])
    with pytest.raises(ValueError, match="twice"):
        tables.write(tmp_path, tables.PAPER_INFO, [{"paper": "1"}, {"paper": "1"}])


def test_a_key_lookup_touches_only_the_row_group_that_can_hold_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tables, "ROW_GROUP_ROWS", 2)
    rows = [{"paper": str(n), "kind": "xml", "name": f"{n}.xml"} for n in range(10, 16)]
    path = tables.write(tmp_path, tables.FILES, reversed(rows))
    fragment = next(ds.dataset(path).get_fragments())
    assert fragment.metadata.num_row_groups == 3
    held = fragment.subset(ds.field("paper") == "13").row_groups
    assert [g.id for g in held] == [1]
    assert tables.read(tmp_path, tables.FILES) == [
        {c: r.get(c) for c in tables.FILES.schema.names} for r in rows
    ]
