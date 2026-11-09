"""The paper reader: a paper in one call, and the papers a Series or BioProject links."""

from __future__ import annotations

import datetime as dt
import io
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labdata.exceptions import NotMirrored
from labdata.papers import init_paper_store, open_paper_store, tables
from labdata.papers.reader import Author, PaperReader
from tests._slices import built_root


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


def paper_store(tmp_path: Path, slice_name: str) -> PaperReader:
    store = built_root(tmp_path / "store", slice_name)
    root = tmp_path / "papers"
    init_paper_store(root, store)
    tables.write(
        root,
        tables.PAPERS,
        [
            {"paper": "40167329", "pmid": 40167329, "status": "fetched", "xml_tier": "pmc"},
            {"paper": "38114495", "pmid": 38114495, "status": "waiting", "reason": "pmc-embargo"},
            {"paper": "30115783", "pmid": 30115783, "status": "manual", "reason": "no-route"},
        ],
    )
    tables.write(
        root,
        tables.PAPER_INFO,
        [
            {
                "paper": "40167329",
                "pmid": 40167329,
                "title": "A cell atlas",
                "published": dt.date(2025, 4, 1),
                "authors": [{"name": "Liu H", "affiliations": ["Lab A", "Lab B"]}],
                "publication_type": ["Journal Article"],
            }
        ],
    )
    tables.write(
        root,
        tables.FILES,
        [
            {
                "paper": "40167329",
                "kind": "xml",
                "name": "40167329.xml",
                "path": "papers/40167nnn/40167329/40167329.xml",
                "tier": "pmc",
            },
            {"paper": "40167329", "kind": "supplement", "name": "big.zip", "path": None},
        ],
    )
    return open_paper_store(root).reader


def test_paper_gives_details_status_and_files(tmp_path: Path) -> None:
    reader = paper_store(tmp_path, "geo_slice")
    paper = reader.paper("PMID:40167329")
    assert (paper.key, paper.status, paper.xml_tier) == ("40167329", "fetched", "pmc")
    assert paper.info is not None
    assert paper.info.title == "A cell atlas"
    assert paper.info.authors == (Author("Liu H", ("Lab A", "Lab B")),)
    assert [(f.kind, f.name) for f in paper.files] == [
        ("supplement", "big.zip"),
        ("xml", "40167329.xml"),
    ]
    assert paper.files[1].path == tmp_path / "papers/papers/40167nnn/40167329/40167329.xml"
    assert paper.files[0].path is None
    assert reader.paper("38114495").info is None
    with pytest.raises(NotMirrored, match="12345678"):
        reader.paper("12345678")


def test_a_batch_lookup_reads_each_table_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = paper_store(tmp_path, "geo_slice")
    read = []
    real = pq.read_table

    def counted(path: Path, **kwargs: Any) -> pa.Table:
        read.append(Path(path).name)
        return real(path, **kwargs)

    monkeypatch.setattr(pq, "read_table", counted)
    found = reader.papers(["40167329", "PMID:38114495", "30115783", "12345678"])
    assert sorted(read) == ["files.parquet", "paper_info.parquet", "papers.parquet"]
    assert list(found) == ["40167329", "38114495", "30115783"]
    assert found["40167329"] == reader.paper("40167329")
    assert [f.name for f in found["40167329"].files] == ["big.zip", "40167329.xml"]
    assert (found["30115783"].status, found["30115783"].files) == ("manual", ())


class _TrackedFile(io.RawIOBase):
    """A local file that records the byte ranges read from it."""

    def __init__(self, path: Path, ranges: list[tuple[int, int]]) -> None:
        self._fh = path.open("rb")
        self._ranges = ranges

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._fh.seek(offset, whence)

    def tell(self) -> int:
        return self._fh.tell()

    def read(self, size: int = -1) -> bytes:
        start = self._fh.tell()
        data = self._fh.read(size)
        self._ranges.append((start, start + len(data)))
        return data

    def close(self) -> None:
        self._fh.close()
        super().close()


def row_groups_read(path: Path, ranges: list[tuple[int, int]]) -> set[int]:
    """Return the row groups of ``path`` whose column data any of ``ranges`` touches.

    A read that ends at the end of the file is the footer read, which on a small file spans
    every row group without decoding any, so it is left out.
    """
    meta = pq.ParquetFile(path).metadata
    size = path.stat().st_size
    ranges = [(a, b) for a, b in ranges if b != size]
    touched = set()
    for i in range(meta.num_row_groups):
        for j in range(meta.num_columns):
            col = meta.row_group(i).column(j)
            start = col.dictionary_page_offset or col.data_page_offset
            end = start + col.total_compressed_size
            if any(a < end and start < b for a, b in ranges):
                touched.add(i)
    return touched


def test_a_lookup_decodes_only_the_row_groups_that_can_hold_its_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tables, "ROW_GROUP_ROWS", 1)
    reader = paper_store(tmp_path, "geo_slice")
    ranges: dict[str, list[tuple[int, int]]] = {}
    real = pq.read_table

    def tracked(path: Path, **kwargs: Any) -> pa.Table:
        source = pa.PythonFile(_TrackedFile(Path(path), ranges.setdefault(Path(path).name, [])))
        return real(source, **kwargs)

    monkeypatch.setattr(pq, "read_table", tracked)
    assert reader.paper("38114495").status == "waiting"

    root = tmp_path / "papers"
    assert pq.ParquetFile(tables.PAPERS.path(root)).metadata.num_row_groups == 3
    # Papers sort by key, one per row group: 30115783, 38114495, 40167329.
    assert row_groups_read(tables.PAPERS.path(root), ranges["papers.parquet"]) == {1}
    assert row_groups_read(tables.PAPER_INFO.path(root), ranges["paper_info.parquet"]) == set()
    assert row_groups_read(tables.FILES.path(root), ranges["files.parquet"]) == set()


def test_series_papers_joined_from_the_build(tmp_path: Path) -> None:
    reader = paper_store(tmp_path, "geo_slice")
    found = reader.papers_for(["GSE246310", "GSE233931", "GSE28770", "GSE153830"])
    fetched, waiting, unlinked, unsynced = found.values()
    assert [(p.key, p.status) for p in fetched.papers] == [("40167329", "fetched")]
    assert fetched.fetched
    assert [(p.key, p.status, p.reason) for p in waiting.papers] == [
        ("38114495", "waiting", "pmc-embargo")
    ]
    assert waiting.linked
    assert not waiting.fetched
    assert not unlinked.linked
    assert unlinked.papers == ()
    # Linked in the build, but not yet taken in by the paper store.
    assert [(p.key, p.status) for p in unsynced.papers] == [("33281976", None)]


def test_bioproject_no_series_names(tmp_path: Path) -> None:
    reader = paper_store(tmp_path, "sra_slice")
    linked, unlinked = reader.papers_for(["PRJNA436361", "PRJNA530347"]).values()
    assert [(p.key, p.status, p.reason) for p in linked.papers] == [
        ("30115783", "manual", "no-route")
    ]
    assert not unlinked.linked
