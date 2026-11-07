"""The paper reader: a paper in one call, and the papers a Series or BioProject links."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

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
