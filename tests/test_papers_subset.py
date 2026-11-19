"""A paper store cut down to the papers a subset store links, read back against its source."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import NotMirrored
from labdata.papers import open_paper_store, tables
from labdata.papers.reader import Paper, PaperReader
from labdata.papers.root import pointer_path
from labdata.store.reader import BuildReader
from tests._paper_fakes import fixture_build, init_papers
from tests._slices import BuiltStore
from tests.test_store_subset import LISTED, cut, tree

#: The papers the listed Series link, and one only an unlisted Series links.
LINKED = ["38114495", "40167329"]
UNLINKED = "20194744"
XML = "papers/40167nnn/40167329/40167329.xml"
PDF = "papers/38114nnn/38114495/38114495.pdf"

PAPERS: list[dict[str, Any]] = [
    {"paper": "40167329", "pmid": 40167329, "doi": "10.1/atlas", "status": "fetched"},
    {"paper": "38114495", "pmid": 38114495, "status": "fetched", "pdf_tier": "openalex"},
    {"paper": UNLINKED, "pmid": 20194744, "status": "manual", "reason": "no-route"},
]
INFO: list[dict[str, Any]] = [
    {"paper": "40167329", "pmid": 40167329, "title": "A cell atlas"},
    {"paper": UNLINKED, "pmid": 20194744, "title": "Not linked"},
]
FILES: list[dict[str, Any]] = [
    {"paper": "40167329", "kind": "xml", "name": "40167329.xml", "path": XML, "tier": "pmc"},
    {"paper": "40167329", "kind": "supplement", "name": "big.zip", "path": None},
    {"paper": "38114495", "kind": "pdf", "name": "38114495.pdf", "path": PDF},
    {
        "paper": UNLINKED,
        "kind": "pdf",
        "name": "20194744.pdf",
        "path": "papers/20194nnn/20194744/20194744.pdf",
    },
]
ATTEMPTS: list[dict[str, Any]] = [
    {
        "paper": "38114495",
        "tier": "openalex",
        "date": dt.date(2026, 11, 2),
        "account": "someone@example.org",
        "outcome": "fetched",
    },
    {"paper": UNLINKED, "tier": "pmc", "date": dt.date(2026, 11, 2), "outcome": "absent"},
]
LAST_PASS = {
    "kind": "daily",
    "start": "2026-11-02T00:30:00Z",
    "end": "2026-11-02T00:41:00Z",
    "outcome": "success",
}


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


@pytest.fixture(scope="module")
def stores(built_store: BuiltStore, tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """The geo_slice store, and the small store cut from it to ``LISTED``."""
    folder = tmp_path_factory.mktemp("stores")
    source = built_store(folder / "store", "geo_slice")
    code, output = cut(folder, source, LISTED, folder / "small")
    assert code == 0, output
    return source, folder / "small"


def paper_store(root: Path, store: Path, papers: list[dict[str, Any]] = PAPERS) -> Path:
    """Make a paper store at ``root`` over ``store``, holding the rows above and their files."""
    init_papers(root, store, copy_to="gcs:bucket/papers", drive_remote="drive", drive_folder="f")
    for table, rows in zip(tables.TABLES, (papers, INFO, FILES, ATTEMPTS), strict=True):
        tables.write(root, table, rows)
    for row in FILES:
        if row["path"]:
            (root / row["path"]).parent.mkdir(parents=True)
            (root / row["path"]).write_bytes(f"the bytes of {row['name']}".encode())
    (root / "paper-status.json").write_text(json.dumps({"last_pass": LAST_PASS}), encoding="utf-8")
    return root


def cut_papers(source: Path, store: Path, root: Path) -> tuple[int, str]:
    """Run ``labdata papers subset``; return its exit code and output."""
    result = CliRunner().invoke(
        app, ["papers", "subset", "--from", str(source), "--store", str(store), "--root", str(root)]
    )
    return result.exit_code, result.output


def placed(paper: Paper, root: Path) -> Paper:
    """Return ``paper`` with each file's path made relative to its paper store ``root``."""
    files = tuple(
        f if f.path is None else replace(f, path=f.path.relative_to(root)) for f in paper.files
    )
    return replace(paper, files=files)


def test_the_small_stores_answer_as_their_sources_for_the_listed_series(
    stores: tuple[Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, small = stores
    source = paper_store(tmp_path / "papers", store)
    pointer = pointer_path().read_bytes()
    before = tree(source)

    code, output = cut_papers(source, small, tmp_path / "few")

    assert code == 0, output
    assert tree(source) == before
    assert pointer_path().read_bytes() == pointer
    full = PaperReader(source, BuildReader(store / "builds" / "b1"))
    monkeypatch.setenv("LABDATA_STORE", str(small))
    monkeypatch.setenv("LABDATA_PAPERS", str(tmp_path / "few"))
    opened = open_paper_store()
    reader = opened.reader
    assert reader.papers_for(LISTED) == full.papers_for(LISTED)
    assert [p.key for links in reader.papers_for(LISTED).values() for p in links.papers] == LINKED
    for key in LINKED:
        paper = reader.paper(key)
        assert placed(paper, opened.root) == placed(full.paper(key), source)
        for file in (f for f in paper.files if f.path is not None):
            with file.open() as fh:
                assert fh.read() == f"the bytes of {file.name}".encode()
    with pytest.raises(NotMirrored):
        reader.paper(UNLINKED)
    assert not (tmp_path / "few" / "papers" / "20194nnn").exists()
    # The job's own table comes too, without the account that paid.
    assert tables.read(opened.root, tables.ATTEMPTS) == [
        {**row, "account": None}
        for row in tables.read(source, tables.ATTEMPTS)
        if row["paper"] in LINKED
    ]
    assert opened.settings == {
        "format_version": 1,
        "subset_of": "b1-subset",
        "copy_to": "",
        "drive_remote": "",
        "drive_folder": "",
    }
    written = sum(p.stat().st_size for p in (tmp_path / "few").rglob("*") if p.is_file())
    assert output.splitlines()[1:] == ["papers: 2", "files: 2", f"Wrote {written} bytes"]
    readme = (tmp_path / "few" / "README.md").read_text(encoding="utf-8")
    assert LAST_PASS["end"] in readme
    assert "Its tables were read from the full paper store at 20" in readme
    for path in (p for p in (tmp_path / "few").rglob("*") if p.is_file()):
        data = path.read_bytes()
        assert str(tmp_path).encode() not in data, path
        assert b"someone@example.org" not in data, path
        assert b"gcs:bucket" not in data, path


def test_a_linked_doi_finds_the_paper_that_lists_it_as_another_doi(tmp_path: Path) -> None:
    store = fixture_build(tmp_path / "store", [("GSE1", "DOI:10.1101/preprint", "SOFT")])
    aliased = {**PAPERS[0], "other_dois": ["10.1101/preprint"]}
    source = paper_store(tmp_path / "papers", store, [aliased, *PAPERS[1:]])

    code, output = cut_papers(source, store, tmp_path / "few")

    assert code == 0, output
    assert [r["paper"] for r in tables.read(tmp_path / "few", tables.PAPERS)] == ["40167329"]
    assert [r["paper"] for r in tables.read(tmp_path / "few", tables.PAPER_INFO)] == ["40167329"]
    assert (tmp_path / "few" / XML).is_file()
    assert not (tmp_path / "few" / PDF).exists()


def test_a_rerun_into_the_same_root_drops_what_is_no_longer_linked(
    stores: tuple[Path, Path], tmp_path: Path
) -> None:
    store, small = stores
    source = paper_store(tmp_path / "papers", store)
    assert cut_papers(source, small, tmp_path / "few")[0] == 0
    # The paper leaves the source, as when its DOI moved to another paper.
    for table in tables.TABLES:
        tables.write(
            source, table, [r for r in tables.read(source, table) if r["paper"] != "38114495"]
        )

    code, output = cut_papers(source, small, tmp_path / "few")

    assert code == 0, output
    assert [r["paper"] for r in tables.read(tmp_path / "few", tables.PAPERS)] == ["40167329"]
    assert sorted(
        p.relative_to(tmp_path / "few").as_posix()
        for p in (tmp_path / "few" / "papers").rglob("*")
        if p.is_file()
    ) == [XML]


def test_a_source_that_is_saving_or_changes_during_the_read_is_refused(
    stores: tuple[Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, small = stores
    source = paper_store(tmp_path / "papers", store)
    (source / tables.SAVING).write_text("[]", encoding="utf-8")
    before = tree(source)

    code, output = cut_papers(source, small, tmp_path / "few")

    assert code == 1
    assert "is saving its tables" in output
    assert tree(source) == before
    assert not (tmp_path / "few").exists()

    # A pass saves the files table while the cut reads.
    (source / tables.SAVING).unlink()
    found = PaperReader.papers

    def saved_meanwhile(self: PaperReader, keys: Any) -> dict[str, Paper]:
        out = found(self, keys)
        tables.write(source, tables.FILES, tables.read(source, tables.FILES))
        return out

    monkeypatch.setattr(PaperReader, "papers", saved_meanwhile)
    code, output = cut_papers(source, small, tmp_path / "few")

    assert code == 1
    assert "changed during the read (files)" in output
    assert not (tmp_path / "few").exists()


@pytest.mark.parametrize(
    ("root", "message"),
    [
        ("papers", "apart from its source"),
        ("taken", "not empty"),
        ("other", "is a subset of b0, not a subset of b1-subset"),
    ],
)
def test_a_root_that_is_not_a_fresh_or_matching_subset_is_refused(
    stores: tuple[Path, Path], tmp_path: Path, root: str, message: str
) -> None:
    store, small = stores
    source = paper_store(tmp_path / "papers", store)
    (tmp_path / "taken").mkdir()
    (tmp_path / "taken" / "notes.txt").write_text("mine", encoding="utf-8")
    (tmp_path / "other").mkdir()
    (tmp_path / "other" / "labdata-papers.toml").write_text(
        'format_version = 1\nsubset_of = "b0"\n', encoding="utf-8"
    )
    before = tree(tmp_path)

    code, output = cut_papers(source, small, tmp_path / root)

    assert code == 1
    assert message in output
    assert tree(tmp_path) == before
