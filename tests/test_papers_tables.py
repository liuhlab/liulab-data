"""The paper store's four tables: written whole, through a temp file and a rename."""

from __future__ import annotations

import datetime as dt
import os
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import pytest

from labdata.papers import paper_dir, tables

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


class Killed(BaseException):
    """The process is gone: nothing after it runs, no ``except Exception`` included."""


class Kill:
    """Kill a step at its ``at``-th rename or delete, the steps a save is made of."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, at: int) -> None:
        self.left = at
        for name in ("replace", "unlink"):
            monkeypatch.setattr(os, name, self._step(getattr(os, name)))

    def _step(self, real: Callable[..., Any]) -> Callable[..., Any]:
        def step(*args: Any, **kw: Any) -> Any:
            self.left -= 1
            if self.left <= 0:
                raise Killed
            return real(*args, **kw)

        return step


def test_a_rekey_killed_at_any_point_heals_on_the_next_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    doi, pmid, today = "10.1000/rekey", "41000005", dt.date(2026, 11, 7)
    old = (paper_dir(doi) / "10.1000%2Frekey.pdf").as_posix()
    new = (paper_dir(pmid) / f"{pmid}.pdf").as_posix()
    seed = tmp_path / "seed"
    (seed / old).parent.mkdir(parents=True)
    (seed / old).write_bytes(b"%PDF")
    tables.write(seed, tables.PAPERS, [{"paper": doi, "doi": doi}])
    row = {"paper": doi, "kind": "pdf", "name": "10.1000%2Frekey.pdf", "path": old}
    tables.write(seed, tables.FILES, [row])
    tables.write(seed, tables.ATTEMPTS, [{"paper": doi, "tier": "host", "outcome": "fetched"}])

    def rekey(root: Path) -> None:
        state = tables.PaperTables.load(root, today)
        if doi in state.papers:
            state.rekey(doi, pmid, doi)
            state.save(today)

    def on_disk(root: Path) -> set[str]:
        return {p.relative_to(root).as_posix() for p in (root / "papers").rglob("*") if p.is_file()}

    healed_to = set()
    for at in range(1, 100):
        root = tmp_path / str(at)
        shutil.copytree(seed, root)
        with monkeypatch.context() as m:
            Kill(m, at)
            try:
                rekey(root)
            except Killed:
                pass
            else:
                break
        # A kill never leaves the files table naming a file the disk lacks.
        assert {f["path"] for f in tables.read(root, tables.FILES)} <= on_disk(root)
        state = tables.PaperTables.load(root, today)
        assert {f["paper"] for f in state.files} <= set(state.papers)
        assert on_disk(root) == state.named()
        healed_to.add(next(iter(state.papers)))
        rekey(root)
        assert [r["paper"] for r in tables.read(root, tables.PAPERS)] == [pmid]
        assert [(f["paper"], f["path"]) for f in tables.read(root, tables.FILES)] == [(pmid, new)]
        assert [a["paper"] for a in tables.read(root, tables.ATTEMPTS)] == [pmid]
        assert on_disk(root) == {new}
    # Kills fell both before the save took and after it.
    assert healed_to == {doi, pmid}
