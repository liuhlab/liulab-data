"""Cut a paper store down to the papers a subset store's records link.

The source paper store is only read: its tables through Parquet filters and its paper files
as they are. This module takes no lock there, finishes no stopped save, and writes neither
its marker, its status record nor the per-user pointer file.
"""

from __future__ import annotations

import importlib.resources
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from labdata._files import read_toml_marker, temp_beside, toml_line, utc_stamp, write_atomic
from labdata._gcs import COPY_TO_KEY
from labdata.exceptions import LabdataError
from labdata.papers import paths, tables
from labdata.papers.reader import PaperReader
from labdata.papers.root import FORMAT_VERSION, MARKER, PAPERS_DIR, paper_key
from labdata.papers.status import read_status
from labdata.store import open_store
from labdata.store.layout import HOLDINGS
from labdata.store.reader import BuildReader, named_build
from labdata.store.subset import SUBSET_OF_KEY, check_new_root

#: The record types whose paper links choose the papers.
_LINKING = ("GEO Series", "BioProject")
#: The forward relations that link a paper.
_RELATIONS = ("PubMed", "DOI")
_PAPER_PREFIXES = ("PMID:", "DOI:")


@dataclass(frozen=True, slots=True)
class PaperSubset:
    """What one cut of a paper store wrote.

    Parameters
    ----------
    root : pathlib.Path
        The new paper store root.
    papers : int
        Papers kept.
    files : int
        Paper files copied.
    bytes : int
        The bytes written: the four tables, the paper files, the marker and the README.
    """

    root: Path
    papers: int
    files: int
    bytes: int


def subset(source: Path, store: Path, root: Path) -> PaperSubset:
    """Cut the paper store at ``source`` to the papers the store at ``store`` links.

    A paper is kept when a Series or BioProject that ``store``'s current build holds links
    it by ``PubMed`` or ``DOI``, found as :meth:`PaperReader.papers` finds it. Its rows of all
    four tables are kept, with the ``account`` of each ``attempts`` row left null, and every
    file its ``files`` rows name is copied to the same relative path. A rerun into the same
    root replaces the tables and deletes the paper files they no longer name.

    Parameters
    ----------
    source : pathlib.Path
        The source paper store's root; only read.
    store : pathlib.Path
        The root of the GEO/SRA store whose links choose the papers: a subset store.
    root : pathlib.Path
        The new paper store root: empty or missing, or a subset cut for the same build
        of ``store``.

    Returns
    -------
    PaperSubset
        What was written.

    Raises
    ------
    NotAStoreError
        If ``source`` is not a paper store, or ``store`` not a store with a build.
    StoreBuildError
        If ``root`` may not take the subset (see :func:`labdata.store.subset.check_new_root`).
    LabdataError
        If the source is saving its tables, a table changes during the read, or a ``files``
        row names a file the source does not hold. Run it again once the source's pass ends.
    """
    read_toml_marker(source, MARKER, "a paper store")
    opened = open_store(store)
    build = named_build(opened.root, opened.settings)
    check_new_root(root, source, MARKER, build.name, "paper store")

    before = _stamps(source)
    keys = _linked(source, build)
    rows = {table.name: _rows(source, table, keys) for table in tables.TABLES}
    read_at = utc_stamp()
    _unchanged(source, before)
    for row in rows[tables.ATTEMPTS.name]:
        row["account"] = None

    root.mkdir(parents=True, exist_ok=True)
    marker = root / MARKER
    if not marker.is_file():
        lines = [
            toml_line("format_version", FORMAT_VERSION),
            toml_line(SUBSET_OF_KEY, build.name),
            toml_line(COPY_TO_KEY, ""),
            toml_line("drive_remote", ""),
            toml_line("drive_folder", ""),
        ]
        write_atomic(marker, "\n".join(lines) + "\n")
    named = sorted({f["path"] for f in rows[tables.FILES.name] if f["path"]})
    for rel in named:
        _copy_file(source, root, rel)
    _unchanged(source, before)
    written = [tables.write(root, table, rows[table.name]) for table in tables.TABLES]
    _drop_unnamed(root, set(named))
    readme = paths.README.at(root)
    write_atomic(readme, _readme(source, build.name, read_at))
    kept = {r["paper"] for r in rows[tables.PAPERS.name]}
    return PaperSubset(
        root=root,
        papers=len(kept),
        files=len(named),
        bytes=sum(p.stat().st_size for p in (*written, *(root / n for n in named), marker, readme)),
    )


def _linked(source: Path, build: Path) -> set[str]:
    """Return the keys of the source's papers that the build's Series and BioProjects link."""
    reader = BuildReader(build)
    try:
        held = pq.read_table(
            HOLDINGS.path(build), columns=["accession"], filters=[("type", "in", _LINKING)]
        )
        linking = set(held.column(0).to_pylist())
        targets = {
            dst
            for src, dst, _relation, _source in (
                reader.links(linking, _RELATIONS) if linking else []
            )
            if src in linking and dst.startswith(_PAPER_PREFIXES) and _is_paper(dst)
        }
        found = PaperReader(source, reader).papers(paper_key(t) for t in targets)
    finally:
        reader.connection.close()
    return {paper.key for paper in found.values()}


def _is_paper(target: str) -> bool:
    try:
        paper_key(target)
    except ValueError:
        return False
    return True


def _rows(source: Path, table: tables.PaperTable, keys: Iterable[str]) -> list[dict[str, Any]]:
    """Return the rows of one source table whose paper is in ``keys``."""
    path = table.path(source)
    wanted = sorted(keys)
    if not wanted or not path.is_file():
        return []
    return pq.read_table(path, schema=table.schema, filters=[("paper", "in", wanted)]).to_pylist()


def _stamps(source: Path) -> dict[str, tuple[int, int, int] | None]:
    """Return each source table's inode, size and change time; ``None`` for a missing one.

    Raises
    ------
    LabdataError
        If the source is in the middle of saving its tables.
    """
    if (source / tables.SAVING).is_file():
        raise LabdataError(
            f"{source} is saving its tables ({tables.SAVING} is there); "
            "run this again once its pass ends"
        )
    out: dict[str, tuple[int, int, int] | None] = {}
    for table in tables.TABLES:
        path = table.path(source)
        if path.is_file():
            stat = path.stat()
            out[table.name] = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        else:
            out[table.name] = None
    return out


def _unchanged(source: Path, before: dict[str, tuple[int, int, int] | None]) -> None:
    """Raise if a source table is not the file it was when the read began."""
    now = _stamps(source)
    changed = sorted(name for name in before if now[name] != before[name])
    if changed:
        raise LabdataError(
            f"{source} changed during the read ({', '.join(changed)}); the subset is not "
            "whole. Run this again once its pass ends"
        )


def _copy_file(source: Path, root: Path, rel: str) -> None:
    """Copy one paper file to the same relative path, whole or not at all."""
    path = Path(rel)
    if path.is_absolute() or ".." in path.parts or path.parts[:1] != (PAPERS_DIR,):
        raise LabdataError(f"the files table names a path outside {PAPERS_DIR}/: {rel}")
    out = root / path
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(out)
    try:
        shutil.copyfile(source / path, tmp)
        tmp.replace(out)
    except FileNotFoundError:
        raise LabdataError(
            f"{source} does not hold {rel}, which its files table names; "
            "run this again once its pass ends"
        ) from None
    finally:
        tmp.unlink(missing_ok=True)


def _drop_unnamed(root: Path, named: set[str]) -> None:
    """Delete the paper files a rerun's tables no longer name, then the folders left empty."""
    folder = paths.PAPERS.at(root)
    if not folder.is_dir():
        return
    for path in sorted(folder.rglob("*"), reverse=True):
        if path.is_dir():
            if not any(path.iterdir()):
                path.rmdir()
        elif path.relative_to(root).as_posix() not in named:
            path.unlink()


def _readme(source: Path, build: str, read_at: str) -> str:
    """Return the subset's README: where it comes from, then the paper store's own README."""
    last = read_status(source).last_pass
    passed = (
        f"a {last.get('kind')} pass that started {last.get('start')}, ended "
        f"{last.get('end') or 'not yet'}, with outcome {last.get('outcome')}"
        if last
        else "not recorded"
    )
    packaged = importlib.resources.files("labdata.papers").joinpath("papers_readme.md")
    return (
        "# A subset of a labdata paper store\n\n"
        "This folder is a subset of a labdata paper store. It holds the papers that the Series "
        f"and BioProjects of store build `{build}` link, with every file the full paper store "
        "held for them.\n\n"
        f"Its tables were read from the full paper store at {read_at}. The full paper store's "
        f"last pass then: {passed}.\n\n"
        "The marker names no copy target and no Drive, so no pass or copy run here reaches "
        "the full paper store's bucket. The `account` column of `attempts` is empty. To take "
        "newer papers, run `labdata papers subset` again.\n\n"
        "What follows is the README of a full paper store, which describes the tables and "
        "the paper folders.\n\n" + packaged.read_text(encoding="utf-8")
    )
