"""Cut a build down to a list of Series: a subset build, in a store root of its own.

The rows are copied from one build of a source store, chosen by walking links forward from
the listed Series. Nothing is parsed again, and the new root holds no ``raw/``, ``snapshots/``
or ``extracts/``. The source store is only read: this module opens its marker, its build's
manifest and its build's tables, and writes nothing under its root.
"""

from __future__ import annotations

import importlib.resources
import json
import shutil
import string
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from labdata._files import read_toml_marker, temp_beside, write_atomic
from labdata.exceptions import AccessionError, StoreBuildError
from labdata.store import layout, paths, provenance
from labdata.store.layout import (
    GAPS,
    HOLDINGS,
    KNOWN,
    LINKS_BY_FROM,
    LINKS_BY_TO,
    NOTES,
    RECORDS,
    SUPPLEMENTARY_FILES,
    Table,
    sql_ident,
    sql_string,
)
from labdata.store.model import kind_of
from labdata.store.reader import BuildReader, build_path, named_build
from labdata.store.root import FORMAT_VERSION

if TYPE_CHECKING:
    import duckdb

#: The marker key of a subset's root naming the build it was cut from, by ID, never by path.
SUBSET_OF_KEY = "subset_of"

#: The forward relations the walk follows from a held record. No reverse relation is followed.
FOLLOWED = ("sample", "platform", "SRA", "run", "study", "SRA sample", "BioSample", "BioProject")
#: The relation whose ``from`` end is a held BioProject's parent; the parent's record is held,
#: and the walk does not go on from it.
_UMBRELLA = "umbrella of"
#: The forward relations from a listed Series to another Series, which the walk does not follow.
_TO_SERIES = ("SuperSeries of", "Reanalyzed by")
#: The record types whose paper links keep the paper's link rows.
_LINK_PAPERS = ("GEO Series", "BioProject")
_SERIES = "GEO Series"
_PAPER_PREFIXES = ("PMID:", "DOI:")

#: Keys per query; each query names its keys literally, so DuckDB skips row groups by range.
_BATCH = 10_000

_LINK_COLUMNS = ", ".join(sql_ident(c) for c in LINKS_BY_FROM.column_names)


@dataclass(frozen=True, slots=True)
class Subset:
    """What one cut wrote.

    Parameters
    ----------
    build : pathlib.Path
        The subset build's folder.
    source_build : str
        The ID of the build it was cut from.
    records : dict of str to int
        Records held, by record type; a type with none is left out.
    not_held : tuple of str
        The listed Series the source build does not hold.
    outside : tuple of (str, str, str)
        Each link from a listed Series to an unlisted one: the listed Series, the relation
        as read from it, and the unlisted Series.
    bytes : int
        The bytes written: the build folder, the marker and the root README.
    """

    build: Path
    source_build: str
    records: dict[str, int]
    not_held: tuple[str, ...]
    outside: tuple[tuple[str, str, str], ...]
    bytes: int


def read_series(path: Path) -> list[str]:
    """Return the Series a list file names: one accession a line, blank lines left out.

    Raises
    ------
    AccessionError
        If a line is not a GEO Series accession.
    """
    return _listed(path.read_text(encoding="utf-8").splitlines())


def check_new_root(root: Path, source: Path, marker: str, subset_of: str, what: str) -> bool:
    """Check that a subset may be written into ``root``; return whether it holds one already.

    Parameters
    ----------
    root : pathlib.Path
        The new root.
    source : pathlib.Path
        The root the subset is read from.
    marker : str
        The marker file's name in a root of this kind.
    subset_of : str
        What a subset's marker in ``root`` must name under ``SUBSET_OF_KEY``.
    what : str
        The kind of root, for messages, such as ``"store"``.

    Raises
    ------
    StoreBuildError
        If ``root`` is the source or lies inside it, holds a marker that is not this
        subset's, or holds anything else.
    """
    new, old = root.resolve(), source.resolve()
    if new == old or old in new.parents:
        raise StoreBuildError(
            f"a subset needs a root apart from its source {what}, not {root}; "
            "the source is only read"
        )
    if (root / marker).is_file():
        named = read_toml_marker(root, marker, f"a {what}").get(SUBSET_OF_KEY)
        if named != subset_of:
            was = "is not a subset" if named is None else f"is a subset of {named}"
            raise StoreBuildError(
                f"{root} holds a {what} that {was}, not a subset of {subset_of}; "
                "pick an empty folder"
            )
        return True
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise StoreBuildError(f"{root} is not empty and is not a subset; pick an empty folder")
    return False


def subset(source: Path, series: Iterable[str], root: Path, build_id: str | None = None) -> Subset:
    """Cut the source store's current build to ``series`` and what they link forward to.

    The new build holds the listed Series the source holds, and every record reached from
    them over ``FOLLOWED``, step after step, with the parent of each held BioProject. It
    keeps every link row with a held record at an end, and every link row of a paper that
    a held Series or BioProject links. A build of the same ID already in ``root`` is replaced.

    Parameters
    ----------
    source : pathlib.Path
        The source store's root; only read.
    series : iterable of str
        The GEO Series accessions to keep.
    root : pathlib.Path
        The new root: empty or missing, or a subset of the same source build.
    build_id : str or None
        The new build's ID; defaults to ``<source build ID>-subset``.

    Returns
    -------
    Subset
        What was written.

    Raises
    ------
    AccessionError
        If an accession is not a GEO Series.
    NotAStoreError
        If ``source`` is not a store, or names no build.
    StoreBuildError
        If no Series is listed, the build ID is not a folder name, the source's build is
        missing, or ``root`` may not take the subset (see :func:`check_new_root`).
    """
    listed = _listed(series)
    if not listed:
        raise StoreBuildError("the Series list is empty")
    src = named_build(source, paths.read_marker(source))
    if not src.is_dir():
        raise StoreBuildError(f"the source store's build {src.name} is missing: {src}")
    build_id = build_id or f"{src.name}-subset"
    if Path(build_id).name != build_id or build_id.startswith("."):
        raise StoreBuildError(f"not a build folder name: {build_id!r}")
    check_new_root(root, source, paths.MARKER.rel, src.name, "store")

    out = build_path(root, build_id)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(out)
    shutil.rmtree(tmp, ignore_errors=True)
    RECORDS.path(tmp).mkdir(parents=True)
    reader = BuildReader(src)
    try:
        held, outside = _write_tables(reader.connection, src, tmp, listed)
        not_held = [a for a in listed if a not in held]
        manifest = layout.manifest_path(src)
        provenance.document_subset(
            tmp,
            build_id,
            src.name,
            json.loads(manifest.read_text(encoding="utf-8")) if manifest.is_file() else {},
            series=listed,
            not_held=not_held,
            outside=[{"series": a, "relation": r, "other": b} for a, r, b in outside],
        )
        shutil.rmtree(out, ignore_errors=True)
        tmp.rename(out)
    finally:
        reader.connection.close()
        shutil.rmtree(tmp, ignore_errors=True)

    if not paths.MARKER.at(root).is_file():
        paths.write_marker(root, {"format_version": FORMAT_VERSION, SUBSET_OF_KEY: src.name})
    paths.switch(root, build_id)
    readme = _write_readme(root, out, src.name, len(listed))
    counts: dict[str, int] = {}
    for kind in held.values():
        counts[kind] = counts.get(kind, 0) + 1
    written = [p for p in out.rglob("*") if p.is_file()] + [paths.MARKER.at(root), readme]
    return Subset(
        build=out,
        source_build=src.name,
        records=dict(sorted(counts.items())),
        not_held=tuple(not_held),
        outside=tuple(outside),
        bytes=sum(p.stat().st_size for p in written),
    )


def _listed(series: Iterable[str]) -> list[str]:
    """Return the accessions upper-cased, each once, in order; every one must be a Series."""
    out = list(dict.fromkeys(a.strip().upper() for a in series if a.strip()))
    for accession in out:
        if kind_of(accession) != _SERIES:
            raise AccessionError(f"not a GEO Series accession: {accession!r}")
    return out


def _write_tables(
    con: duckdb.DuckDBPyConnection, src: Path, out: Path, listed: list[str]
) -> tuple[dict[str, str], list[tuple[str, str, str]]]:
    """Write every table of the subset into ``out``.

    Returns each held accession's record type, and the links from a listed Series to an
    unlisted one.
    """
    held = _walk(con, src, listed)
    for kind in sorted(set(held.values())):
        accessions = [a for a, k in held.items() if k == kind]
        _copy(
            con, RECORDS, layout.records_path(src, kind), layout.records_path(out, kind), accessions
        )
    _copy(con, HOLDINGS, HOLDINGS.path(src), HOLDINGS.path(out), held)
    _copy(
        con,
        SUPPLEMENTARY_FILES,
        SUPPLEMENTARY_FILES.path(src),
        SUPPLEMENTARY_FILES.path(out),
        held,
        "owner",
    )

    ends = _kept_links(con, src, held)
    for table in (LINKS_BY_FROM, LINKS_BY_TO):
        layout.write(con, table, table.path(out), "SELECT DISTINCT * FROM kept")
    series = set(listed)
    outside = sorted(
        {
            (a, relation, b) if a in series else (b, "SubSeries of", a)
            for a, b, relation in con.execute(
                'SELECT "from", "to", relation FROM kept WHERE relation IN '
                f"({', '.join(sql_string(r) for r in _TO_SERIES)})"
            ).fetchall()
            if (a in series) != (b in series)
            and (a in series or relation == "SuperSeries of")
            and (b if a in series else a).startswith("GSE")
        }
    )

    _copy(con, KNOWN, KNOWN.path(src), KNOWN.path(out), {*held, *series, *ends})
    # The gaps sort by Series number, which a batch of accessions sorted as text is not in.
    _copy(con, GAPS, GAPS.path(src), GAPS.path(out), listed, batch=len(listed))
    notes = NOTES.path(src)
    layout.write(
        con,
        NOTES,
        NOTES.path(out),
        f"SELECT {_as_text(NOTES)} FROM {_file(notes)} WHERE accession IN "
        f"(SELECT accession FROM {_file(KNOWN.path(out))})"
        if notes.is_file()
        else _empty(NOTES),
    )
    return held, outside


def _walk(con: duckdb.DuckDBPyConnection, src: Path, listed: list[str]) -> dict[str, str]:
    """Return every record the subset holds, with its type: the walk from the listed Series."""
    followed = ", ".join(sql_string(r) for r in FOLLOWED)
    held = _held(con, src, listed)
    asked = set(listed)
    frontier = set(held)
    while frontier:
        reached = {
            row[0]
            for row in _rows(
                con,
                f'SELECT DISTINCT "to" FROM {_file(LINKS_BY_FROM.path(src))}',
                "from",
                frontier,
                f" AND relation IN ({followed})",
            )
        } - asked
        asked |= reached
        # No followed relation leads to a Series; were one to, the list still decides.
        new = {a: k for a, k in _held(con, src, reached).items() if k != _SERIES}
        held |= new
        frontier = set(new)
    projects = [a for a, kind in held.items() if kind == "BioProject"]
    parents = {
        row[0]
        for row in _rows(
            con,
            f'SELECT DISTINCT "from" FROM {_file(LINKS_BY_TO.path(src))}',
            "to",
            projects,
            f" AND relation = {sql_string(_UMBRELLA)}",
        )
    }
    held |= {a: k for a, k in _held(con, src, parents - set(held)).items() if k == "BioProject"}
    return dict(sorted(held.items()))


def _held(con: duckdb.DuckDBPyConnection, src: Path, accessions: Collection[str]) -> dict[str, str]:
    """Return which of ``accessions`` the source build holds, each with its record type."""
    select = f"SELECT accession, type FROM {_file(HOLDINGS.path(src))}"
    return {str(a): str(kind) for a, kind in _rows(con, select, "accession", accessions)}


def _kept_links(con: duckdb.DuckDBPyConnection, src: Path, held: dict[str, str]) -> set[str]:
    """Fill the temp table ``kept`` with the subset's link rows; return their record ends.

    A row is kept when an end is a held record, or a paper a held Series or BioProject links.
    """
    by_from, by_to = _file(LINKS_BY_FROM.path(src)), _file(LINKS_BY_TO.path(src))
    papers = " OR ".join(f'starts_with("to", {sql_string(p)})' for p in _PAPER_PREFIXES)
    linking = [a for a, kind in held.items() if kind in _LINK_PAPERS]
    ends = {
        *held,
        *(
            row[0]
            for row in _rows(
                con, f'SELECT DISTINCT "to" FROM {by_from}', "from", linking, f" AND ({papers})"
            )
        ),
    }
    con.execute(
        "CREATE OR REPLACE TEMP TABLE kept AS "
        f"SELECT {_LINK_COLUMNS} FROM ({LINKS_BY_FROM.empty()})"
    )
    for file, end in ((by_from, "from"), (by_to, "to")):
        for query in _queries(f"SELECT {_LINK_COLUMNS} FROM {file}", end, ends):
            con.execute(f"INSERT INTO kept {query}")
    rows = con.execute('SELECT "from" FROM kept UNION SELECT "to" FROM kept').fetchall()
    return {str(a) for (a,) in rows if not str(a).startswith(_PAPER_PREFIXES)}


def _copy(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    src: Path,
    out: Path,
    keys: Collection[str],
    key: str = "accession",
    *,
    batch: int = _BATCH,
) -> None:
    """Write ``table`` to ``out`` from the rows of ``src`` whose ``key`` is in ``keys``.

    The file's key-value metadata is copied. A missing ``src`` gives a table of no rows.
    """
    import pyarrow.parquet as pq

    if not src.is_file():
        layout.write(con, table, out, _empty(table))
        return
    stored = pq.read_metadata(src).metadata or {}
    metadata = {
        k.decode(): v.decode()
        for k, v in stored.items()
        if not k.startswith((b"ARROW:", b"pandas"))
    }
    select = f"SELECT {_as_text(table)} FROM {_file(src)}"
    queries = _queries(select, key, keys, batch=batch) or [_empty(table)]
    layout.write(con, table, out, queries, metadata or None)


def _queries(
    select: str, key: str, keys: Collection[str], tail: str = "", *, batch: int = _BATCH
) -> list[str]:
    """Return ``select`` once per sorted batch of ``keys``, each naming its keys literally."""
    ordered = sorted(set(keys))
    return [
        f"{select} WHERE {sql_ident(key)} IN "
        f"({', '.join(sql_string(k) for k in ordered[start : start + batch])}){tail}"
        for start in range(0, len(ordered), max(batch, 1))
    ]


def _rows(
    con: duckdb.DuckDBPyConnection, select: str, key: str, keys: Collection[str], tail: str = ""
) -> list[tuple[Any, ...]]:
    return [
        row for query in _queries(select, key, keys, tail) for row in con.execute(query).fetchall()
    ]


def _as_text(table: Table) -> str:
    """Return the table's columns as a select list, its dates as the text the writer parses."""
    return ", ".join(
        f"CAST({sql_ident(name)} AS VARCHAR) AS {sql_ident(name)}"
        if kind in (layout.DATE, layout.TIMESTAMP)
        else sql_ident(name)
        for name, kind in table.columns
    )


def _empty(table: Table) -> str:
    return f"SELECT {_as_text(table)} FROM ({table.empty()})"


def _file(path: Path) -> str:
    return f"read_parquet({sql_string(str(path))})"


def _write_readme(root: Path, build: Path, source_build: str, series: int) -> Path:
    """Write the subset root's README from the packaged ``subset_readme.md``."""
    manifest = json.loads(layout.manifest_path(build).read_text(encoding="utf-8"))
    cut = manifest["subset"]
    made = manifest["produced_by"]
    template = importlib.resources.files("labdata.store").joinpath("subset_readme.md")
    text = string.Template(template.read_text("utf-8")).substitute(
        build=build.name,
        source_build=source_build,
        source_commit=cut["source_commit"] or "unknown",
        snapshots=", ".join(f"{name} {date}" for name, date in cut["snapshot_dates"].items())
        or "not recorded",
        series=series,
        cut=manifest["date"],
        command=made["command"],
        commit=made["commit"] or "unknown",
        version=made["version"],
    )
    out = paths.README.at(root)
    write_atomic(out, text)
    return out
