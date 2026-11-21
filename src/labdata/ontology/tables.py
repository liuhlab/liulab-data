"""The one writer of a snapshot's term tables, and the terms it takes as input.

Every reader of a source file, and the tiny-store builder, hands :func:`write_snapshot` a
:class:`SnapshotData`; nothing else creates a snapshot's tables. The tables live in one SQLite
file per snapshot, ``snapshots/<vocabulary>-<release>/terms.sqlite``.
"""

from __future__ import annotations

import enum
import json
import os
import re
import shutil
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from labdata.exceptions import OntologyError
from labdata.ontology import paths

#: The layout of the tables :func:`write_snapshot` writes; a change to them raises it.
TABLE_LAYOUT = 2

_SCHEMA = """
create table meta (key text primary key, value text not null);
create table terms (id text primary key, name text, obsolete integer not null);
create table texts (
    id text not null, text text not null, kind text not null,
    scope text, synonym_type text, folded text not null
);
create table retired (old_id text not null, live_id text, reason text not null);
create index texts_id on texts (id);
create index retired_old_id on retired (old_id);
"""

# Built after the inserts: the whole-text index, and the word index over live terms' texts.
# Texts go in by term id, so their row ids order terms by id, which breaks a search's ties.
_INDEX = """
create index texts_folded on texts (folded);
create virtual table texts_words using fts5 (
    folded, content = '', tokenize = 'porter unicode61 remove_diacritics 2'
);
insert into texts_words (rowid, folded)
    select x.rowid, x.folded from texts x join terms t on t.id = x.id where t.obsolete = 0
    order by x.rowid;
"""

_PREFIX = re.compile(r"[A-Za-z][A-Za-z0-9]*[:_]")


class Scope(enum.StrEnum):
    """A synonym's scope, as OBO gives it."""

    EXACT = "EXACT"
    NARROW = "NARROW"
    BROAD = "BROAD"
    RELATED = "RELATED"


class TextKind(enum.StrEnum):
    """Whether a text is a term's name or one of its synonyms."""

    NAME = "name"
    SYNONYM = "synonym"


class Reason(enum.StrEnum):
    """Why an id names no live term."""

    #: The term is obsolete; it may name its replacements.
    OBSOLETE = "obsolete"
    #: Merged into another id, as NCBI Taxonomy merges ids.
    MERGED = "merged"
    #: Kept only as another term's alternative id (OBO ``alt_id``).
    ALTERNATIVE = "alternative"
    #: Deleted, pointing to no live term.
    DELETED = "deleted"


@dataclass(frozen=True, slots=True)
class Synonym:
    """One synonym of a source term.

    ``scope`` is one of :class:`Scope`, given as the enum or its string; ``type`` is the
    vocabulary's own synonym type, such as UBERON's ``INCONSISTENT``.
    """

    text: str
    scope: str
    type: str | None = None


@dataclass(frozen=True, slots=True)
class SourceTerm:
    """One term as a reader gives it.

    An obsolete term's ``replaced_by`` may name ids in another vocabulary. Each of
    ``alt_ids`` becomes a retired id pointing to this term.
    """

    id: str
    name: str | None
    obsolete: bool = False
    replaced_by: tuple[str, ...] = ()
    alt_ids: tuple[str, ...] = ()
    synonyms: tuple[Synonym, ...] = ()


@dataclass(frozen=True, slots=True)
class RetiredEntry:
    """A retired id that is no term's own: merged or deleted, as taxonomy and Cellosaurus list.

    ``live_id`` is ``None`` for an id that points to no live term.
    """

    id: str
    live_id: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class SnapshotData:
    """Everything one snapshot's tables hold, as plain data.

    Parameters
    ----------
    vocabulary : str
        The vocabulary's name, such as ``uberon``.
    release : str
        The release, such as ``2026-11-05`` or ``257``; the snapshot is
        ``<vocabulary>-<release>``.
    terms : Sequence of SourceTerm
        The vocabulary's own terms; a reader leaves out imported ones.
    retired : Sequence of RetiredEntry
        Retired ids beyond those the terms give.
    prefixes : tuple of str
        The id prefixes of the vocabulary's own terms, such as ``("UBERON:",)``, which route a
        lookup to it. Empty takes each term id's leading ``<letters>:`` or ``<letters>_``.
    """

    vocabulary: str
    release: str
    terms: Sequence[SourceTerm]
    retired: Sequence[RetiredEntry] = ()
    prefixes: tuple[str, ...] = ()


def fold(text: str) -> str:
    """Return ``text`` as search matches it: case folded and whitespace collapsed."""
    return " ".join(text.split()).casefold()


def snapshot_name(vocabulary: str, release: str) -> str:
    """Return a snapshot's name, ``<vocabulary>-<release>``."""
    return f"{vocabulary}-{release}"


def tables_path(root: Path, snapshot: str) -> Path:
    """Return a snapshot's tables file, which may not exist."""
    return paths.SNAPSHOTS.at(root, snapshot, paths.TABLES_FILE)


def write_snapshot(root: Path, data: SnapshotData, *, sources: Sequence[Path] = ()) -> str:
    """Write one snapshot's tables, and move its source files in; switch nothing.

    A new snapshot is built under ``work/building/`` and renamed into ``snapshots/`` whole. A
    snapshot that exists gets its source files moved in, then its tables file replaced whole by
    one written under ``work/building/``, so its tables are remade with no download.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root.
    data : SnapshotData
        The terms and retired ids.
    sources : Sequence of pathlib.Path
        Downloaded source files, moved into the snapshot's ``source/`` folder by name.

    Returns
    -------
    str
        The snapshot's name.

    Raises
    ------
    OntologyError
        If a synonym's scope or a retired id's reason is not one of its kind, or a term id
        appears twice.
    """
    name = snapshot_name(data.vocabulary, data.release)
    final = paths.SNAPSHOTS.at(root, name)
    if final.is_dir():
        _move_sources(final, sources)
        tmp = paths.BUILDING.at(root, f"{name}.{os.getpid()}.sqlite")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        try:
            _write_tables(tmp, data, name)
            tmp.replace(tables_path(root, name))
        finally:
            tmp.unlink(missing_ok=True)
        return name
    staging = paths.BUILDING.at(root, f"{name}.{os.getpid()}")
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        _write_tables(staging / paths.TABLES_FILE, data, name)
        _move_sources(staging, sources)
        final.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(final)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return name


def layout_of(root: Path, snapshot: str) -> int | None:
    """Return the table layout a snapshot was written with; ``None`` if it has no tables."""
    path = tables_path(root, snapshot)
    if not path.is_file():
        return None
    with closing(connect(path)) as db:
        return int(read_meta(db)["layout"])


def connect(path: Path) -> sqlite3.Connection:
    """Open a snapshot's tables file read-only; it is never written once in place."""
    return sqlite3.connect(f"{path.as_uri()}?mode=ro&immutable=1", uri=True)


def read_meta(db: sqlite3.Connection) -> dict[str, str]:
    """Return a snapshot's ``meta`` table as a dict."""
    return dict(db.execute("select key, value from meta").fetchall())


def _write_tables(path: Path, data: SnapshotData, name: str) -> None:
    prefixes = data.prefixes or tuple(sorted(_prefixes(data.terms)))
    meta = {
        "vocabulary": data.vocabulary,
        "release": data.release,
        "snapshot": name,
        "prefixes": json.dumps(list(prefixes)),
        "layout": str(TABLE_LAYOUT),
    }
    path.unlink(missing_ok=True)
    with closing(sqlite3.connect(path)) as db:
        db.executescript(_SCHEMA)
        try:
            db.executemany("insert into meta values (?, ?)", meta.items())
            db.executemany(
                "insert into terms values (?, ?, ?)",
                ((t.id, t.name, int(t.obsolete)) for t in data.terms),
            )
            db.executemany(
                "insert into texts values (?, ?, ?, ?, ?, ?)",
                _texts(sorted(data.terms, key=lambda t: t.id)),
            )
            db.executemany("insert into retired values (?, ?, ?)", _retired(data))
            db.executescript(_INDEX)
        except sqlite3.IntegrityError as err:
            raise OntologyError(f"{name}: a term id appears twice ({err})") from err
        except ValueError as err:
            raise OntologyError(f"{name}: {err}") from err
        db.commit()


def _texts(
    terms: Sequence[SourceTerm],
) -> Iterator[tuple[str, str, str, str | None, str | None, str]]:
    for term in terms:
        if term.name:
            yield term.id, term.name, TextKind.NAME.value, None, None, fold(term.name)
        for syn in term.synonyms:
            scope = Scope(syn.scope).value
            yield term.id, syn.text, TextKind.SYNONYM.value, scope, syn.type, fold(syn.text)


def _retired(data: SnapshotData) -> Iterator[tuple[str, str | None, str]]:
    for term in data.terms:
        if term.obsolete:
            for live in term.replaced_by or (None,):
                yield term.id, live, Reason.OBSOLETE.value
        for alt in term.alt_ids:
            yield alt, term.id, Reason.ALTERNATIVE.value
    for entry in data.retired:
        yield entry.id, entry.live_id, Reason(entry.reason).value


def _prefixes(terms: Sequence[SourceTerm]) -> set[str]:
    return {m.group(0) for t in terms if (m := _PREFIX.match(t.id))}


def _move_sources(folder: Path, sources: Sequence[Path]) -> None:
    if not sources:
        return
    target = folder / paths.SOURCE_DIR
    target.mkdir(exist_ok=True)
    for source in sources:
        source.replace(target / source.name)
