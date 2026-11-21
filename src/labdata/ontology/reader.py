"""An open ontology store, pinned at open to each vocabulary's current snapshot.

Opening reads the record of current snapshots once and opens each snapshot's tables, so a
switch or a prune after that changes nothing an open store reads.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import Any, Self

from labdata.exceptions import OntologyError
from labdata.ontology import tables
from labdata.ontology.root import find_root, read_current
from labdata.ontology.tables import Reason, Scope, TextKind, fold

_WORD = re.compile(r"\w+")
_TEXT_ROW = "x.id, t.name, x.text, x.kind, x.scope, x.synonym_type"


@dataclass(frozen=True, slots=True)
class Term:
    """What a lookup finds for an id: a term, or a retired id.

    ``retired`` is ``None`` for a live term. An obsolete term has its own name and
    ``retired=Reason.OBSOLETE``. A merged, alternative or deleted id has no term of its own,
    so its ``name`` is ``None``. ``replaced_by`` lists the live ids it points to, which may be
    in another vocabulary.
    """

    id: str
    name: str | None
    obsolete: bool
    replaced_by: tuple[str, ...]
    retired: Reason | None
    snapshot: str


@dataclass(frozen=True, slots=True)
class Candidate:
    """A term a search returns for a label's text, with the text of the term that matched.

    ``scope`` and ``synonym_type`` are ``None`` when the name matched; ``synonym_type`` is also
    ``None`` for a synonym the vocabulary gives no type.
    """

    id: str
    name: str | None
    text: str
    matched_on: TextKind
    scope: Scope | None
    synonym_type: str | None
    snapshot: str


class OntologyStore:
    """An open ontology store, pinned to the snapshots current when it opened.

    Close it, or use it as a context manager, to release the snapshot files.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        current = read_current(root)
        self._dbs: dict[str, sqlite3.Connection] = {}
        routes: list[tuple[str, str]] = []
        try:
            for vocabulary, snapshot in sorted(current.items()):
                path = tables.tables_path(root, snapshot)
                if not path.is_file():
                    raise OntologyError(f"{snapshot} is current but has no tables at {path}")
                db = self._dbs[vocabulary] = tables.connect(path)
                meta = tables.read_meta(db)
                routes += [(prefix, vocabulary) for prefix in json.loads(meta["prefixes"])]
        except BaseException:
            self.close()
            raise
        self._routes = sorted(routes, key=lambda r: (-len(r[0]), r[0]))
        self.snapshots: MappingProxyType[str, str] = MappingProxyType(dict(sorted(current.items())))

    def __repr__(self) -> str:
        """Return the root."""
        return f"OntologyStore({str(self.root)!r})"

    def __enter__(self) -> Self:
        """Return the open store."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the store."""
        self.close()

    def close(self) -> None:
        """Release every snapshot's tables; the store answers nothing after."""
        for db in self._dbs.values():
            db.close()
        self._dbs.clear()

    def lookup(self, id: str) -> Term | None:
        """Return the term or retired id ``id``, from the vocabulary its prefix names.

        Parameters
        ----------
        id : str
            A term id, such as ``UBERON:0002421``.

        Returns
        -------
        Term or None
            ``None`` for an id the vocabulary never had, or whose prefix no vocabulary owns.

        Examples
        --------
        >>> with open_ontology_store() as store:
        ...     store.lookup("CHEBI:1").replaced_by
        ('CHEBI:18357',)
        """
        key = id.strip()
        for prefix, vocabulary in self._routes:
            if key.startswith(prefix):
                found = self._find(vocabulary, key)
                if found is not None:
                    return found
        return None

    def search(self, text: str, vocabulary: str, limit: int = 10) -> list[Candidate]:
        """Return up to ``limit`` candidates for a label's text, best first.

        Case is folded and whitespace collapsed. Whole-text matches come first: on the name,
        then on an EXACT synonym, then on another scope. Then terms sharing words with the text,
        by BM25 relevance. Ties break on id. Each term appears once, with its best-ranked text;
        an obsolete term never appears.

        Parameters
        ----------
        text : str
            The label's text; blank text has no candidates.
        vocabulary : str
            The vocabulary to search, such as ``uberon``.
        limit : int
            The most candidates to return.

        Returns
        -------
        list of Candidate

        Raises
        ------
        OntologyError
            If the store holds no such vocabulary.

        Examples
        --------
        >>> with open_ontology_store() as store:
        ...     store.search("hippocampus", "uberon")[0].snapshot
        'uberon-2026-11-05'
        """
        db = self._searchable(vocabulary)
        key = fold(text)
        if not key or limit <= 0:
            return []
        snapshot = self.snapshots[vocabulary]
        whole: dict[str, tuple[int, int, Candidate]] = {}
        for rowid, *row in db.execute(
            f"select x.rowid, {_TEXT_ROW} from texts x join terms t on t.id = x.id"
            " where x.folded = ? and t.obsolete = 0",
            (key,),
        ):
            found = _candidate(row, snapshot)
            rank = (_tier(found), rowid, found)
            if found.id not in whole or rank[:2] < whole[found.id][:2]:
                whole[found.id] = rank
        ranked = [c for _, _, c in sorted(whole.values(), key=lambda r: (r[0], r[2].id))]
        words = " OR ".join(f'"{word}"' for word in _WORD.findall(key))
        if len(ranked) < limit and words:
            rows = db.execute(
                f"select {_TEXT_ROW}, min(f.rank) as score from texts_words f"
                " join texts x on x.rowid = f.rowid join terms t on t.id = x.id"
                " where texts_words match ? group by x.id order by score, x.id limit ?",
                (words, limit + len(ranked)),
            )
            ranked += [c for *row, _ in rows if (c := _candidate(row, snapshot)).id not in whole]
        return ranked[:limit]

    def _searchable(self, vocabulary: str) -> sqlite3.Connection:
        # The one gate every search passes on its vocabulary.
        if vocabulary not in self._dbs:
            held = ", ".join(sorted(self.snapshots)) or "none"
            raise OntologyError(f"no vocabulary {vocabulary!r} in this store; it holds: {held}")
        return self._dbs[vocabulary]

    def _find(self, vocabulary: str, key: str) -> Term | None:
        db = self._dbs[vocabulary]
        snapshot = self.snapshots[vocabulary]
        row = db.execute("select name, obsolete from terms where id = ?", (key,)).fetchone()
        retired = db.execute(
            "select live_id, reason from retired where old_id = ? order by rowid", (key,)
        ).fetchall()
        if row is None and not retired:
            return None
        replaced_by = tuple(live for live, _ in retired if live is not None)
        reason = Reason(retired[0][1]) if retired else None
        if row is None:
            return Term(key, None, False, replaced_by, reason, snapshot)
        return Term(key, row[0], bool(row[1]), replaced_by, reason, snapshot)


def _candidate(row: list[Any], snapshot: str) -> Candidate:
    id, name, text, kind, scope, synonym_type = row
    return Candidate(
        id, name, text, TextKind(kind), Scope(scope) if scope else None, synonym_type, snapshot
    )


def _tier(candidate: Candidate) -> int:
    if candidate.matched_on is TextKind.NAME:
        return 0
    return 1 if candidate.scope is Scope.EXACT else 2


def open_ontology_store(root: str | os.PathLike[str] | None = None) -> OntologyStore:
    """Open the ontology store, pinned to each vocabulary's current snapshot.

    The root is the first of: the ``root`` argument, ``LABDATA_ONTOLOGIES``, then the per-user
    pointer file written by ``labdata ontology init``.

    Raises
    ------
    StoreNotConfiguredError
        If no root is set anywhere.
    NotAStoreError
        If the root has no ``labdata-ontologies.toml``.
    OntologyError
        If a current snapshot has no tables.
    """
    return OntologyStore(find_root(root))
