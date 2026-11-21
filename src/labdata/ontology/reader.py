"""An open ontology store, pinned at open to each vocabulary's current snapshot.

Opening reads the record of current snapshots once and opens each snapshot's tables, so a
switch or a prune after that changes nothing an open store reads.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import Self

from labdata.exceptions import OntologyError
from labdata.ontology import tables
from labdata.ontology.root import find_root, read_current
from labdata.ontology.tables import Reason


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
