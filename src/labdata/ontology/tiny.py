"""Build a tiny ontology store from a few given terms, for tests, with no download."""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from labdata.ontology import paths
from labdata.ontology.root import make_store, switch
from labdata.ontology.tables import SnapshotData, write_snapshot


def build_tiny_store(root: str | os.PathLike[str], snapshots: Iterable[SnapshotData]) -> Path:
    """Write each snapshot through the store's one writer, and make each current.

    Makes the store first when ``root`` is not one; writes no per-user pointer, so open the
    result with ``open_ontology_store(root)``.

    Parameters
    ----------
    root : str or os.PathLike
        The store root; created if missing.
    snapshots : Iterable of SnapshotData
        One per vocabulary, each with its terms.

    Returns
    -------
    pathlib.Path
        The store root.

    Examples
    --------
    >>> from labdata.ontology import SnapshotData, SourceTerm, Synonym, open_ontology_store
    >>> uberon = SnapshotData("uberon", "2026-11-05", [
    ...     SourceTerm("UBERON:0002421", "hippocampal formation",
    ...                synonyms=(Synonym("hippocampus", "NARROW", "INCONSISTENT"),)),
    ... ])
    >>> with open_ontology_store(build_tiny_store("ontologies", [uberon])) as store:
    ...     store.lookup("UBERON:0002421").name
    'hippocampal formation'
    """
    path = Path(root).expanduser().absolute()
    if not paths.MARKER.at(path).is_file():
        make_store(path)
    names = {data.vocabulary: write_snapshot(path, data) for data in snapshots}
    switch(path, names)
    return path
