"""The kinds of path in a store root, which both stores declare: state, rebuildable or scratch.

:mod:`labdata.store.paths` and :mod:`labdata.papers.paths` each declare their store's paths.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from pathlib import Path


class Kind(enum.Enum):
    """What a path is to GCS and to a cleanup."""

    #: GCS holds it: a copy sends it, and a restore brings it back.
    STATE = "state"
    #: Remade on demand from state; never copied, and kept by a cleanup.
    REBUILDABLE = "rebuildable"
    #: On the work disk only; a cleanup may delete it when no run is going.
    SCRATCH = "scratch"


STATE, REBUILDABLE, SCRATCH = Kind.STATE, Kind.REBUILDABLE, Kind.SCRATCH


@dataclass(frozen=True, slots=True)
class Declared:
    """One declared path: a file, or a folder and everything under it.

    Parameters
    ----------
    rel : str
        The path from the store root; a folder's ends in ``/``.
    kind : Kind
        What the path is.
    """

    rel: str
    kind: Kind

    @property
    def name(self) -> str:
        """The last part of the path, without a folder's ``/``."""
        return self.rel.rstrip("/").rsplit("/", 1)[-1]

    def at(self, root: Path, *parts: str) -> Path:
        """Return this path under ``root``, with ``parts`` below it for a folder."""
        return root.joinpath(self.rel, *parts)

    def holds(self, rel: str) -> bool:
        """Return whether ``rel``, a path from the root, is this path or lies under it."""
        return rel == self.rel or (self.rel.endswith("/") and f"{rel}/".startswith(self.rel))


def kind(rel: str, layout: tuple[Declared, ...]) -> Kind:
    """Return the kind of a path from the root, by the longest declared path that holds it.

    A hidden name nothing declares is a temp file a write left: scratch.

    Parameters
    ----------
    rel : str
        A POSIX path from the store root, such as ``raw/geo-soft/GSE1nnn.parquet``.
    layout : tuple of Declared
        The store's declaration.

    Returns
    -------
    Kind
        The path's kind.

    Raises
    ------
    ValueError
        If no declared path holds it.
    """
    holders = [d for d in layout if d.holds(rel)]
    if holders:
        return max(holders, key=lambda d: len(d.rel)).kind
    if any(part.startswith(".") for part in rel.split("/")):
        return SCRATCH
    raise ValueError(f"{rel} is not a declared path of the store root")


def declared(of: Kind, layout: tuple[Declared, ...]) -> list[str]:
    """Return the declared paths of one kind, from the root, in declaration order."""
    return [d.rel for d in layout if d.kind is of]
