"""Every path in a GEO/SRA store root, with its kind, and the only reader and switcher of the marker.

Each path is state, rebuildable or scratch (:class:`Kind`). The copy sends the state files of
the current build: the build, the raw files and snapshots it read, and the top-level files.
Other modules ask this one for a path, ``paths.RAW.at(root, source)``, instead of spelling it.
"""

from __future__ import annotations

import datetime as dt
import enum
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from labdata.store._files import read_toml_marker, toml_line, write_atomic


class Kind(enum.Enum):
    """What a path is to GCS and to a cleanup."""

    #: GCS holds it: a copy sends it, and a restore brings it back.
    STATE = "state"
    #: Remade on demand from state; never copied, and kept by a cleanup.
    REBUILDABLE = "rebuildable"
    #: On the work disk only; a cleanup may delete it when no run is going.
    SCRATCH = "scratch"


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


STATE, REBUILDABLE, SCRATCH = Kind.STATE, Kind.REBUILDABLE, Kind.SCRATCH

#: The marker: format version, the current build and when the store switched to it.
MARKER = Declared("labdata-store.toml", STATE)
#: The root README, written at each switch.
README = Declared("README.md", STATE)
#: The copy record: what GCS holds, as the last successful copy left it.
COPY_RECORD = Declared(".copied-files.json", STATE)
#: The status record of the last refresh.
STATUS = Declared("refresh-status.json", STATE)
BUILDS = Declared("builds/", STATE)
RAW = Declared("raw/", STATE)
SNAPSHOTS = Declared("snapshots/", STATE)
#: One Parquet extract per snapshot, ``<source>-<date>/``.
EXTRACTS = Declared("extracts/", REBUILDABLE)
#: Run logs kept by hand; nothing reads them.
SUPPORT = Declared("support/", SCRATCH)
BUILD_LOCK = Declared("work/build.lock", SCRATCH)
#: A running build's parse parts, ``<build-id>/``.
BUILD_WORK = Declared("work/build/", SCRATCH)
#: Fetch and removal parts that pack merges into ``raw/``, ``<source>/``.
PARTS = Declared("work/parts/", SCRATCH)
#: The full run's rounds, ``<date>-<fraction>/``.
RUNS = Declared("work/run/", SCRATCH)
#: The refreshes, ``<started>/``.
REFRESHES = Declared("work/refresh/", SCRATCH)
#: The target Series lists.
TARGETS = Declared("work/targets/", SCRATCH)
#: The sorted public accession lists builds link.
KNOWN = Declared("work/known/", SCRATCH)
#: The E-utilities lane's lists and spill.
EUTILS = Declared("work/eutils/", SCRATCH)
#: DuckDB spill, ``<pid>/``.
TMP = Declared("work/tmp/", SCRATCH)
#: The yearly SRA removal's batches, ``<tab date>/``.
SRA_REMOVAL = Declared("work/sra-removal/", SCRATCH)
#: Where extracts were kept before; the store's next extract or build moves them out.
OLD_EXTRACTS = Declared("work/extract/", SCRATCH)

#: Every declared path.
LAYOUT = (
    MARKER,
    README,
    COPY_RECORD,
    STATUS,
    BUILDS,
    RAW,
    SNAPSHOTS,
    EXTRACTS,
    SUPPORT,
    BUILD_LOCK,
    BUILD_WORK,
    PARTS,
    RUNS,
    REFRESHES,
    TARGETS,
    KNOWN,
    EUTILS,
    TMP,
    SRA_REMOVAL,
    OLD_EXTRACTS,
)


def kind(rel: str, layout: tuple[Declared, ...] = LAYOUT) -> Kind:
    """Return the kind of a path from the root, by the longest declared path that holds it.

    A hidden name nothing declares is a temp file a write left: scratch.

    Parameters
    ----------
    rel : str
        A POSIX path from the store root, such as ``raw/geo-soft/GSE1nnn.parquet``.
    layout : tuple of Declared
        The declaration to look in.

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


def declared(of: Kind, layout: tuple[Declared, ...] = LAYOUT) -> list[str]:
    """Return the declared paths of one kind, from the root, in declaration order."""
    return [d.rel for d in layout if d.kind is of]


#: The marker key naming the current build.
BUILD_KEY = "build"
#: The marker key holding when the current build was switched to, in UTC.
SWITCHED_KEY = "switched"
#: The marker table mapping each earlier build to when it was switched away from, in UTC.
SWITCHED_AWAY_KEY = "switched_away"


def read_marker(root: Path) -> dict[str, Any]:
    """Return the store's parsed marker.

    Raises
    ------
    NotAStoreError
        If the root has no marker.
    """
    return read_toml_marker(root, MARKER.rel, "a labdata store")


def parse_marker(text: str) -> dict[str, Any]:
    """Return a marker's settings from its text, such as the copy GCS holds."""
    return tomllib.loads(text)


def write_marker(root: Path, settings: dict[str, Any]) -> None:
    """Write the marker whole: its scalar settings, then each table."""
    scalars = [toml_line(k, v) for k, v in settings.items() if not isinstance(v, dict)]
    tables = [
        f"\n[{name}]\n" + "".join(f"{_quoted(k)} = {_quoted(v)}\n" for k, v in rows.items())
        for name, rows in settings.items()
        if isinstance(rows, dict)
    ]
    write_atomic(MARKER.at(root), "".join(f"{line}\n" for line in scalars) + "".join(tables))


def switch(root: Path, build_id: str) -> None:
    """Point the marker at a build and record when the old one was switched away from."""
    settings = read_marker(root)
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    old = settings.get(BUILD_KEY)
    if old and old != build_id:
        settings.setdefault(SWITCHED_AWAY_KEY, {})[str(old)] = now
    settings[BUILD_KEY] = build_id
    settings[SWITCHED_KEY] = now
    write_marker(root, settings)


def _quoted(key: str) -> str:
    """Return ``key`` as a quoted TOML key."""
    return toml_line("k", key)[len("k = ") :]
