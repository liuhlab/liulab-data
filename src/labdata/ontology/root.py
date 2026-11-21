"""Find the ontology store root, make one, read or switch its current snapshots, and prune.

The pattern is the other stores': a marker file in the root, ``labdata-ontologies.toml``,
found through ``LABDATA_ONTOLOGIES`` or a per-user pointer file beside ``store.toml``.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import os
import re
import shutil
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from pathlib import Path

from labdata._cache import liulab_data_cache_dir
from labdata._files import create_marker, read_toml_marker, toml_line, write_atomic
from labdata._root import resolve_root, write_pointer
from labdata.exceptions import OntologyError, StoreNotConfiguredError
from labdata.ontology import paths, tables

logger = logging.getLogger(__name__)

_PART = re.compile(r"\d+|\D+")

#: Environment variable naming the ontology store root.
ONTOLOGIES_ENV = "LABDATA_ONTOLOGIES"

#: Basename of the per-user pointer file.
POINTER_FILENAME = "ontologies.toml"
_SECTION = "ontologies"

#: The ontology store layout version ``labdata ontology init`` writes.
FORMAT_VERSION = 1


def find_root(root: str | os.PathLike[str] | None = None) -> Path:
    """Return the ontology store root, checked for its marker, without network or prompt.

    The root is the first of: the ``root`` argument, ``LABDATA_ONTOLOGIES``, then the
    per-user pointer file written by ``labdata ontology init``.

    Raises
    ------
    StoreNotConfiguredError
        If no root is set anywhere.
    NotAStoreError
        If the root has no ``labdata-ontologies.toml``.
    """
    path = configured_root(root)
    if path is None:
        raise StoreNotConfiguredError(
            "ontology store not configured: pass open_ontology_store(root=...), set "
            f"{ONTOLOGIES_ENV}, or run `labdata ontology init <root>`"
        )
    read_toml_marker(path, paths.MARKER.rel, "an ontology store")
    return path


def configured_root(root: str | os.PathLike[str] | None = None) -> Path | None:
    """Return the root set by argument, ``LABDATA_ONTOLOGIES`` or pointer; ``None`` if unset.

    Checks no marker, so a restore can name an empty root.
    """
    return resolve_root(root, ONTOLOGIES_ENV, pointer_path(), _SECTION)


def init_ontology_store(root: str | os.PathLike[str], *, copy_to: str = "") -> Path:
    """Make an ontology store: its marker, ``README.md`` and an empty current record.

    Also writes the per-user pointer to it.

    Parameters
    ----------
    root : str or os.PathLike
        The ontology store root; created if missing.
    copy_to : str
        The rclone path the store is copied to, ``<remote>:<bucket>/<path>``.

    Returns
    -------
    pathlib.Path
        The marker written.

    Raises
    ------
    LabdataError
        If the root already has a marker.
    """
    marker = make_store(root, copy_to=copy_to)
    write_pointer(pointer_path(), _SECTION, marker.parent)
    return marker


def make_store(root: str | os.PathLike[str], *, copy_to: str = "") -> Path:
    """Make an ontology store as :func:`init_ontology_store` does, but write no pointer.

    Returns
    -------
    pathlib.Path
        The marker written.

    Raises
    ------
    LabdataError
        If the root already has a marker.
    """
    path = Path(root).expanduser().absolute()
    path.mkdir(parents=True, exist_ok=True)
    marker = paths.MARKER.at(path)
    lines = [toml_line("format_version", FORMAT_VERSION), toml_line("copy_to", copy_to)]
    create_marker(marker, "\n".join(lines) + "\n", "ontology store")
    readme = importlib.resources.files("labdata.ontology").joinpath("ontology_readme.md")
    write_atomic(paths.README.at(path), readme.read_text(encoding="utf-8"))
    paths.SNAPSHOTS.at(path).mkdir(exist_ok=True)
    write_atomic(paths.CURRENT.at(path), _dump({}))
    return marker


def pointer_path() -> Path:
    """Return the per-user pointer file, ``<cache>/ontologies.toml``, which may not exist."""
    return liulab_data_cache_dir() / POINTER_FILENAME


def read_current(root: Path) -> dict[str, str]:
    """Return each vocabulary's current snapshot name; empty when none is switched yet."""
    path = paths.CURRENT.at(root)
    if not path.is_file():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")))


def switch(root: Path, snapshots: Mapping[str, str]) -> dict[str, str]:
    """Make each named snapshot its vocabulary's current one, rewriting the record whole.

    Vocabularies not named keep their current snapshot.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root.
    snapshots : Mapping of str to str
        Vocabulary name to snapshot name, such as ``{"uberon": "uberon-2026-11-05"}``.

    Returns
    -------
    dict of str to str
        The record as written.

    Raises
    ------
    OntologyError
        If a named snapshot has no tables; the record is then left as it was.
    """
    for name in snapshots.values():
        if not (paths.SNAPSHOTS.at(root, name) / paths.TABLES_FILE).is_file():
            raise OntologyError(f"no snapshot {name} in {root}: it has no {paths.TABLES_FILE}")
    current = read_current(root) | dict(snapshots)
    write_atomic(paths.CURRENT.at(root), _dump(current))
    return current


def prune(root: Path) -> list[str]:
    """Delete every snapshot but each vocabulary's current one and the newest one before it.

    The snapshot before the current one stays, so a reader opened before the last switch still
    reads its files. Releases order as numbers where they hold digits, so ``10`` follows ``9``.
    A snapshot is moved into scratch whole, then deleted there.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root.

    Returns
    -------
    list of str
        The snapshots deleted, by name.
    """
    pruning = paths.PRUNING.at(root)
    shutil.rmtree(pruning, ignore_errors=True)
    record = read_current(root)
    current = set(record.values())
    older: dict[str, list[tuple[str, str]]] = {}
    for folder in sorted(paths.SNAPSHOTS.at(root).iterdir()):
        if folder.name in current or not folder.is_dir():
            continue
        meta = _meta(folder / paths.TABLES_FILE)
        if meta is None:
            logger.warning("snapshot %s has no readable tables; prune leaves it", folder.name)
            continue
        older.setdefault(meta["vocabulary"], []).append((meta["release"], folder.name))
    gone: list[str] = []
    for vocabulary, held in older.items():
        held.sort(key=lambda pair: _release_key(pair[0]))
        # A vocabulary with no current snapshot keeps its newest two.
        keep = 1 if vocabulary in record else 2
        gone += [name for _, name in held[:-keep]]
    for name in gone:
        pruning.mkdir(parents=True, exist_ok=True)
        paths.SNAPSHOTS.at(root, name).rename(pruning / name)
    shutil.rmtree(pruning, ignore_errors=True)
    return sorted(gone)


def _meta(path: Path) -> dict[str, str] | None:
    if not path.is_file():
        return None
    try:
        with closing(tables.connect(path)) as db:
            return tables.read_meta(db)
    except sqlite3.Error:
        return None


def _release_key(release: str) -> tuple[tuple[int, int | str], ...]:
    return tuple((0, int(p)) if p.isdigit() else (1, p) for p in _PART.findall(release))


def _dump(record: Mapping[str, str]) -> str:
    return json.dumps(dict(record), indent=2, sort_keys=True) + "\n"
