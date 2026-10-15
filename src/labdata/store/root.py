"""Find the store root and check its marker.

The per-user pointer file follows the ``ncbi.toml`` pattern and holds only the root;
store settings live in the store's own ``labdata-store.toml``.
"""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from labdata._cache import ensure_cache_dir, liulab_data_cache_dir
from labdata.exceptions import NotAStoreError, StoreNotConfiguredError

#: Environment variable naming the store root.
STORE_ENV = "LABDATA_STORE"

#: Basename of the per-user pointer file, beside ``ncbi.toml``.
POINTER_FILENAME = "store.toml"

#: The marker file every store root carries.
MARKER = "labdata-store.toml"


@dataclass(frozen=True, slots=True)
class Store:
    """An opened store.

    Parameters
    ----------
    root : pathlib.Path
        The store's root folder.
    settings : dict
        The parsed ``labdata-store.toml``.
    """

    root: Path
    settings: dict[str, Any]


def open_store(root: str | os.PathLike[str] | None = None) -> Store:
    """Open the store, resolving its root without network or prompt.

    The root is the first of: the ``root`` argument, ``LABDATA_STORE``, then the per-user
    pointer file written by ``labdata store configure``. Opening writes nothing in the store.

    Parameters
    ----------
    root : str or os.PathLike or None
        The store root. ``None`` falls back to the environment, then the pointer file.

    Returns
    -------
    Store
        The opened store.

    Raises
    ------
    StoreNotConfiguredError
        If no root is set anywhere.
    NotAStoreError
        If the root has no ``labdata-store.toml``.
    """
    if root is None:
        root = os.environ.get(STORE_ENV, "").strip() or _from_pointer()
    if root is None:
        raise StoreNotConfiguredError(
            f"store not configured: pass open_store(root=...), set {STORE_ENV}, "
            "or run `labdata store configure <root>`"
        )
    path = Path(root)
    marker = path / MARKER
    if not marker.is_file():
        raise NotAStoreError(f"{path} is not a labdata store: it has no {MARKER}")
    with marker.open("rb") as fh:
        settings = tomllib.load(fh)
    return Store(root=path, settings=settings)


def pointer_path() -> Path:
    """Return the path to the per-user pointer file (which may not exist).

    Returns
    -------
    pathlib.Path
        ``<cache>/store.toml``.
    """
    return liulab_data_cache_dir() / POINTER_FILENAME


def save_pointer(root: str | os.PathLike[str]) -> Path:
    """Write the per-user pointer file, which holds only the store root.

    Parameters
    ----------
    root : str or os.PathLike
        The store root; saved as an absolute path.

    Returns
    -------
    pathlib.Path
        The path the pointer was written to.
    """
    ensure_cache_dir()
    path = pointer_path()
    absolute = Path(root).expanduser().absolute()
    # A JSON string is a valid TOML basic string, escapes included.
    path.write_text(f"[store]\nroot = {json.dumps(str(absolute))}\n", encoding="utf-8")
    return path


def _from_pointer() -> str | None:
    path = pointer_path()
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return str(data.get("store", {}).get("root", "")).strip() or None
