"""Find a store's root: the argument, then the store's environment variable, then its pointer.

The per-user pointer file follows the ``ncbi.toml`` pattern and holds only the root, under one
table; each store names its own variable, pointer file and table.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

from labdata._cache import ensure_cache_dir
from labdata._files import toml_line, write_atomic


def resolve_root(
    root: str | os.PathLike[str] | None, env: str, pointer: Path, section: str
) -> Path | None:
    """Return a root: ``root``, else the ``env`` variable, else the pointer file's.

    Parameters
    ----------
    root : str or os.PathLike or None
        The root as given; ``None`` falls back.
    env : str
        The environment variable naming the root.
    pointer : pathlib.Path
        The per-user pointer file, which holds ``root`` under ``[section]``.
    section : str
        The pointer file's table.

    Returns
    -------
    pathlib.Path or None
        ``None`` when no root is set anywhere.
    """
    if root is None:
        root = os.environ.get(env, "").strip() or _from_pointer(pointer, section)
    return None if root is None else Path(root)


def write_pointer(path: Path, section: str, root: str | os.PathLike[str]) -> Path:
    """Write a per-user pointer file holding only ``root``, absolute, under ``[section]``."""
    ensure_cache_dir()
    absolute = Path(root).expanduser().absolute()
    write_atomic(path, f"[{section}]\n{toml_line('root', str(absolute))}\n")
    return path


def _from_pointer(path: Path, section: str) -> str | None:
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return str(data.get(section, {}).get("root", "")).strip() or None
