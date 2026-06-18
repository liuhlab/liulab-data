"""Cache-directory resolution for liulab-data.

A small I/O-boundary helper: it computes where the package stores cached
configuration (e.g. NCBI credentials) and, later, downloads. Nothing here
touches the network.
"""

from __future__ import annotations

import os
from pathlib import Path

#: Environment variable overriding the XDG cache base directory.
XDG_CACHE_ENV = "XDG_CACHE_HOME"

#: Subdirectory (under the cache base) that namespaces all liulab-data files.
CACHE_NAMESPACE = "liulab-data"


def liulab_data_cache_dir() -> Path:
    """Return the liulab-data cache directory.

    The base is read from ``XDG_CACHE_HOME`` when set (and non-empty), otherwise
    it defaults to ``~/.cache``; the package namespaces its files under a
    ``liulab-data`` subdirectory. The path is expanded but **not** created here —
    callers create it on first write via :func:`ensure_cache_dir`.

    Returns
    -------
    pathlib.Path
        The resolved cache directory, e.g. ``~/.cache/liulab-data``.
    """
    base = os.environ.get(XDG_CACHE_ENV)
    root = Path(base) if base else Path.home() / ".cache"
    return (root / CACHE_NAMESPACE).expanduser()


def ensure_cache_dir() -> Path:
    """Return the cache directory, creating it (and any parents) if absent.

    Returns
    -------
    pathlib.Path
        The existing cache directory.
    """
    path = liulab_data_cache_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path
