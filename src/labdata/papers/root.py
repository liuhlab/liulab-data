"""Find the paper store root, make one, lock it, and place each paper's folder in it.

The pattern is the GEO/SRA store's: a marker file in the root, ``labdata-papers.toml``,
found through ``LABDATA_PAPERS`` or a per-user pointer file beside ``store.toml``.
"""

from __future__ import annotations

import fcntl
import importlib.resources
import os
import re
import sys
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from labdata._cache import liulab_data_cache_dir
from labdata.exceptions import StoreNotConfiguredError
from labdata.store._files import toml_line, write_atomic
from labdata.store.model import norm_doi
from labdata.store.root import (
    Store,
    create_marker,
    open_store,
    read_toml_marker,
    resolve_root,
    write_pointer,
)

if TYPE_CHECKING:
    from labdata.papers.reader import PaperReader

#: Environment variable naming the paper store root.
PAPERS_ENV = "LABDATA_PAPERS"

#: Basename of the per-user pointer file.
POINTER_FILENAME = "papers.toml"

#: The marker file every paper store root carries.
MARKER = "labdata-papers.toml"

#: The lock file the paper job's passes hold.
LOCK = ".lock"

#: The paper store layout version ``labdata papers init`` writes.
FORMAT_VERSION = 1

#: The folder under the root that holds one folder per paper.
PAPERS_DIR = "papers"

_PMID = re.compile(r"(?:PMID:)?(\d+)")


class PaperStore:
    """An opened paper store.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.
    settings : dict
        The parsed ``labdata-papers.toml``.
    """

    def __init__(self, root: Path, settings: dict[str, Any]) -> None:
        self.root = root
        self.settings = settings

    def __repr__(self) -> str:
        """Return the root."""
        return f"PaperStore({str(self.root)!r})"

    @cached_property
    def store(self) -> Store:
        """The GEO/SRA store whose current build links the papers, opened on first use."""
        return open_store(self.settings["store"])

    @cached_property
    def reader(self) -> PaperReader:
        """The reader over this paper store and the GEO/SRA store's current build."""
        from labdata.papers.reader import PaperReader
        from labdata.store.reader import BuildReader, current_path

        return PaperReader(self.root, BuildReader(current_path(self.store.root)))


def open_paper_store(root: str | os.PathLike[str] | None = None) -> PaperStore:
    """Open the paper store, resolving its root without network or prompt.

    The root is the first of: the ``root`` argument, ``LABDATA_PAPERS``, then the per-user
    pointer file written by ``labdata papers init``.

    Raises
    ------
    StoreNotConfiguredError
        If no root is set anywhere.
    NotAStoreError
        If the root has no ``labdata-papers.toml``.
    """
    path = resolve_root(root, PAPERS_ENV, pointer_path(), "papers")
    if path is None:
        raise StoreNotConfiguredError(
            f"paper store not configured: pass open_paper_store(root=...), set {PAPERS_ENV}, "
            "or run `labdata papers init <root>`"
        )
    return PaperStore(path, read_toml_marker(path, MARKER, "a paper store"))


def init_paper_store(
    root: str | os.PathLike[str],
    store: str | os.PathLike[str] | None = None,
    *,
    copy_to: str = "",
    drive_remote: str = "",
    drive_folder: str = "",
) -> Path:
    """Make a paper store: its marker and ``README.md``, and the per-user pointer to it.

    Parameters
    ----------
    root : str or os.PathLike
        The paper store root; created if missing.
    store : str or os.PathLike or None
        The GEO/SRA store it reads; ``None`` takes the one ``open_store()`` finds.
    copy_to : str
        The rclone path the paper store is copied to, ``<remote>:<bucket>/<path>``.
    drive_remote : str
        The rclone remote of the Google Drive that holds the Paperpile folder.
    drive_folder : str
        The Drive ID of the Paperpile folder.

    Returns
    -------
    pathlib.Path
        The marker written.

    Raises
    ------
    LabdataError
        If the root already has a marker, or ``store`` is not a GEO/SRA store.
    """
    geo = open_store(store).root.expanduser().absolute()
    path = Path(root).expanduser().absolute()
    path.mkdir(parents=True, exist_ok=True)
    marker = path / MARKER
    lines = [
        toml_line("format_version", FORMAT_VERSION),
        toml_line("store", str(geo)),
        toml_line("copy_to", copy_to),
        toml_line("drive_remote", drive_remote),
        toml_line("drive_folder", drive_folder),
    ]
    create_marker(marker, "\n".join(lines) + "\n", "paper store")
    readme = importlib.resources.files("labdata.papers").joinpath("papers_readme.md")
    write_atomic(path / "README.md", readme.read_text(encoding="utf-8"))
    save_pointer(path)
    return marker


def pointer_path() -> Path:
    """Return the per-user pointer file, ``<cache>/papers.toml``, which may not exist."""
    return liulab_data_cache_dir() / POINTER_FILENAME


def save_pointer(root: str | os.PathLike[str]) -> Path:
    """Write the per-user pointer file, which holds only the paper store root.

    Returns
    -------
    pathlib.Path
        The pointer file written.
    """
    return write_pointer(pointer_path(), "papers", root)


@contextmanager
def paper_lock(root: Path) -> Iterator[None]:
    """Hold the paper store's lock, ``<root>/.lock``; a second holder waits for it.

    The daily pass and the PMC sync each hold it for their whole run, so they never overlap.
    """
    lock = root / LOCK
    with lock.open("a") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"waiting for {lock}: another paper pass is running", file=sys.stderr, flush=True)
            fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def paper_key(text: str) -> str:
    """Return the paper key of a PMID or DOI, as written in a link or by hand.

    A PMID, bare or as ``PMID:<n>``, gives its digits; a DOI, bare, as ``DOI:<doi>`` or as a
    ``doi.org`` URL, gives the DOI in lower case.

    Raises
    ------
    ValueError
        If the text is neither a PMID nor a DOI.

    Examples
    --------
    >>> paper_key("PMID:39637862"), paper_key("https://doi.org/10.1101/2024.01.15.575678")
    ('39637862', '10.1101/2024.01.15.575678')
    """
    text = text.strip()
    if m := _PMID.fullmatch(text):
        return m.group(1)
    doi = norm_doi(text)
    if not doi.startswith("10.") or "/" not in doi:
        raise ValueError(f"not a PMID or a DOI: {text!r}")
    return doi


def paper_dir(key: str) -> Path:
    """Return a paper's folder, relative to the paper store root.

    A PMID's folder sits in a group of 1,000, as GEO's FTP groups Series; a DOI's folder is
    the DOI with ``/`` written ``%2F``, so the name converts back.

    Examples
    --------
    >>> paper_dir("39637862").as_posix()
    'papers/39637nnn/39637862'
    >>> paper_dir("10.1101/2024.01.15.575678").as_posix()
    'papers/doi/10.1101%2F2024.01.15.575678'
    """
    if key.isdigit():
        return Path(PAPERS_DIR, f"{key[:-3]}nnn", key)
    return Path(PAPERS_DIR, "doi", urllib.parse.quote(key, safe=""))
