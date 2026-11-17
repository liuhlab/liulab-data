"""The thin-slice fixtures as stores: each fixture's fake Entrez client, fetch and build.

Three fixture folders under ``tests/data/`` hold the thin-slice records:
``prjna1027859`` (record 1), ``sra_slice`` (records 5 and 6) and ``geo_slice`` (the rest).
"""

from __future__ import annotations

import fcntl
import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from labdata.store import extract
from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import FakeEntrezClient

DATA = Path(__file__).parent / "data"
TODAY = "2026-10-14"

PRJNA1027859 = "PRJNA1027859"
GEO_SERIES = [
    "GSE246310",
    "GSE153830",
    "GSE19456",
    "GSE233931",
    "GSE233685",
    "GSE20301",
    "GSE28770",
    "GSE15556",
]
SRA_PROJECTS = ["PRJNA436361", "PRJNA530347"]

#: The ``built_store`` fixture: ``built_store(path, slice_name)`` copies a prebuilt store there.
BuiltStore = Callable[[Path, str], Path]


def _soft_url(gse: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/soft/{gse}_family.soft.gz"


def _filelist_url(gse: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/suppl/filelist.txt"


def prjna1027859_client(**drops: int) -> FakeEntrezClient:
    """Serve record 1: its six packages and its ``[si]`` answer."""
    srx = [f"SRX2428313{i}" for i in range(6)]
    return FakeEntrezClient(
        esearch={f"{PRJNA1027859}[si]": ["39637862"]},
        packages={a: (DATA / "sra_package" / f"{a}.xml").read_text() for a in srx},
        drops=drops,
    )


def geo_client() -> FakeEntrezClient:
    """Serve the GEO records: trimmed SOFT, ``filelist.txt``, packages and Entrez answers."""
    folder = DATA / "geo_slice"
    entrez = json.loads((folder / "entrez.json").read_text())
    elink: dict[tuple[str, str, str], list[str]] = {}
    for key, uids in entrez["elink"].items():
        dbfrom, db, uid = key.split()
        elink[dbfrom, db, uid] = uids
    esummary: dict[tuple[str, str], dict[str, Any]] = {}
    for uid, srx in entrez["accessions"]["sra"].items():
        esummary["sra", uid] = {"Id": uid, "ExpXml": f'<Experiment acc="{srx}" ver="1"/>'}
    for uid, prj in entrez["accessions"]["bioproject"].items():
        esummary["bioproject", uid] = {"Id": uid, "Project_Acc": prj}
    pages = {_soft_url(p.stem): p.read_text(encoding="utf-8") for p in (folder / "soft").iterdir()}
    pages |= {_filelist_url(p.stem): p.read_text() for p in (folder / "filelist").iterdir()}
    return FakeEntrezClient(
        pages=pages,
        packages={p.stem: p.read_text(encoding="utf-8") for p in (folder / "packages").iterdir()},
        elink=elink,
        esummary=esummary,
        esearch={f"{acc}[si]": pmids for acc, pmids in entrez["pubmed_si"].items()},
    )


def sra_slice_client() -> FakeEntrezClient:
    """Serve records 5 and 6: their packages and ``[si]`` answers."""
    folder = DATA / "sra_slice"
    si = json.loads((folder / "pubmed_si.json").read_text())
    return FakeEntrezClient(
        esearch={f"{acc}[si]": pmids for acc, pmids in si.items()},
        packages={p.stem: p.read_text(encoding="utf-8") for p in (folder / "packages").iterdir()},
    )


#: Each fixture: its snapshot folder, the accessions fetched and its client.
SLICES = {
    "prjna1027859": (DATA / "prjna1027859" / "snapshots", [PRJNA1027859], prjna1027859_client),
    "geo_slice": (DATA / "geo_slice" / "snapshots", GEO_SERIES, geo_client),
    "sra_slice": (DATA / "sra_slice" / "snapshots", SRA_PROJECTS, sra_slice_client),
}


def copy_snapshots(snapshots: Path, root: Path) -> None:
    """Copy a fixture's snapshots into ``root`` and extract each, as an operator would."""
    shutil.copytree(snapshots, root / "snapshots")
    for source in extract.SOURCES:
        extract.extract(root, source, workers=1)


def new_root(path: Path, slice_name: str) -> Path:
    """Make a store root at ``path`` holding one fixture's snapshots and no build."""
    copy_snapshots(SLICES[slice_name][0], path)
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


def built_root(path: Path, slice_name: str) -> Path:
    """Make a store root at ``path``, fetch one fixture's records into it and build ``b1``."""
    _, accessions, client = SLICES[slice_name]
    new_root(path, slice_name)
    fetch(path, accessions, client=client(), today=TODAY)
    build(path, "b1")
    return path


#: Each fixture fetched and built once by :func:`prebuild`, kept for :func:`copy_built`.
_BUILT: dict[str, Path] = {}
#: The folder the fixtures are built in; every pytest-xdist worker is given the same one.
_folder: Path | None = None
#: Whether this process made the folder, and so removes it.
_owned = False
#: The build this process started, if it started one.
_builder: subprocess.Popen[bytes] | None = None
#: Why the build failed, raised again on every later :func:`prebuild`.
_failed: str | None = None

#: In the folder: the lock a build holds until it ends, its output, and the mark of success.
_LOCK, _LOG, _DONE = "build.lock", "log.txt", "done"


class Decompressor:
    """Stands in for rapidgzip or pigz: a gzip file's bytes on ``stdout``, then ``code``."""

    def __init__(self, command: list[str], code: int = 0) -> None:
        self.stdout = gzip.open(command[-1], "rb")  # noqa: SIM115 - the caller closes the pipe
        self._code = code

    def kill(self) -> None:
        pass

    def wait(self) -> int:
        return self._code


def defaults(patch: pytest.MonkeyPatch) -> None:
    """Set what every test runs under: one worker per parallel step, and no decompressor binary.

    One worker means no test starts Ray unasked; snapshots decompress in Python at the
    extract's subprocess seam.
    """
    patch.setenv("SLURM_CPUS_ON_NODE", "1")
    patch.setattr(extract, "_popen", Decompressor)


def _build_all(folder: str) -> None:
    """Fetch and build every fixture under ``folder``, under the test defaults, then mark it done."""
    with pytest.MonkeyPatch.context() as patch:
        defaults(patch)
        for name in SLICES:
            built_root(Path(folder) / name, name)
    (Path(folder) / _DONE).touch()


def shared_folder() -> Path:
    """Return the folder to build in, made by this process, which removes it at the end."""
    global _folder, _owned
    if _folder is None:
        _folder = Path(tempfile.mkdtemp(prefix="labdata-slices-"))
        _owned = True
    return _folder


def share(folder: Path) -> None:
    """Build in ``folder``, which another process made and removes: a pytest-xdist worker's."""
    global _folder
    _folder = folder


def start_prebuild() -> None:
    """Start fetching and building every fixture once, in a child process, unless one started.

    The child holds the folder's lock until it exits, so a worker that did not start it waits
    for it there. A folder with a log holds a build that ran, so none starts again.
    """
    global _builder
    folder = shared_folder()
    if _builder is not None or _BUILT or _failed is not None:
        return
    lock = os.open(folder / _LOCK, os.O_RDWR | os.O_CREAT)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return  # another worker's build holds it
        if (folder / _LOG).exists():
            return
        with (folder / _LOG).open("wb") as log:
            _builder = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    f"from tests import _slices; _slices._build_all({str(folder)!r})",
                ],
                cwd=Path(__file__).parents[1],
                stdout=log,
                stderr=subprocess.STDOUT,
                pass_fds=(lock,),
            )
    except BaseException:
        remove_prebuilt()
        raise
    finally:
        os.close(lock)


def prebuild() -> None:
    """Wait for the fixtures :func:`start_prebuild` builds, starting it if it has not started.

    A failed build raises with the child's output, then and on every later call.
    """
    global _builder, _failed
    if _failed is not None:
        raise RuntimeError(_failed)
    if _BUILT:
        return
    start_prebuild()
    folder = shared_folder()
    try:
        if _builder is not None:
            _builder.wait()
            _builder = None
        lock = os.open(folder / _LOCK, os.O_RDWR | os.O_CREAT)
        try:
            fcntl.flock(lock, fcntl.LOCK_SH)
        finally:
            os.close(lock)
    except BaseException:
        remove_prebuilt()
        raise
    if not (folder / _DONE).is_file():
        log = (folder / _LOG).read_text(encoding="utf-8", errors="replace")
        remove_prebuilt()
        _failed = f"building the thin slices failed:\n{log}"
        raise RuntimeError(_failed)
    _BUILT.update((name, folder / name) for name in SLICES)


def copy_built(path: Path, slice_name: str) -> Path:
    """Copy a prebuilt fixture's store to ``path``: what :func:`built_root` makes there."""
    return copy_store(_BUILT[slice_name], path)


def copy_store(root: Path, path: Path) -> Path:
    """Copy the store at ``root`` to ``path``.

    Files hard-linked to each other in the store, as a build links its known table, stay so.
    """
    copied: dict[int, str] = {}

    def copy(src: str, dst: str) -> str:
        stat = Path(src).stat()
        if stat.st_nlink > 1 and stat.st_ino in copied:
            Path(dst).hardlink_to(copied[stat.st_ino])
        else:
            shutil.copy2(src, dst)
            copied[stat.st_ino] = dst
        return dst

    shutil.copytree(root, path, copy_function=copy)
    return path


def remove_prebuilt() -> None:
    """End the build this process started, and delete the folder if this process made it.

    A build in a folder this process did not make is waited for, not stopped: another
    pytest-xdist worker may need it.
    """
    global _folder, _owned, _builder
    if _builder is not None:
        if _owned:
            _builder.kill()
        _builder.wait()
        _builder = None
    if _owned and _folder is not None:
        shutil.rmtree(_folder, ignore_errors=True)
        _folder, _owned = None, False
    _BUILT.clear()
