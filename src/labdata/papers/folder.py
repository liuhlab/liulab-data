"""The only code that writes, moves and deletes paper files.

A file is written under :data:`TEMP` and renamed into place, zstd-compressed when its name
calls for it, so it is whole or absent. :meth:`PaperFolder.commit` writes the queued moves
and deletes to :data:`PENDING`, puts each moved file at its target as well, saves the tables,
then deletes. A pass started after a kill finishes or drops them by what the saved ``files``
table names, and sweeps :data:`TEMP`. Losing :data:`PENDING` leaves at most orphan files,
never a table that names a missing file.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import threading
from collections.abc import Callable, Collection, Iterator
from contextlib import contextmanager
from pathlib import Path

from backports import zstd

from labdata._files import write_atomic
from labdata.papers import paths

#: The folder under the root that holds files being written; a pass empties it at its start.
TEMP = paths.TEMP.name

#: The moves and deletes a save has yet to apply.
PENDING = paths.PENDING.rel

#: The ending a compressed file gets.
SUFFIX = ".zst"

#: The zstd level files are compressed at.
LEVEL = 10

#: File endings stored compressed: XML, plain text, and images in uncompressed formats.
#: Everything else (PDF, xlsx, docx, zip, jpg, png, gif, ...) is stored as it is.
COMPRESSED = frozenset(
    {
        ".xml",
        ".csv",
        ".tsv",
        ".txt",
        ".tab",
        ".json",
        ".htm",
        ".html",
        ".md",
        ".rtf",
        ".xls",
        ".doc",
        ".ppt",
        ".eps",
        ".ps",
        ".svg",
        ".tif",
        ".tiff",
        ".bmp",
        ".fa",
        ".fasta",
        ".bed",
        ".gtf",
        ".gff",
        ".vcf",
        ".sdf",
        ".pdb",
        ".r",
        ".py",
        ".sh",
    }
)

# Folders this deep or deeper (``papers/<group>/<paper>/...``) are removed once empty.
_PAPER_DEPTH = 3

_Op = list[str]


def compressed(name: str) -> bool:
    """Return whether a file of this name is stored compressed."""
    return Path(name).suffix.lower() in COMPRESSED


def open_plain(path: Path) -> io.BufferedIOBase:
    """Open a stored paper file for reading, decompressed when it ends in ``.zst``."""
    if path.name.endswith(SUFFIX):
        return zstd.ZstdFile(path, "rb")
    return path.open("rb")


class PaperFolder:
    """The paper files under one paper store root.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root; every path given is relative to it.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self._ops: list[_Op] = []
        self._lock = threading.Lock()

    def recover(self, named: Collection[str]) -> None:
        """Finish or drop what a stopped save left, by the saved table, and sweep temp files.

        A move is finished when ``named`` holds its target, else its placed target goes; a
        delete is made when ``named`` lacks its path. Call it at a pass's start, holding the
        paper store's lock.
        """
        pending = self.root / PENDING
        if pending.is_file():
            self._apply(json.loads(pending.read_text(encoding="utf-8")), set(named))
            pending.unlink()
        shutil.rmtree(self.root / TEMP, ignore_errors=True)

    def temp_dir(self) -> Path:
        """Return :data:`TEMP`, made if missing."""
        path = self.root / TEMP
        path.mkdir(exist_ok=True)
        return path

    @contextmanager
    def writing(self, path: str | Path, *, compress: bool = False) -> Iterator[io.BufferedIOBase]:
        """Open ``path`` for writing; it appears only if the block ends without an error.

        With ``compress``, what is written is stored zstd-compressed.
        """
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(suffix=".part", dir=self.temp_dir())
        os.close(fd)
        tmp = Path(name)
        try:
            with _open_stored(tmp, compress) as fh:
                yield fh
            tmp.replace(target)
        finally:
            tmp.unlink(missing_ok=True)

    def move(self, path: str, to: str) -> None:
        """Move ``path`` to ``to``: in place before the tables naming ``to`` save, gone after."""
        with self._lock:
            self._ops.append(["move", path, to])

    def delete(self, path: str) -> None:
        """Delete ``path`` once the tables no longer naming it are saved."""
        with self._lock:
            self._ops.append(["delete", path])

    def commit(self, save: Callable[[], None], named: Collection[str]) -> None:
        """Run ``save``, which writes tables naming ``named``, then apply the moves and deletes.

        One the saved table is not yet ready for stays queued for the next commit.
        """
        with self._lock:
            ops = list(self._ops)
        pending = self.root / PENDING
        if ops:
            write_atomic(pending, json.dumps(ops))
        for op in ops:
            if op[0] == "move":
                self._place(op, named)
        save()
        if not ops:
            return
        left = self._apply(ops, set(named), keep=True)
        with self._lock:
            self._ops[: len(ops)] = left
            if self._ops:
                write_atomic(pending, json.dumps(self._ops))
            else:
                pending.unlink(missing_ok=True)

    def _apply(self, ops: list[_Op], named: set[str], *, keep: bool = False) -> list[_Op]:
        """Apply the ops ``named`` is ready for; return the others when ``keep`` is set."""
        left = []
        for op in ops:
            if op[0] == "move":
                src, dst = op[1], op[2]
                ready = dst in named and src not in named
                if (self.root / src).exists():
                    if ready:
                        if (self.root / dst).exists():
                            (self.root / src).unlink()
                        else:
                            (self.root / dst).parent.mkdir(parents=True, exist_ok=True)
                            (self.root / src).replace(self.root / dst)
                        self._prune((self.root / src).parent)
                    elif dst not in named:
                        # A placed target the saved tables never named.
                        (self.root / dst).unlink(missing_ok=True)
                        self._prune((self.root / dst).parent)
            else:
                src = op[1]
                ready = src not in named
                if ready:
                    (self.root / src).unlink(missing_ok=True)
                    self._prune((self.root / src).parent)
            if keep and not ready:
                left.append(op)
        return left

    def _place(self, op: _Op, named: Collection[str]) -> None:
        """Put a move's source at its target too, if ``named`` is ready for the move.

        A hard link, or a copy where the link fails; either lands by a rename.
        """
        src, dst = self.root / op[1], self.root / op[2]
        if op[2] not in named or op[1] in named or not src.exists():
            return
        if dst.exists() and src.samefile(dst):
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.temp_dir() / f"{os.getpid()}-{threading.get_ident()}.link"
        tmp.unlink(missing_ok=True)
        try:
            try:
                os.link(src, tmp)
            except OSError:
                shutil.copy2(src, tmp)
            tmp.replace(dst)
        finally:
            tmp.unlink(missing_ok=True)

    def _prune(self, folder: Path) -> None:
        """Remove ``folder`` and its parents while empty, down to a paper's folder."""
        while len(folder.relative_to(self.root).parts) >= _PAPER_DEPTH:
            try:
                folder.rmdir()
            except OSError:
                return
            folder = folder.parent


def _open_stored(path: Path, compress: bool) -> io.BufferedIOBase:
    if compress:
        return zstd.ZstdFile(path, "wb", level=LEVEL)
    return path.open("wb")
