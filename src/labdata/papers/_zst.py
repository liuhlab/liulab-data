"""Paper files stored zstd-compressed, one file at a time, as ``<name>.zst``."""

from __future__ import annotations

import io
from pathlib import Path

from backports import zstd

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


def compressed(name: str) -> bool:
    """Return whether a file of this name is stored compressed."""
    return Path(name).suffix.lower() in COMPRESSED


def open_plain(path: Path) -> io.BufferedIOBase:
    """Open a stored paper file for reading, decompressed when it ends in ``.zst``."""
    if path.name.endswith(SUFFIX):
        return zstd.ZstdFile(path, "rb")
    return path.open("rb")


def open_stored(path: Path, compress: bool) -> io.BufferedIOBase:
    """Open ``path`` for writing, compressing what is written when ``compress`` is set."""
    if compress:
        return zstd.ZstdFile(path, "wb", level=LEVEL)
    return path.open("wb")
