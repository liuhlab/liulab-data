"""Read the bulk snapshots under ``<root>/snapshots/<source>/<date>/`` as streams.

Source names are the bulk downloader's. Nothing here unpacks a file to disk or holds a
whole file in memory.
"""

from __future__ import annotations

import gzip
import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from labdata.exceptions import StoreBuildError

SRA_ACCESSIONS = "sra-accessions"
BIOPROJECT = "bioproject"
BIOSAMPLE = "biosample"

_FILES = {
    SRA_ACCESSIONS: "SRA_Accessions.tab",
    BIOPROJECT: "bioproject.xml",
    BIOSAMPLE: "biosample_set.xml.gz",
}
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CHUNK = 1 << 20


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One snapshot file and the date of the folder it sits in."""

    path: Path
    date: str


def snapshot(root: Path, source: str) -> Snapshot:
    """Return the newest snapshot of one source.

    Raises
    ------
    StoreBuildError
        If the store has no dated folder holding the source's file.
    """
    folder = root / "snapshots" / source
    dates = (
        sorted(p.name for p in folder.glob("*") if _DATE.match(p.name)) if folder.is_dir() else []
    )
    for date in reversed(dates):
        path = folder / date / _FILES[source]
        if path.is_file():
            return Snapshot(path, date)
    raise StoreBuildError(f"no {source} snapshot: expected {folder}/<date>/{_FILES[source]}")


def tab_rows(
    path: Path,
    keep: Callable[[dict[str, str]], bool],
    *,
    line_filter: Callable[[str], bool] | None = None,
) -> Iterator[dict[str, str]]:
    """Yield the ``SRA_Accessions.tab`` rows that ``keep`` accepts, as column-name dicts.

    ``line_filter`` is a cheap first pass over each raw line; only lines it accepts are
    split into columns. A ``-`` cell reads as an empty string.
    """
    with path.open(encoding="utf-8", newline="\n") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            if line_filter is not None and not line_filter(line):
                continue
            cells = line.rstrip("\n").split("\t")
            row = {
                name: ("" if cell == "-" else cell)
                for name, cell in zip(header, cells, strict=False)
            }
            if keep(row):
                yield row


def elements(path: Path, tag: str, wanted: Iterable[str], key: re.Pattern[str]) -> dict[str, str]:
    """Return the wanted ``<tag …>…</tag>`` elements of a large XML file, keyed by accession.

    Parameters
    ----------
    path : pathlib.Path
        The XML file; a ``.gz`` file is decompressed as a stream.
    tag : str
        The element name. Elements of this name must not nest.
    wanted : iterable of str
        The accessions to keep. Reading stops once all are found.
    key : re.Pattern
        Finds an element's accession in its text (group 1).

    Returns
    -------
    dict of str to str
        Each found accession's element text, from ``<tag`` to ``</tag>``, exactly as in the file.
    """
    want = set(wanted)
    found: dict[str, str] = {}
    if not want:
        return found
    start = re.compile(rf"<{tag}[\s>]")
    end = f"</{tag}>"
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as fh:
        buf = ""
        while chunk := fh.read(_CHUNK):
            buf += chunk
            while True:
                head = start.search(buf)
                if head is None:
                    buf = buf[-len(tag) - 2 :]
                    break
                stop = buf.find(end, head.start())
                if stop < 0:
                    buf = buf[head.start() :]
                    break
                text = buf[head.start() : stop + len(end)]
                buf = buf[stop + len(end) :]
                match = key.search(text)
                if match and match[1] in want:
                    found[match[1]] = text
                    if len(found) == len(want):
                        return found
    return found
