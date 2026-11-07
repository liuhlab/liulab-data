"""The ``geo-series`` snapshot: the ``esummary`` of every GEO Series, patched or dumped whole.

A patch lists every Series UID with paged ``esearch``, fetches the summaries of Series the
previous snapshot lacks or GEO updated in the change window, drops the Series no longer listed
and copies the rest. Its file is the one a full dump of the same state writes: one compact JSON
record per line, sorted by UID.

Fetched batches wait in the folder's hidden ``.parts/``, so a killed run resumes there. The
snapshot file appears whole by a rename, and only once the listing has passed its checks.
"""

from __future__ import annotations

import datetime as dt
import gzip
import heapq
import json
import logging
import re
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from labdata.exceptions import SnapshotError
from labdata.store import changes, provenance, snapshots
from labdata.store._files import temp_beside, write_atomic
from labdata.store.fetch import GDS_OFFSET
from labdata.store.layout import MANIFEST

if TYPE_CHECKING:
    from labdata.ncbi.entrez import EntrezClient

FILE = snapshots._FILES[snapshots.GEO_SERIES]
#: Every Series UID the snapshot was made from, one per line.
UIDS = "gse_uids.txt"
TERM = "gse[ETYP]"
#: UIDs per ``esearch`` page and per ``esummary`` call.
PAGE = 10_000
BATCH = 500
#: Listings tried before a short or shifting one fails the step.
LIST_TRIES = 3
#: The most Series a listing may drop from the previous snapshot: the larger of the two.
DROP_FLOOR = 1_000
DROP_SHARE = 0.01

_WORK = ".parts"
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_PARSE_TRIES = 3

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SeriesSnapshot:
    """A finished ``geo-series`` snapshot and how it was made.

    ``patched_from`` is the previous snapshot's date, or ``None`` for a full dump.
    """

    path: Path
    records: int
    fetched: int
    dropped: int
    patched_from: str | None


def download(
    out: Path, client: EntrezClient, *, base: Path | None = None, full: bool = False
) -> SeriesSnapshot:
    """Write the ``geo-series`` snapshot into ``out``, ``<snapshots>/geo-series/<date>/``.

    Parameters
    ----------
    out : pathlib.Path
        The dated folder. A finished snapshot there is verified against its manifest.
    client : EntrezClient
        The NCBI seam.
    base : pathlib.Path or None
        The snapshot folder to patch from; defaults to the newest earlier dated folder beside
        ``out``. With none, every summary is fetched.
    full : bool
        Fetch every summary even when there is a snapshot to patch from.

    Returns
    -------
    SeriesSnapshot
        The snapshot file and its counts.

    Raises
    ------
    SnapshotError
        If the listing is short, shifts while paged, or drops too many Series, or a file
        does not match its manifest. No snapshot file is written.
    EntrezError
        If an NCBI call fails after its retries.
    """
    final = out / FILE
    if final.is_file() and (out / MANIFEST).is_file():
        return _verified(out)
    previous = None if full else (base or _previous(out))
    base_uids = _base_uids(previous) if previous else set()
    work = out / _WORK
    work.mkdir(parents=True, exist_ok=True)
    uids = _saved(work / UIDS) or _write_uids(work / UIDS, _listing(client))
    listed = set(uids)
    dropped = base_uids - listed
    limit = max(DROP_FLOOR, int(DROP_SHARE * len(base_uids)))
    if len(dropped) > limit:
        raise SnapshotError(
            f"esearch lists {len(uids)} GEO Series, dropping {len(dropped)} of the "
            f"{len(base_uids)} in {previous}; more than {limit} reads as a short list"
        )
    since = None
    if previous is None:
        todo = uids
    else:
        since = dt.date.fromisoformat(previous.name) - dt.timedelta(days=changes.OVERLAP_DAYS)
        todo = _saved(work / "todo.txt") or _write_uids(
            work / "todo.txt", sorted((listed - base_uids) | (_updated(client, since) & listed))
        )
    logger.info(
        "geo-series: %d listed, %d to fetch, %d dropped%s",
        len(uids),
        len(todo),
        len(dropped),
        f", patching {previous}" if previous else "",
    )
    _fetch(client, work, todo)
    records = _assemble(final, previous, listed, work)
    write_atomic(out / UIDS, "".join(f"{uid}\n" for uid in uids))
    _document(
        out, previous, since, uids=len(uids), records=records, fetched=len(todo),
        dropped=len(dropped), full=full,
    )  # fmt: skip
    shutil.rmtree(work)
    return SeriesSnapshot(
        final, records, len(todo), len(dropped), previous.name if previous else None
    )


def _previous(out: Path) -> Path | None:
    older = sorted(
        p
        for p in out.parent.glob("*")
        if _DATE.match(p.name) and p.name < out.name and (p / FILE).is_file()
    )
    return older[-1] if older else None


def _verified(folder: Path) -> SeriesSnapshot:
    manifest = json.loads((folder / MANIFEST).read_text(encoding="utf-8"))
    _check(folder, manifest)
    return SeriesSnapshot(
        folder / FILE,
        manifest["records"],
        manifest.get("fetched", manifest["records"]),
        manifest.get("dropped", 0),
        manifest.get("patched_from"),
    )


def _check(folder: Path, manifest: dict[str, Any]) -> None:
    for entry in manifest.get("files", []):
        if entry["name"] == FILE and provenance.sha256_file(folder / FILE) != entry["sha256"]:
            raise SnapshotError(f"{folder / FILE} does not match its {MANIFEST}")


def _base_uids(folder: Path) -> set[int]:
    if (folder / MANIFEST).is_file():
        _check(folder, json.loads((folder / MANIFEST).read_text(encoding="utf-8")))
    return {uid for uid, _ in _rows(folder / FILE)}


def _rows(path: Path) -> Iterator[tuple[int, str]]:
    """Yield each line of a snapshot file with its UID, checking they ascend."""
    last = -1
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            uid = int(json.loads(line)["uid"])
            if uid <= last:
                raise SnapshotError(f"{path} is not sorted by UID; rerun with --full")
            last = uid
            yield uid, line.rstrip("\n")


def _saved(path: Path) -> list[int]:
    return [int(u) for u in path.read_text().split()] if path.is_file() else []


def _write_uids(path: Path, uids: list[int]) -> list[int]:
    write_atomic(path, "".join(f"{uid}\n" for uid in uids))
    return uids


def _listing(client: EntrezClient) -> list[int]:
    """Return every Series UID, sorted; a listing that is short or shifts is tried again."""
    seen: set[int] = set()
    counts: set[int] = set()
    for attempt in range(1, LIST_TRIES + 1):
        seen, counts = set(), set()
        start, count = 0, 1
        while start < count:
            params = {"db": "gds", "term": TERM, "retstart": str(start), "retmax": str(PAGE)}
            result = _json(client, "esearch", params | {"retmode": "json"})["esearchresult"]
            count = int(result["count"])
            counts.add(count)
            ids = [int(u) for u in result.get("idlist", [])]
            if not ids:
                break
            seen.update(ids)
            start += len(ids)
        if len(counts) == 1 and len(seen) == count:
            return sorted(seen)
        logger.warning("geo-series: listing %d got %d UIDs of %s", attempt, len(seen), counts)
    raise SnapshotError(
        f"esearch {TERM} listed {len(seen)} UIDs of a count of {sorted(counts)} "
        f"in {LIST_TRIES} tries; no snapshot written"
    )


def _updated(client: EntrezClient, since: dt.date) -> set[int]:
    offset = GDS_OFFSET["GSE"]
    return {offset + int(gse[3:]) for gse in changes.updated_series(client, since)}


def _json(client: EntrezClient, util: str, params: dict[str, str]) -> dict[str, Any]:
    """Return one E-utilities JSON reply; a reply cut short is asked again."""
    for attempt in range(1, _PARSE_TRIES + 1):
        text = client.eutil_text(util, params)
        try:
            return json.loads(text)
        except json.JSONDecodeError as err:
            logger.warning("geo-series: %s reply %d unreadable: %s", util, attempt, err)
    raise SnapshotError(f"{util} replies were unreadable {_PARSE_TRIES} times")


def _fetch(client: EntrezClient, work: Path, todo: list[int]) -> None:
    """Save each batch's summaries as one part; a part already there is skipped."""
    for i in range(0, len(todo), BATCH):
        part = work / f"{i:07d}.json"
        if part.is_file():
            continue
        batch = todo[i : i + BATCH]
        params = {"db": "gds", "id": ",".join(map(str, batch)), "retmode": "json"}
        result = _json(client, "esummary", params | {"version": "2.0"}).get("result", {})
        got = sorted(int(u) for u in result.get("uids", []))
        if len(got) != len(batch):
            logger.warning("geo-series: batch %d asked %d, got %d", i, len(batch), len(got))
        lines = [json.dumps(result[str(u)], separators=(",", ":")) for u in got]
        write_atomic(part, json.dumps(lines))
        if (i // BATCH) % 40 == 0:
            logger.info("geo-series: fetched %d of %d", min(i + BATCH, len(todo)), len(todo))


def _parts(work: Path) -> Iterator[tuple[int, str]]:
    for part in sorted(work.glob("[0-9]*.json")):
        for line in json.loads(part.read_text(encoding="utf-8")):
            yield int(json.loads(line)["uid"]), line


def _assemble(final: Path, previous: Path | None, listed: set[int], work: Path) -> int:
    """Merge the fresh summaries over the previous snapshot's listed rows; return the count.

    A fresh summary replaces the previous row of its UID. A Series asked for but not
    answered keeps its previous row.
    """
    old = (row for row in _rows(previous / FILE) if row[0] in listed) if previous else iter(())
    # Fresh rows sort first among equal UIDs, so the previous row of a fetched UID is skipped.
    merged = heapq.merge(
        ((uid, 0, line) for uid, line in _parts(work)),
        ((uid, 1, line) for uid, line in old),
    )
    tmp = temp_beside(final)
    n, last = 0, -1
    try:
        with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as fh:
            for uid, _, line in merged:
                if uid == last:
                    continue
                fh.write(line + "\n")
                n, last = n + 1, uid
        tmp.replace(final)
    finally:
        tmp.unlink(missing_ok=True)
    logger.info("geo-series: wrote %d records", n)
    return n


def _document(
    out: Path,
    previous: Path | None,
    since: dt.date | None,
    *,
    uids: int,
    records: int,
    fetched: int,
    dropped: int,
    full: bool,
) -> None:
    how = (
        f"Patched from the {previous.name} snapshot: Series new since it or updated on or "
        f"after {since} were fetched again, Series GEO no longer lists were dropped, and the "
        "rest were copied."
        if previous
        else "Every summary was fetched."
    )
    command = f"{provenance.DOWNLOADER} geo-series" + (" --full" if full else "")
    manifest = {
        "title": f"snapshots/{snapshots.GEO_SERIES}/{out.name}",
        "what": (
            f"The esummary of every GEO Series in gds (term {TERM}), one JSON record per "
            f"line, sorted by UID; {UIDS} lists the UIDs. {how}"
        ),
        "source": f"{_EUTILS}/esearch.fcgi and esummary.fcgi?db=gds&retmode=json&version=2.0",
        "date": out.name,
        "query_uids": uids,
        "records": records,
        "fetched": fetched,
        "dropped": dropped,
        "patched_from": previous.name if previous else None,
        "files": provenance.file_entries(out),
        "produced_by": provenance.produced_by(command),
    }
    write_atomic(out / MANIFEST, json.dumps(manifest, indent=2) + "\n")
    (out / provenance.README).unlink(missing_ok=True)
    provenance.document_snapshot(out)
