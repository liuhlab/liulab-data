"""The E-utilities lane: the SRA ``EXPERIMENT_PACKAGE`` of every GEO-linked experiment.

The experiment list is the union of the SRX accessions in saved SOFT ``!Sample_relation``
lines and the ``SRA_Accessions.tab`` experiments with ``Center=GEO`` and a GSM alias, each with
its tab status. Packages come from ``efetch`` in batches, several calls in flight under the
client's shared rate cap. Each bucket's piece is split into parts of a few thousand
experiments, so a preempted run loses at most a few open parts. An experiment NCBI serves no
package for is saved as an ``sra-xml`` row with empty text, which the build lists in its notes. Every step streams: the tab, the SOFT text and the list are read
in chunks and spilled to Parquet under ``work/eutils/``.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import shutil
from collections.abc import Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import EntrezError
from labdata.geo.sra_records import split_packages
from labdata.ncbi.entrez import EntrezClient, FetchedRecords
from labdata.store import extract, raw, snapshots
from labdata.store._files import temp_beside
from labdata.store.progress import EVERY, Progress

#: Experiments per ``efetch`` call.
BATCH = 300
#: ``efetch`` calls in flight.
INFLIGHT = 8
#: Experiments per part: a bucket's piece is saved in parts this size.
PART_SIZE = 6_000
#: Parts with calls in flight at once.
OPEN_PARTS = 4

_SOFT_SRX = re.compile(rb"^!Sample_relation\s*=\s*SRA:\s*\S*?[?&]term=([SED]RX\d+)", re.M)
_GSM = re.compile(r"GSM\d+")
_TAB_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("status", pa.string()),
        ("geo", pa.bool_()),
        ("gsm", pa.string()),
    ]
)
_SOFT_SCHEMA = pa.schema([("accession", pa.string())])
_SPILL_SCHEMA = pa.schema([("accession", pa.string()), ("raw_text", pa.string())])
_ROWS = 500_000
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LaneReport:
    """What one lane run did: packages saved, experiments with none, experiments to retry."""

    fetched: int
    missing: int
    failed: int


def experiment_list(
    root: Path, *, date: str | None = None, series: Iterable[str] | None = None
) -> Path:
    """Write the lane's experiment list and return its path.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    date : str or None
        The ``SRA_Accessions.tab`` snapshot date; defaults to the newest.
    series : iterable of str or None
        Only these GEO Series' experiments: SOFT relations of these Series, and tab rows
        whose GSM the newest ``geo-series`` snapshot lists under one of them. Every
        Series by default.

    Returns
    -------
    pathlib.Path
        ``work/eutils/experiments-<date>.parquet``: ``accession`` and ``status`` (empty when
        the tab does not list it), one row per experiment, in bucket order. It is rewritten
        on every call, as the saved SOFT may have grown.

    Raises
    ------
    StoreBuildError
        If the store has no such snapshot.
    """
    snap = snapshots.snapshot(root, snapshots.SRA_ACCESSIONS, date)
    folder = root / "work" / "eutils"
    tab = _tab_experiments(snap, folder / f"tab-{snap.date}.parquet")
    wanted = None if series is None else set(series)
    soft = _soft_experiments(root, folder / "soft.parquet", wanted)
    geo = "geo"
    if wanted is not None:
        samples = _samples(root, folder / "samples.parquet", wanted)
        geo = f"geo AND gsm IN (SELECT accession FROM read_parquet('{samples}'))"
    out = list_path(root, snap.date)
    tmp = temp_beside(out)
    try:
        with extract.connect(root) as con:
            con.execute(
                f"""
                COPY (
                  WITH tab AS (
                    SELECT accession, any_value(status) AS status, bool_or({geo}) AS geo
                    FROM read_parquet('{tab}') GROUP BY accession
                  ),
                  wanted AS (
                    SELECT accession FROM tab WHERE geo
                    UNION SELECT accession FROM read_parquet('{soft}')
                  )
                  SELECT w.accession, coalesce(t.status, '') AS status
                  FROM wanted w LEFT JOIN tab t USING (accession)
                  WHERE regexp_matches(w.accession, '^[A-Z]+[0-9]+$')
                  ORDER BY regexp_extract(w.accession, '^[A-Z]+'), length(w.accession),
                           w.accession
                ) TO '{tmp}' (FORMAT parquet)
                """
            )
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def list_path(root: Path, date: str) -> Path:
    """Return where :func:`experiment_list` writes the list for one tab snapshot date."""
    return root / "work" / "eutils" / f"experiments-{date}.parquet"


def _tab_experiments(snap: snapshots.Snapshot, out: Path) -> Path:
    """Spill every experiment row of the tab: accession, status, and whether GEO lists it."""
    if out.is_file():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(out)
    try:
        with snap.path.open("rb") as fh, pq.ParquetWriter(tmp, _TAB_SCHEMA) as writer:
            names = fh.readline().decode().rstrip("\n").split("\t")
            at = [names.index(n) for n in ("Accession", "Status", "Type", "Center", "Alias")]
            width = max(at) + 1
            rows: list[tuple[str, str, bool, str | None]] = []
            for line in fh:
                if b"\tEXPERIMENT\t" not in line:
                    continue
                cells = line.decode("utf-8", "replace").rstrip("\n").split("\t")
                if len(cells) < width:
                    continue
                accession, status, kind, center, alias = (cells[i] for i in at)
                if kind != "EXPERIMENT":
                    continue
                gsm = _GSM.search(alias) if center == "GEO" else None
                state = "" if status == "-" else status
                rows.append((accession.strip(), state, gsm is not None, gsm and gsm[0]))
                if len(rows) >= _ROWS:
                    writer.write_table(_tab_table(rows))
                    rows = []
            writer.write_table(_tab_table(rows))
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _tab_table(rows: list[tuple[str, str, bool, str | None]]) -> pa.Table:
    columns = list(zip(*rows, strict=True)) if rows else [[]] * len(_TAB_SCHEMA)
    return pa.table(dict(zip(_TAB_SCHEMA.names, columns, strict=True)), schema=_TAB_SCHEMA)


def _samples(root: Path, out: Path, series: set[str]) -> Path:
    """Spill the GSMs the newest ``geo-series`` snapshot lists under these Series."""
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES)
    tmp = temp_beside(out)
    try:
        with (
            gzip.open(snap.path, "rt", encoding="utf-8") as fh,
            pq.ParquetWriter(tmp, _SOFT_SCHEMA) as writer,
        ):
            writer.write_table(_SOFT_SCHEMA.empty_table())
            found: list[str] = []
            for line in fh:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record["accession"] in series:
                    found += [s["accession"] for s in record.get("samples") or ()]
                if len(found) >= _ROWS:
                    writer.write_table(pa.table({"accession": found}, schema=_SOFT_SCHEMA))
                    found = []
            writer.write_table(pa.table({"accession": found}, schema=_SOFT_SCHEMA))
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _soft_experiments(root: Path, out: Path, series: set[str] | None) -> Path:
    """Spill the SRX accessions of the saved SOFT of these Series, packed or in a part."""
    out.parent.mkdir(parents=True, exist_ok=True)
    paths = sorted((root / "raw" / raw.GEO_SOFT).glob("*.parquet"))
    paths += raw.part_files(root, raw.GEO_SOFT)
    tmp = temp_beside(out)
    try:
        with pq.ParquetWriter(tmp, _SOFT_SCHEMA) as writer:
            writer.write_table(_SOFT_SCHEMA.empty_table())
            for path in paths:
                found: set[str] = set()
                columns = ["accession", "raw_text"]
                for batch in pq.ParquetFile(path).iter_batches(batch_size=64, columns=columns):
                    for gse, text in zip(*batch.to_pydict().values(), strict=True):
                        if series is not None and gse not in series:
                            continue
                        found.update(m.decode() for m in _SOFT_SRX.findall(text.encode()))
                writer.write_table(pa.table({"accession": sorted(found)}, schema=_SOFT_SCHEMA))
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


@dataclass(eq=False)
class _Part:
    """One part's worth of a piece; its packages are spilled to disk as batches come back."""

    piece: raw.Piece
    batches: int
    spill: Path
    fetched: int = 0
    missing: list[str] = field(default_factory=list)
    failed: set[str] = field(default_factory=set)
    _writer: pq.ParquetWriter | None = None

    def add(self, got: FetchedRecords) -> None:
        """Spill one batch's packages, an empty text for each it has none for."""
        texts = got.records | dict.fromkeys(got.missing, "")
        if texts:
            if self._writer is None:
                self.spill.parent.mkdir(parents=True, exist_ok=True)
                self._writer = pq.ParquetWriter(self.spill, _SPILL_SCHEMA)
            columns = {"accession": list(texts), "raw_text": list(texts.values())}
            self._writer.write_table(pa.table(columns, schema=_SPILL_SCHEMA))
        self.fetched += len(got.records)
        self.missing += got.missing
        self.failed |= set(got.failed)
        self.batches -= 1

    def rows(self, date: str) -> Iterator[raw.RawRow]:
        """Yield the spilled packages back, a few at a time."""
        if self._writer is None:
            return
        self._writer.close()
        for chunk in pq.ParquetFile(self.spill).iter_batches(batch_size=64):
            for accession, text in zip(*chunk.to_pydict().values(), strict=True):
                yield raw.RawRow(accession, text, date)


def fetch_packages(
    root: Path,
    experiments: Path,
    *,
    client: EntrezClient,
    date: str,
    batch: int = BATCH,
    inflight: int = INFLIGHT,
    part_size: int = PART_SIZE,
    log_every: float = EVERY,
) -> LaneReport:
    """Fetch the package of every listed experiment no part or raw bucket holds yet.

    Calls finish in any order: a slow call holds back only its own part, while at most
    :data:`OPEN_PARTS` parts are open at once.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    experiments : pathlib.Path
        The list from :func:`experiment_list`.
    client : EntrezClient
        The Entrez seam; its limiter sets the rate.
    date : str
        The fetch date recorded on every row.
    batch, inflight, part_size : int
        Experiments per call, calls in flight, experiments per part.
    log_every : float
        Seconds between progress lines.

    Returns
    -------
    LaneReport
        Counts for this run. An experiment whose call failed is asked once more before the
        lane ends; if that fails too it is not saved, so the next run asks for it again. One
        NCBI has no package for is saved as asked, with an empty text.
    """
    first = _fetch_once(root, experiments, client, date, batch, inflight, part_size, log_every)
    if not first.failed:
        return first
    logger.info("asking again for the %d experiments whose calls failed", first.failed)
    last = _fetch_once(root, experiments, client, date, batch, inflight, part_size, log_every)
    return LaneReport(first.fetched + last.fetched, first.missing + last.missing, last.failed)


def _fetch_once(
    root: Path,
    experiments: Path,
    client: EntrezClient,
    date: str,
    batch: int,
    inflight: int,
    part_size: int,
    log_every: float,
) -> LaneReport:
    spill = root / "work" / "eutils" / "spill"
    shutil.rmtree(spill, ignore_errors=True)
    total = sum(len(p.todo) for _, pieces in _pieces(root, experiments) for p in pieces)
    report = {"fetched": 0, "missing": 0, "failed": 0}
    jobs = _jobs(root, experiments, batch, part_size, spill)
    pending: dict[Future[FetchedRecords], _Part] = {}
    opened: set[_Part] = set()
    upcoming: list[tuple[_Part, list[str]]] = []
    with (
        Progress(logger, "experiments", total, every=log_every) as progress,
        ThreadPoolExecutor(inflight, thread_name_prefix="eutils") as pool,
    ):

        def refill() -> None:
            while len(pending) < 2 * inflight:
                if not upcoming and (job := next(jobs, None)):
                    upcoming.append(job)
                if not upcoming:
                    return
                part, ids = upcoming[0]
                if part not in opened and len(opened) >= OPEN_PARTS:
                    return
                upcoming.pop()
                opened.add(part)
                pending[pool.submit(_fetch, client, ids)] = part

        refill()
        while pending:
            finished, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in finished:
                part = pending.pop(future)
                got = future.result()
                part.add(got)
                progress.add(
                    len(got.records) + len(got.missing) + len(got.failed),
                    missing=len(got.missing),
                    failed=len(got.failed),
                )
                if part.batches == 0:
                    _save(root, part, date)
                    opened.discard(part)
                    report["fetched"] += part.fetched
                    report["missing"] += len(part.missing)
                    report["failed"] += len(part.failed)
            refill()
    return LaneReport(**report)


def _fetch(client: EntrezClient, ids: list[str]) -> FetchedRecords:
    try:
        return client.efetch_records("sra", ids, rettype="full", split=split_packages)
    except EntrezError as err:
        logger.warning("efetch of %d experiments from %s failed: %s", len(ids), ids[0], err)
        return FetchedRecords(failed=dict.fromkeys(ids, str(err)))


def _save(root: Path, part: _Part, date: str) -> None:
    """Write the part; an experiment whose call failed is left unasked, to be fetched again."""
    bucket = part.piece.bucket
    asked = tuple(a for a in part.piece.todo if a not in part.failed)
    if asked:
        raw.write_part(root, raw.Piece(raw.SRA_XML, bucket, asked), part.rows(date))
    part.spill.unlink(missing_ok=True)


def _jobs(
    root: Path, experiments: Path, batch: int, part_size: int, spill: Path
) -> Iterator[tuple[_Part, list[str]]]:
    for _, pieces in _pieces(root, experiments):
        for piece in pieces:
            for start in range(0, len(piece.todo), part_size):
                todo = piece.todo[start : start + part_size]
                part = _Part(
                    raw.Piece(piece.source, piece.bucket, todo),
                    -(-len(todo) // batch),
                    spill / f"{piece.bucket}.{start}.parquet",
                )
                for at in range(0, len(todo), batch):
                    yield part, list(todo[at : at + batch])


def _pieces(root: Path, experiments: Path) -> Iterator[tuple[dict[str, str], list[raw.Piece]]]:
    """Yield each bucket's experiment statuses and its unsaved piece, one bucket at a time."""
    for status in _buckets(experiments):
        yield status, raw.pieces(root, raw.SRA_XML, status)


def _buckets(experiments: Path) -> Iterator[dict[str, str]]:
    """Yield the list's experiments bucket by bucket, each mapped to its status."""
    current, chunk = None, {}
    for accession, status in _rows(experiments):
        bucket = raw.bucket_of(raw.SRA_XML, accession)
        if bucket != current and chunk:
            yield chunk
            chunk = {}
        current = bucket
        chunk[accession] = status
    if chunk:
        yield chunk


def _rows(path: Path) -> Iterable[tuple[str, str]]:
    for batch in pq.ParquetFile(path).iter_batches(columns=["accession", "status"]):
        yield from zip(batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=True)
