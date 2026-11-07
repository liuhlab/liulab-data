"""The yearly SRA removal: confirm which held SRA experiments and runs are withdrawn.

A candidate is a held experiment, or a run in a held experiment's package, that the newest
``SRA_Accessions.tab`` marks non-public, or a held experiment no live Series reaches. A live
Series is one the newest ``geo-series`` snapshot lists; it reaches the experiments its saved
SOFT, its samples' tab rows and its Entrez links name.

A candidate is removed only when a fetch of its experiment alone confirms it: NCBI's "ID list
is empty" or "is not public" removes the experiment with its runs, and a served package
without a candidate run removes that run, by saving the package as served. Any other answer
keeps it. Batched fetches only keep: they settle the experiments served with every candidate
run, so fewer are asked alone. Removals are written as removal parts for pack to apply.

The step works in batches under ``work/sra-removal/<tab date>/``: a rerun skips what an
earlier run settled and asks the failed again. The monthly refresh never runs it.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
import re
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import EntrezError
from labdata.geo.sra_records import split_packages
from labdata.ncbi.entrez import EntrezClient, ids_empty
from labdata.store import eutils, extract, raw, snapshots
from labdata.store._files import temp_beside
from labdata.store.layout import read_parquet_sql, sql_string

#: Fetches in flight; the client's limiter sets the rate.
INFLIGHT = 4
#: Candidates confirmed between writes of removal parts and progress.
BATCH = 1_000

_NOT_PUBLIC = r"<ERROR[^>]*>SRA Experiment {acc} is not public</ERROR>"
_EMPTY_IDS = r"<ERROR[^>]*>ID list is empty![^<]*</ERROR>"
_SRA_LINK = r"\tsra\t[^\t\n]*\t([^\t\n]+)"
_TAB_SCHEMA = pa.schema(
    [("accession", pa.string()), ("kind", pa.string()), ("experiment", pa.string())]
)
_LIVE_SCHEMA = pa.schema([("accession", pa.string())])
_ROWS = 500_000
logger = logging.getLogger(__name__)


class _State(Enum):
    GONE = "gone"
    SERVED = "served"
    FAILED = "failed"


@dataclass
class SraRemovalReport:
    """What one run found.

    ``candidates`` counts the experiments listed: ``non_public`` marked so as a whole,
    ``with_runs`` holding a non-public run, ``unreached`` reached by no live Series (an
    experiment may count under more than one). ``asked`` counts the experiments this run
    asked about; ``gone`` those confirmed gone, ``runs_gone`` the runs confirmed gone from a
    served experiment, ``kept`` the experiments still served with every candidate run and
    ``failed`` those whose answer settled nothing.
    """

    candidates: int = 0
    non_public: int = 0
    with_runs: int = 0
    unreached: int = 0
    asked: int = 0
    gone: int = 0
    runs_gone: int = 0
    kept: int = 0
    failed: int = 0
    removals: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Candidate:
    accession: str
    non_public: bool
    unreached: bool
    runs: tuple[str, ...]


def work_dir(root: Path) -> Path:
    """Return this step's folder for the newest ``SRA_Accessions.tab`` snapshot."""
    snap = snapshots.snapshot(root, snapshots.SRA_ACCESSIONS)
    return root / "work" / "sra-removal" / snap.date


def candidates(root: Path) -> Path:
    """Write the candidate list once per tab snapshot and return its path.

    Returns
    -------
    pathlib.Path
        ``candidates.parquet`` in :func:`work_dir`: ``accession`` (a held experiment),
        ``non_public``, ``unreached`` and ``runs`` (its non-public runs its package holds),
        sorted by accession. A rerun reuses it.
    """
    folder = work_dir(root)
    out = folder / "candidates.parquet"
    if out.is_file():
        return out
    folder.mkdir(parents=True, exist_ok=True)
    files = raw.source_files(root, raw.SRA_XML)
    if not files:
        pq.write_table(_candidate_table([]), out)
        return out
    tab = _tab_non_public(snapshots.snapshot(root, snapshots.SRA_ACCESSIONS), folder)
    reached = _reached(root, folder)
    held = read_parquet_sql(files)
    tmp = temp_beside(out)
    try:
        with extract.connect(root) as con:
            con.execute(
                f"""
                COPY (
                  WITH tab AS (SELECT * FROM read_parquet({sql_string(str(tab))})),
                  held AS (SELECT DISTINCT accession FROM {held}),
                  runs AS (
                    SELECT p.accession, list(DISTINCT t.accession ORDER BY t.accession) AS runs
                    FROM {held} p JOIN tab t ON t.kind = 'RUN' AND t.experiment = p.accession
                    WHERE strpos(p.raw_text, 'accession="' || t.accession || '"') > 0
                    GROUP BY p.accession
                  ),
                  flagged AS (
                    SELECT h.accession,
                      h.accession IN (SELECT accession FROM tab WHERE kind = 'EXPERIMENT')
                        AS non_public,
                      h.accession NOT IN
                        (SELECT accession FROM read_parquet({sql_string(str(reached))}))
                        AS unreached,
                      coalesce(r.runs, []::VARCHAR[]) AS runs
                    FROM held h LEFT JOIN runs r USING (accession)
                  )
                  SELECT * FROM flagged WHERE non_public OR unreached OR len(runs) > 0
                  ORDER BY accession
                ) TO {sql_string(str(tmp))} (FORMAT parquet)
                """
            )
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def remove(root: Path, *, client: EntrezClient, date: str | None = None) -> SraRemovalReport:
    """Confirm each candidate with NCBI and write removal parts for the gone.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    client : EntrezClient
        The NCBI seam.
    date : str or None
        The fetch date of a package saved again because a run left it; defaults to today.

    Returns
    -------
    SraRemovalReport
        Counts for this run.
    """
    date = date or dt.datetime.now(dt.UTC).date().isoformat()
    listing = candidates(root)
    report = _count(listing)
    logger.info(
        "%d candidates: %d non-public, %d with a non-public run, %d no live Series reaches",
        report.candidates,
        report.non_public,
        report.with_runs,
        report.unreached,
    )
    progress = listing.parent / "settled.txt"
    skip = _settled(progress) | _pending_removals(root)
    report.asked = sum(1 for c in _read(listing) if c.accession not in skip)
    done = 0
    with ThreadPoolExecutor(INFLIGHT, thread_name_prefix="sra-removal") as pool:
        for batch in _batches((c for c in _read(listing) if c.accession not in skip), BATCH):
            settled = _confirm(root, client, pool, batch, date, report)
            with progress.open("a", encoding="utf-8") as fh:
                fh.write("".join(f"{a}\n" for a in sorted(settled)))
            done += len(batch)
            logger.info(
                "confirmed %d of %d: %d gone, %d runs gone, %d served, %d failed",
                done,
                report.asked,
                report.gone,
                report.runs_gone,
                report.kept,
                report.failed,
            )
    return report


def _confirm(
    root: Path,
    client: EntrezClient,
    pool: ThreadPoolExecutor,
    batch: list[_Candidate],
    date: str,
    report: SraRemovalReport,
) -> set[str]:
    """Settle one batch, write its removal parts and saved packages; return the settled."""
    kept = _batch_kept(client, pool, batch)
    alone = [c for c in batch if c.accession not in kept]
    answers = dict(zip(alone, pool.map(lambda c: _alone(client, c.accession), alone), strict=True))
    gone: set[str] = set()
    saved: list[raw.RawRow] = []
    report.kept += len(kept)
    for candidate, (state, package) in answers.items():
        if state is _State.FAILED:
            report.failed += 1
        elif state is _State.GONE:
            gone.add(candidate.accession)
        elif left := [r for r in candidate.runs if not _has_run(package, r)]:
            report.runs_gone += len(left)
            saved.append(raw.RawRow(candidate.accession, package, date))
        else:
            report.kept += 1
    report.gone += len(gone)
    if gone:
        raw.write_removals(root, raw.SRA_XML, gone)
        report.removals[raw.SRA_XML] = report.removals.get(raw.SRA_XML, 0) + len(gone)
    by_bucket: dict[str, list[raw.RawRow]] = defaultdict(list)
    for row in saved:
        by_bucket[raw.bucket_of(raw.SRA_XML, row.accession)].append(row)
    for bucket, rows in sorted(by_bucket.items()):
        todo = tuple(sorted(row.accession for row in rows))
        raw.write_part(root, raw.Piece(raw.SRA_XML, bucket, todo), rows)
    failed = {c.accession for c, (state, _) in answers.items() if state is _State.FAILED}
    return {c.accession for c in batch} - failed


def _batch_kept(
    client: EntrezClient, pool: ThreadPoolExecutor, batch: list[_Candidate]
) -> set[str]:
    """Return the candidates a batched fetch serves with every candidate run."""

    def one(chunk: list[_Candidate]) -> set[str]:
        ids = [c.accession for c in chunk]
        params = {"db": "sra", "id": ",".join(ids), "rettype": "full", "retmode": "xml"}
        try:
            packages = split_packages(client.eutil_text("efetch", params))
        except EntrezError:
            return set()
        return {
            c.accession
            for c in chunk
            if (package := packages.get(c.accession)) and all(_has_run(package, r) for r in c.runs)
        }

    if len(batch) < 2:
        return set()
    size = eutils.BATCH
    chunks = [batch[i : i + size] for i in range(0, len(batch), size)]
    return {a for got in pool.map(one, chunks) for a in got}


def _alone(client: EntrezClient, accession: str) -> tuple[_State, str]:
    """Ask for one experiment alone; return its state and, when served, its package."""
    params = {"db": "sra", "id": accession, "rettype": "full", "retmode": "xml"}
    try:
        text = client.eutil_text("efetch", params)
    except EntrezError as err:
        if ids_empty(err):
            return _State.GONE, ""
        logger.warning("could not confirm %s: %s", accession, err)
        return _State.FAILED, ""
    if package := split_packages(text).get(accession):
        return _State.SERVED, package
    if re.search(_EMPTY_IDS, text) or re.search(_NOT_PUBLIC.format(acc=re.escape(accession)), text):
        return _State.GONE, ""
    logger.warning(
        "could not confirm %s: an answer that is neither a record nor not-found", accession
    )
    return _State.FAILED, ""


def _has_run(package: str, run: str) -> bool:
    return re.search(rf'<RUN\b[^>]*\baccession="{re.escape(run)}"', package) is not None


def _count(listing: Path) -> SraRemovalReport:
    report = SraRemovalReport()
    for candidate in _read(listing):
        report.candidates += 1
        report.non_public += candidate.non_public
        report.unreached += candidate.unreached
        report.with_runs += bool(candidate.runs)
    return report


def _read(listing: Path) -> Iterator[_Candidate]:
    """Yield the candidates, a batch of rows at a time."""
    for chunk in pq.ParquetFile(listing).iter_batches(batch_size=BATCH):
        values = chunk.to_pydict()
        for accession, non_public, unreached, runs in zip(
            values["accession"],
            values["non_public"],
            values["unreached"],
            values["runs"],
            strict=True,
        ):
            yield _Candidate(accession, bool(non_public), bool(unreached), tuple(runs or ()))


def _batches(items: Iterator[_Candidate], size: int) -> Iterator[list[_Candidate]]:
    batch: list[_Candidate] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _settled(path: Path) -> set[str]:
    return set(path.read_text(encoding="utf-8").split()) if path.is_file() else set()


def _pending_removals(root: Path) -> set[str]:
    return {a for path in raw.part_files(root, raw.SRA_XML) for a in raw.removed(path)}


def _tab_non_public(snap: snapshots.Snapshot, folder: Path) -> Path:
    """Spill the tab's experiment and run rows whose status is set and not ``live``."""
    out = folder / "tab.parquet"
    if out.is_file():
        return out
    tmp = temp_beside(out)
    try:
        with pq.ParquetWriter(tmp, _TAB_SCHEMA) as writer:
            rows: list[tuple[str, str, str]] = []
            for row in snapshots.tab_rows(
                snap.path,
                lambda r: (
                    r.get("Status", "") not in ("", "live")
                    and r.get("Type") in ("EXPERIMENT", "RUN")
                ),
                line_filter=lambda line: "\tlive\t" not in line,
            ):
                rows.append((row["Accession"], row["Type"], row.get("Experiment", "")))
                if len(rows) >= _ROWS:
                    writer.write_table(_table(rows, _TAB_SCHEMA))
                    rows = []
            writer.write_table(_table(rows, _TAB_SCHEMA))
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _reached(root: Path, folder: Path) -> Path:
    """Write the experiments live Series reach: SOFT, tab GSM rows and Entrez links."""
    live = _live_series(root)
    listed = eutils.experiment_list(root, series=live, out=folder / "reached-list.parquet")
    out = folder / "reached.parquet"
    links = raw.source_files(root, raw.ENTREZ_LINKS)
    tmp = temp_beside(out)
    try:
        with extract.connect(root) as con:
            con.register("live", pa.table({"accession": sorted(live)}, schema=_LIVE_SCHEMA))
            linked = ""
            if links:
                linked = f"""UNION
                  SELECT unnest(regexp_extract_all(raw_text, {sql_string(_SRA_LINK)}, 1))
                  FROM {read_parquet_sql(links)}
                  WHERE accession IN (SELECT accession FROM live)"""
            con.execute(
                f"""COPY (
                  SELECT accession FROM read_parquet({sql_string(str(listed))})
                  {linked}
                ) TO {sql_string(str(tmp))} (FORMAT parquet)"""
            )
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _live_series(root: Path) -> set[str]:
    """Return every Series the newest ``geo-series`` snapshot lists."""
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES)
    with gzip.open(snap.path, "rt", encoding="utf-8") as fh:
        return {json.loads(line)["accession"] for line in fh if line.strip()}


def _table(rows: list[tuple[str, ...]], schema: pa.Schema) -> pa.Table:
    columns = list(zip(*rows, strict=True)) if rows else [[]] * len(schema)
    return pa.table(dict(zip(schema.names, columns, strict=True)), schema=schema)


def _candidate_table(rows: list[tuple[str, bool, bool, list[str]]]) -> pa.Table:
    schema = pa.schema(
        [
            ("accession", pa.string()),
            ("non_public", pa.bool_()),
            ("unreached", pa.bool_()),
            ("runs", pa.list_(pa.string())),
        ]
    )
    return _table(rows, schema)  # type: ignore[arg-type]
