"""Removals: confirm which held records are gone, and drop raw rows no target Series reaches.

Both write removal parts for pack to apply (see :mod:`labdata.store.raw`); neither touches
``raw/``. A candidate is removed only when a single-accession fetch confirms it is gone:
a record-level "not found" message in a 200 reply, NCBI's "ID list is empty", or, for a run,
its absence from its experiment's served package. Any other answer keeps the row, so the
next refresh asks again. A gone Series takes its rows in every Series-keyed source with it.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import AccessionError, EntrezError
from labdata.geo.sra_records import split_packages
from labdata.ncbi.entrez import EntrezClient, ids_empty
from labdata.store import eutils, extract, gaps, raw, snapshots, targets
from labdata.store._files import temp_beside
from labdata.store.fetch import GDS_OFFSET, experiments_under, fetched_by_hand
from labdata.store.layout import sql_string
from labdata.store.model import kind_of

#: The raw sources keyed by GEO Series: a gone Series' rows leave each of them.
SERIES_KEYED = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.GEO_GAPS, raw.ENTREZ_LINKS, raw.PUBMED_SI)
#: Confirmation fetches in flight; the client's limiter sets the rate.
INFLIGHT = 4
#: Candidates confirmed between writes of removal parts, so a cut-off run keeps its work.
BATCH = 1_000

_GONE = re.compile(
    r"<ERROR>[^<]*(?:cannot get document summary|ID list is empty|not found)[^<]*</ERROR>",
    re.I,
)
_RELATION = re.compile(
    r"^!Series_relation = (?:SuperSeries of|SubSeries of|Reanalysis of): (GSE\d+)", re.M
)
_LINKED = re.compile(r"\b(PRJ[A-Z]{1,2}\d+|[SED]RP\d+)\b")
_EMPTY = raw.sha256("")
_TAB_SCHEMA = pa.schema(
    [("accession", pa.string()), ("kind", pa.string()), ("experiment", pa.string())]
)
_ACCESSIONS = pa.schema([("accession", pa.string())])
_ROWS = 500_000
logger = logging.getLogger(__name__)


class _State(Enum):
    GONE = "gone"
    SERVED = "served"
    FAILED = "failed"


@dataclass
class ConfirmReport:
    """What one confirmation run found: candidates asked, confirmed gone, kept, failed.

    ``runs_gone`` counts runs absent from their experiment's served package; ``removals``
    the accessions each source's removal parts list.
    """

    asked: int = 0
    gone: int = 0
    kept: int = 0
    failed: int = 0
    runs_gone: int = 0
    removals: dict[str, int] = field(default_factory=dict)


def candidates(root: Path, extra: Iterable[str] = ()) -> list[str]:
    """Return the held accessions that may be gone, sorted.

    A candidate is a held Series the newest ``geo-series`` snapshot does not list, a held
    experiment or a run of one with a non-public status in the newest ``SRA_Accessions.tab``,
    a Series this refresh saved as ``soft-missing``, an experiment it saved with an empty
    package, a re-fetched experiment it was served none for (:func:`note_unserved`), or one
    of ``extra``. A run is given as ``SRR…@SRX…``, with its experiment.
    """
    found = set(extra)
    listed = _gds_series(root)
    found |= {gse for gse in _held_series(root) if gse not in listed}
    found |= _non_public(root)
    for path in raw.part_files(root, raw.GEO_GAPS):
        for gse, text in _part_rows(path, "raw_text"):
            if text.split("\n", 1)[0] == gaps.SOFT_MISSING:
                found.add(gse)
    for path in raw.part_files(root, raw.SRA_XML):
        found |= {srx for srx, digest in _part_rows(path, "hash") if digest == _EMPTY}
    if (noted := _unserved_path(root)).is_file():
        found |= set(noted.read_text().split())
    return sorted(found)


def note_unserved(root: Path, accessions: Iterable[str]) -> None:
    """Keep re-fetched experiments NCBI served none for as candidates for the next confirm."""
    if new := set(accessions):
        path = _unserved_path(root)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.writelines(f"{a}\n" for a in sorted(new))


def clear_unserved(root: Path) -> None:
    """Forget the noted unserved experiments once a confirmation run has asked for them."""
    _unserved_path(root).unlink(missing_ok=True)


def _unserved_path(root: Path) -> Path:
    return root / "work" / "removals" / "unserved.txt"


def confirm(
    root: Path, accessions: Iterable[str], *, client: EntrezClient, date: str
) -> ConfirmReport:
    """Ask NCBI for each candidate alone and write removal parts for those confirmed gone.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    accessions : iterable of str
        Candidates from :func:`candidates`: GEO Series, SRA experiments and ``SRR…@SRX…``
        runs. A candidate a pending removal part already lists is not asked again.
    client : EntrezClient
        The NCBI seam.
    date : str
        The fetch date of a package saved again because a run left it.

    Returns
    -------
    ConfirmReport
        Counts for this run.
    """
    pending = _pending_removals(root)
    runs: dict[str, set[str]] = defaultdict(set)
    asked: set[str] = set()
    for accession in accessions:
        run, _, experiment = accession.partition("@")
        if experiment:
            runs[experiment].add(run)
            asked.add(experiment)
        elif _kind(accession) in ("GEO Series", "SRA experiment"):
            asked.add(accession)
        else:
            logger.warning("not confirmed, as only Series, experiments and runs are: %s", accession)
    todo = sorted(asked - pending)
    report = ConfirmReport(asked=len(todo))
    with ThreadPoolExecutor(INFLIGHT, thread_name_prefix="confirm") as pool:
        for start in range(0, len(todo), BATCH):
            batch = todo[start : start + BATCH]
            states = dict(zip(batch, pool.map(lambda a: _state(client, a), batch), strict=True))
            _apply(root, states, runs, report, date)
    logger.info(
        "confirmed %d of %d candidates gone and %d runs; %d kept, %d failed and kept",
        report.gone,
        report.asked,
        report.runs_gone,
        report.kept,
        report.failed,
    )
    return report


def reach(root: Path) -> dict[str, int]:
    """Write removal parts for every raw row that no target Series reaches.

    A target Series reaches itself, the Series its SOFT names as SuperSeries of, SubSeries
    of or Reanalysis of (in turn), and the experiments, BioProjects and SRA studies those
    Series' SOFT, Entrez links and packages name. The newest ``geo-series`` snapshot's whole
    target list counts, and so does every accession ``labdata store fetch`` was given: an
    experiment, the experiments a BioProject or SRA study has, and their ``[si]`` rows. A
    record a pending removal part deletes reaches nothing.

    Returns
    -------
    dict of str to int
        The accessions listed for removal, per source.
    """
    target = targets.target_list(root)
    series = set(pq.read_table(target, columns=["accession"]).column(0).to_pylist())
    by_hand = fetched_by_hand(root)
    kinds = {a: _kind(a) for a in by_hand}
    series |= {a for a, kind in kinds.items() if kind == "GEO Series"}
    parents = {a for a, kind in kinds.items() if kind in ("BioProject", "SRA study")}
    pending = _pending_removals(root)
    reached = _closure(root, series - pending, pending)
    unreached: dict[str, set[str]] = {
        source: {a for a in _held(root, source) if a not in reached} for source in SERIES_KEYED
    }
    extra = {a for a, kind in kinds.items() if kind == "SRA experiment"}
    extra |= experiments_under(root, parents)
    wanted = _experiments(root, reached, extra - pending)
    linked = _linked(root, reached, target, wanted, unreached[raw.PUBMED_SI])
    unreached[raw.PUBMED_SI] -= linked | parents
    unreached[raw.SRA_XML] = _unreached_experiments(root, wanted)
    out = {}
    for source, gone in unreached.items():
        if gone:
            raw.write_removals(root, source, gone)
            out[source] = len(gone)
            logger.info("%s: %d records no target Series reaches", source, len(gone))
    return out


def _state(client: EntrezClient, accession: str) -> tuple[_State, str]:
    """Ask for one accession alone; return its state and, when served, its package text."""
    uid = ""
    if accession.startswith("GSE"):
        uid = str(GDS_OFFSET["GSE"] + int(accession[3:]))
        params = {"db": "gds", "id": uid}
        util = "esummary"
    else:
        params = {"db": "sra", "id": accession, "rettype": "full", "retmode": "xml"}
        util = "efetch"
    try:
        text = client.eutil_text(util, params)
    except EntrezError as err:
        if ids_empty(err):
            return _State.GONE, ""
        logger.warning("could not confirm %s: %s", accession, err)
        return _State.FAILED, ""
    if util == "esummary" and re.search(rf"<Id>{uid}</Id>", text):
        return _State.SERVED, ""
    if util == "efetch" and (package := split_packages(text).get(accession)):
        return _State.SERVED, package
    if _GONE.search(text):
        return _State.GONE, ""
    logger.warning(
        "could not confirm %s: an answer that is neither a record nor not-found", accession
    )
    return _State.FAILED, ""


def _apply(
    root: Path,
    states: dict[str, tuple[_State, str]],
    runs: dict[str, set[str]],
    report: ConfirmReport,
    date: str,
) -> None:
    """Write one batch's removal parts and the packages a gone run left."""
    gone = {a for a, (state, _) in states.items() if state is _State.GONE}
    report.gone += len(gone)
    report.failed += sum(state is _State.FAILED for state, _ in states.values())
    report.kept += sum(state is _State.SERVED for state, _ in states.values())
    removals = {raw.SRA_XML: {a for a in gone if not a.startswith("GSE")}}
    series = {a for a in gone if a.startswith("GSE")}
    for source in SERIES_KEYED:
        removals[source] = series
    for source, accessions in removals.items():
        held = _holding(root, source, accessions)
        if held:
            raw.write_removals(root, source, held)
            report.removals[source] = report.removals.get(source, 0) + len(held)
    saved: list[raw.RawRow] = []
    for experiment, (state, package) in states.items():
        if state is _State.SERVED and runs.get(experiment):
            left = {r for r in runs[experiment] if not _has_run(package, r)}
            if left:
                report.runs_gone += len(left)
                saved.append(raw.RawRow(experiment, package, date))
    by_bucket: dict[str, list[raw.RawRow]] = defaultdict(list)
    for row in saved:
        by_bucket[raw.bucket_of(raw.SRA_XML, row.accession)].append(row)
    for bucket, rows in sorted(by_bucket.items()):
        todo = tuple(sorted(row.accession for row in rows))
        raw.write_part(root, raw.Piece(raw.SRA_XML, bucket, todo), rows)


def _kind(accession: str) -> str:
    try:
        return kind_of(accession)
    except AccessionError:
        return ""


def _has_run(package: str, run: str) -> bool:
    return re.search(rf'<RUN\b[^>]*\baccession="{re.escape(run)}"', package) is not None


def _pending_removals(root: Path) -> set[str]:
    return {
        accession
        for source in (raw.GEO_SOFT, raw.SRA_XML)
        for path in raw.part_files(root, source)
        for accession in raw.removed(path)
    }


def _files(root: Path, source: str) -> list[Path]:
    """Return a source's bucket files and row parts."""
    paths = sorted((root / "raw" / source).glob("*.parquet"))
    return paths + [p for p in raw.part_files(root, source) if not raw.removed(p)]


def _held(root: Path, source: str) -> set[str]:
    """Return every accession a source holds, packed or in a part."""
    return {a for path in _files(root, source) for a, _ in _part_rows(path, "accession")}


def _holding(root: Path, source: str, accessions: set[str]) -> set[str]:
    """Return which of ``accessions`` a source holds, packed or in a part."""
    if not accessions:
        return set()
    buckets = {raw.bucket_of(source, a) for a in accessions}
    paths = [p for p in _files(root, source) if p.name.split(".", 1)[0] in buckets]
    return {a for path in paths for a, _ in _part_rows(path, "accession") if a in accessions}


def _part_rows(path: Path, column: str) -> Iterator[tuple[str, str]]:
    """Yield a file's accessions with one more column, a batch at a time."""
    columns = ["accession"] if column == "accession" else ["accession", column]
    for batch in pq.ParquetFile(path).iter_batches(columns=columns):
        values = batch.to_pydict()
        yield from zip(values["accession"], values[column], strict=True)


def _held_series(root: Path) -> set[str]:
    return {a for source in SERIES_KEYED for a in _held(root, source) if a.startswith("GSE")}


def _gds_series(root: Path) -> set[str]:
    """Return every Series the newest ``geo-series`` snapshot lists."""
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES)
    with gzip.open(snap.path, "rt", encoding="utf-8") as fh:
        return {json.loads(line)["accession"] for line in fh if line.strip()}


def _non_public(root: Path) -> set[str]:
    """Return held experiments, and ``SRR…@SRX…`` runs of held ones, the tab marks non-public."""
    snap = snapshots.snapshot(root, snapshots.SRA_ACCESSIONS)
    tab = _tab_non_public(snap, root / "work" / "removals" / f"tab-{snap.date}.parquet")
    files = _files(root, raw.SRA_XML)
    if not files:
        return set()
    held = f"read_parquet([{', '.join(sql_string(str(p)) for p in files)}])"
    with extract.connect(root) as con:
        rows = con.execute(
            f"""SELECT t.accession, t.kind, t.experiment FROM read_parquet({sql_string(str(tab))}) t
                WHERE (t.kind = 'EXPERIMENT' AND t.accession IN (SELECT accession FROM {held}))
                   OR (t.kind = 'RUN' AND t.experiment IN (SELECT accession FROM {held}))"""
        ).fetchall()
    return {acc if kind == "EXPERIMENT" else f"{acc}@{exp}" for acc, kind, exp in rows}


def _tab_non_public(snap: snapshots.Snapshot, out: Path) -> Path:
    """Spill the tab's experiment and run rows whose status is set and not ``live``."""
    if out.is_file():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
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
                    writer.write_table(_tab_table(rows))
                    rows = []
            writer.write_table(_tab_table(rows))
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _tab_table(rows: list[tuple[str, str, str]]) -> pa.Table:
    columns = list(zip(*rows, strict=True)) if rows else [[]] * len(_TAB_SCHEMA)
    return pa.table(dict(zip(_TAB_SCHEMA.names, columns, strict=True)), schema=_TAB_SCHEMA)


def _closure(root: Path, series: set[str], pending: set[str]) -> set[str]:
    """Return the Series reached from ``series`` through SOFT relation lines, in turn.

    A Series in ``pending``, about to be removed, names none.
    """
    named: dict[str, set[str]] = defaultdict(set)
    for path in _files(root, raw.GEO_SOFT):
        for gse, text in _part_rows(path, "raw_text"):
            if gse not in pending:
                named[gse] |= set(_RELATION.findall(text)) - pending
    reached = set(series)
    todo = list(series)
    while todo:
        for other in named.get(todo.pop(), ()):
            if other not in reached:
                reached.add(other)
                todo.append(other)
    return reached


def _linked(root: Path, reached: set[str], target: Path, wanted: Path, keys: set[str]) -> set[str]:
    """Return which ``[si]`` keys the reached Series' texts, links or packages name."""
    keys = {k for k in keys if not k.startswith("GSE")}
    if not keys:
        return set()
    names: set[str] = set()
    for prj in pq.read_table(target, columns=["bioproject"]).column(0).to_pylist():
        names.add(prj)
    for source in (raw.GEO_SOFT, raw.ENTREZ_LINKS):
        for path in _files(root, source):
            for gse, text in _part_rows(path, "raw_text"):
                if gse in reached:
                    names |= set(_LINKED.findall(text))
    experiments = set(pq.read_table(wanted, columns=["accession"]).column(0).to_pylist())
    for path in _files(root, raw.SRA_XML):
        for srx, text in _part_rows(path, "raw_text"):
            if srx in experiments:
                names |= set(_LINKED.findall(text))
    return keys & names


def _experiments(root: Path, reached: set[str], extra: set[str]) -> Path:
    """Write the experiments the reached Series name, and ``extra``.

    The Series name them in SOFT, through the tab's GSMs and in Entrez links.
    """
    folder = root / "work" / "removals"
    folder.mkdir(parents=True, exist_ok=True)
    listed = eutils.experiment_list(root, series=reached, out=folder / "reached.parquet")
    linked: set[str] = set()
    for path in _files(root, raw.ENTREZ_LINKS):
        for gse, text in _part_rows(path, "raw_text"):
            if gse in reached:
                linked |= {
                    line.split("\t")[3]
                    for line in text.splitlines()
                    if line.count("\t") >= 3 and line.split("\t")[1] == "sra"
                }
    out = folder / "reached-all.parquet"
    tmp = temp_beside(out)
    try:
        table = pa.concat_tables(
            [
                pq.read_table(listed, columns=["accession"]),
                pa.table({"accession": sorted((linked | extra) - {""})}, schema=_ACCESSIONS),
            ]
        )
        pq.write_table(table, tmp)
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _unreached_experiments(root: Path, wanted: Path) -> set[str]:
    files = _files(root, raw.SRA_XML)
    if not files:
        return set()
    held = f"read_parquet([{', '.join(sql_string(str(p)) for p in files)}])"
    with extract.connect(root) as con:
        rows = con.execute(
            f"""SELECT DISTINCT accession FROM {held}
                WHERE accession NOT IN
                  (SELECT accession FROM read_parquet({sql_string(str(wanted))}))"""
        ).fetchall()
    return {accession for (accession,) in rows}
