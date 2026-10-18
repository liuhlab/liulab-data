"""Build the store's tables from ``raw/`` and ``snapshots/``, then switch the marker to them.

No network. Reads family SOFT, ``elink`` results, SRA packages, ``[si]`` results and
``filelist.txt`` from ``raw/``, and the snapshots only through their extracts
(``labdata store extract``). Writes ``builds/<build-id>/``: ``records/<type>.parquet``
(sorted by accession), ``links_by_from.parquet``, ``links_by_to.parquet``,
``holdings.parquet``, ``known.parquet``, ``supplementary_files.parquet``, ``gaps.parquet``
and ``notes.parquet``, with the folder's README and manifest.

Memory is bounded by a chunk, not by the store: workers parse raw buckets a chunk at a time
into Parquet parts under ``work/build/<build-id>/``, and DuckDB merges and sorts those parts
out of core into the tables. The folder appears by a rename once whole, and the marker
switches to it by a rename, so readers never see half a build.
"""

from __future__ import annotations

import datetime as dt
import fcntl
import glob
import itertools
import logging
import re
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import (
    _limits,
    _parse,
    eutils,
    extract,
    gaps,
    known,
    layout,
    provenance,
    raw,
    readme,
    snapshots,
    suppl,
    targets,
)
from labdata.store._files import temp_beside, toml_line, write_atomic
from labdata.store.layout import (
    HOLDINGS,
    KNOWN,
    LINKS_BY_FROM,
    LINKS_BY_TO,
    RECORDS,
    SUPPLEMENTARY_FILES,
    sql_string,
)
from labdata.store.model import RELATIONS
from labdata.store.reader import build_path
from labdata.store.root import MARKER, read_marker
from labdata.store.sources import SOFT

#: The marker key naming the current build.
BUILD_KEY = "build"

#: The marker key holding when the current build was switched to, in UTC.
SWITCHED_KEY = "switched"

#: The marker table mapping each earlier build to when it was switched away from, in UTC.
SWITCHED_AWAY_KEY = "switched_away"

#: The default bytes of source text a parse worker holds before it writes its parts.
PARSE_CHUNK_BYTES = 1 << 26

#: Chunks of raw text one parse task takes, so a large bucket is parsed by several workers.
_TASK_CHUNKS = 4

#: Accessions looked up in one snapshot query.
_LOOKUP_BATCH = 10_000

#: Rows per row group of the wanted snapshot texts; one parse task takes one row group.
_SNAPSHOT_ROW_GROUP = 2048
#: Decompressed record bytes per byte of DuckDB's cap that one sorted write may take.
_RANGE_SHARE = 1 / 16
#: Bytes per unit DuckDB can show its memory cap in, binary and decimal.
_CAP_UNITS = {
    "byte": 1,
    "bytes": 1,
    **{f"{p}iB": 1 << (10 * i) for i, p in enumerate("KMGTPE", 1)},
    **{f"{p}B": 1000**i for i, p in enumerate("KMGTPE", 1)},
}
_TYPED = ("status", "submitted", "updated", "published")
#: The record type that keeps its newest copy; every other type keeps its lowest-numbered one.
_NEWEST_FIRST = layout.record_stem("GEO Sample")

logger = logging.getLogger(__name__)


def build(
    root: Path,
    build_id: str | None = None,
    *,
    workers: int | None = None,
    chunk_bytes: int = PARSE_CHUNK_BYTES,
    fraction: float | None = None,
) -> Path:
    """Build every table into a new ``builds/<build-id>/`` and point the marker at it.

    Parameters
    ----------
    root : pathlib.Path
        The store root, which must carry ``labdata-store.toml``.
    build_id : str or None
        The new build's folder name; defaults to a UTC timestamp.
    workers : int or None
        Parallel parse workers; defaults to :func:`labdata.store._limits.default_workers`.
        ``1`` parses in this process.
    chunk_bytes : int
        Bytes of source text a worker parses before it writes its parts. Peak memory is about
        ``workers`` chunks plus DuckDB's cap (:func:`labdata.store._limits.duckdb_memory`).
    fraction : float or None
        A round's seeded sample of the target Series list; gaps, notes and ``GEO summary``
        links cover only that sample. ``None`` for the whole list.

    Returns
    -------
    pathlib.Path
        The new build folder.

    Raises
    ------
    StoreBuildError
        If a snapshot is missing or not extracted, a build of that id exists, or another
        build of this store is running.
    """
    fraction = targets.check_fraction(fraction)
    with build_lock(root):
        return _build(root, build_id, workers or _limits.default_workers(), chunk_bytes, fraction)


@contextmanager
def build_lock(root: Path) -> Iterator[None]:
    """Hold the store's build lock, ``<root>/work/build.lock``, for one writer at a time.

    Raises
    ------
    StoreBuildError
        If another process or caller holds it.
    """
    lock = root / "work" / "build.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StoreBuildError(
                f"another build, prune or copy of this store is running (it holds {lock}); "
                "wait for it to finish, then try again"
            ) from None
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _build(
    root: Path, build_id: str | None, workers: int, chunk_bytes: int, fraction: float | None
) -> Path:
    build_id = build_id or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out = build_path(root, build_id)
    if out.exists():
        raise StoreBuildError(f"build {build_id} already exists: {out}")
    for folder in sorted(p for p in (root / "raw").glob("*") if p.is_dir()):
        provenance.document_raw(root, folder.name)
    for stale in out.parent.glob(f".{glob.escape(build_id)}.*.tmp"):
        shutil.rmtree(stale, ignore_errors=True)
    work = root / "work" / "build" / build_id
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    tmp = temp_beside(out)
    try:
        # Each parse phase holds its own Ray session, so its workers are gone before DuckDB
        # writes the tables.
        _build_into(root, build_id, work, tmp, workers, chunk_bytes, fraction)
        tmp.rename(out)
    finally:
        shutil.rmtree(work, ignore_errors=True)
        shutil.rmtree(tmp, ignore_errors=True)
    _switch(root, build_id)
    readme.try_write(root)
    return out


def _build_into(
    root: Path,
    build_id: str,
    work: Path,
    tmp: Path,
    workers: int,
    chunk_bytes: int,
    fraction: float | None,
) -> None:
    _, geo_date = extract.done_dir(root, snapshots.GEO_SERIES)
    target_list = targets.target_list(root, geo_date, fraction)
    _summary_links(target_list, work / "links" / "summary.parquet")
    tasks: list[_parse.Task] = []
    for kind in _parse.RAW_KINDS:
        files = sorted((root / "raw" / kind).glob("*.parquet"))
        for inputs in _runs(files, _TASK_CHUNKS * chunk_bytes, workers):
            tasks.append(_parse.Task(kind, inputs, work, len(tasks), chunk_bytes))
    logger.info("parsing %d raw tasks on %d workers", len(tasks), workers)
    parsed = extract.run_parallel(_parse.run, ((t,) for t in tasks), workers)
    empty = [srx for got in parsed for srx in got.empty]
    unparsable = [
        gaps.Note(srx, gaps.UNPARSABLE_PACKAGE, f"not well-formed XML as saved: {reason}")
        for got in parsed
        for srx, reason in got.unparsable
    ]
    logger.info("parsed raw/; checking copies and looking up snapshot records")

    RECORDS.path(tmp).mkdir(parents=True)
    with extract.connect(root) as con:
        copy_notes = [
            note
            for folder in sorted((work / "records").glob("*"))
            for note in _differing_copies(con, folder)
        ]
        wanted = _wanted_snapshots(con, root, work, len(tasks), chunk_bytes, workers)
        logger.info("parsing %d snapshot tasks", len(wanted))
        extract.run_parallel(_parse.run, ((t,) for t in wanted), workers)
        logger.info("writing record, link and supplementary-file tables")
        for folder in sorted((work / "records").glob("*")):
            _write_records(con, root, folder, RECORDS.path(tmp) / f"{folder.name}.parquet")
        _write_links(con, work, tmp)
        _write_holdings(con, tmp)
        suppl.write(
            con,
            SUPPLEMENTARY_FILES.path(tmp),
            sorted(RECORDS.path(tmp).glob("geo-*.parquet")),
            sorted((work / "filelist").glob("*.parquet")),
        )
        labels = _unknown_labels(con, LINKS_BY_FROM.path(tmp))
    logger.info("writing known, gaps and notes")
    known.write(root, KNOWN.path(tmp))
    coverage = gaps.write_gaps(root, tmp, target_list, geo_date, fraction)
    label_notes = [
        gaps.Note(str(v["example"]), gaps.UNKNOWN_LABEL, f"{label}: {v['links']} links")
        for label, v in labels.items()
    ]
    _, sra_date = extract.done_dir(root, snapshots.SRA_ACCESSIONS)
    note_counts = gaps.write_notes(
        root,
        tmp,
        target_list,
        [*copy_notes, *label_notes, *unparsable, *_no_package_notes(root, empty)],
        experiments=eutils.list_path(root, sra_date),
    )
    provenance.document_build(
        root,
        tmp,
        build_id,
        unknown_labels=labels,
        target_series=coverage,
        notes=note_counts,
    )


def _runs(
    files: list[Path], most: int, workers: int
) -> Iterator[tuple[tuple[Path, tuple[int, ...]], ...]]:
    """Split files' row groups into runs, one per task.

    A run holds at most ``most`` uncompressed bytes, and is small enough that every worker
    gets one. A large file is split across tasks; small files share one.
    """
    groups = [
        (path, i, meta.row_group(i).total_byte_size)
        for path in files
        for meta in (pq.read_metadata(path),)
        for i in range(meta.num_row_groups)
    ]
    size = min(most, -(-sum(g[2] for g in groups) // workers))
    run: dict[Path, list[int]] = {}
    held = 0
    for path, i, nbytes in groups:
        if run and held + nbytes > size:
            yield tuple((p, tuple(g)) for p, g in run.items())
            run, held = {}, 0
        run.setdefault(path, []).append(i)
        held += nbytes
    if run:
        yield tuple((p, tuple(g)) for p, g in run.items())


def _summary_links(target_list: Path, out: Path) -> None:
    """Write each target Series' snapshot BioProject link as a link part, batch by batch."""
    out.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(out, _parse.LINK_SCHEMA) as writer:
        batch: list[dict[str, str]] = []
        for r in targets.summary_links(target_list):
            batch.append({"from": r.from_, "to": r.to, "relation": r.relation, "source": r.source})
            if len(batch) == _SNAPSHOT_ROW_GROUP:
                writer.write_table(pa.Table.from_pylist(batch, schema=_parse.LINK_SCHEMA))
                batch = []
        writer.write_table(pa.Table.from_pylist(batch, schema=_parse.LINK_SCHEMA))


def _read(path: Path) -> str:
    """Read one Parquet file, or every ``*.parquet`` file of a folder, listed by name.

    Files are listed, never globbed, so a build id holding glob characters reads as written.
    """
    paths = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    return f"read_parquet([{', '.join(sql_string(str(p)) for p in paths)}])"


def _kept_order(folder: Path) -> tuple[str, str, str]:
    """Return the SQL key whose least copy is kept, the CTE ``u`` it needs and its ``JOIN``.

    A GEO Sample whose copies are all dated keeps its newest copy; everything else keeps its
    lowest-numbered one. ``u`` reads the copies from a CTE ``r``; the ``JOIN`` brings ``u``
    into a query over the copies.
    """
    if folder.name != _NEWEST_FIRST:
        return "{'a': 0, 'n': number, 'o': origin}", "", ""
    age = "date_diff('day', CAST(updated AS DATE), DATE '9999-12-31')"
    return (
        f"{{'a': CASE WHEN u.dated THEN {age} ELSE 0 END, 'n': number, 'o': origin}}",
        "u AS (SELECT accession, bool_and(updated IS NOT NULL) AS dated FROM r "
        "GROUP BY accession),",
        "JOIN u USING (accession)",
    )


def _differing_copies(con: duckdb.DuckDBPyConnection, folder: Path) -> list[gaps.Note]:
    """Note each record whose copies differ: the copy kept, the others and the fields that differ.

    The build keeps the copy ``_kept_order`` ranks first; GEO renders one sample differently
    in two Series' family SOFT files, so a difference is reported, never fatal. ``fields`` is
    read only for records whose copies differ, so memory follows those, not the store.
    """
    parts = _read(folder)
    key, dated, join = _kept_order(folder)
    rows = con.execute(
        f"""WITH r AS (SELECT accession, content_hash, number, origin, updated FROM {parts}),
            m AS (SELECT accession FROM (SELECT DISTINCT accession, content_hash FROM r)
                  GROUP BY accession HAVING count(*) > 1),
            {dated}
            c AS (SELECT accession, content_hash, min({key}) AS first,
                    arg_min(fields, {key}) AS fields
                  FROM {parts} {join} WHERE accession IN (SELECT accession FROM m)
                  GROUP BY accession, content_hash),
            d AS (SELECT accession, min(first) AS kept FROM c GROUP BY accession),
            o AS (SELECT DISTINCT r.accession, {{'n': r.number, 'o': r.origin}} AS at, x.fields
                  FROM r JOIN d USING (accession)
                  JOIN c x ON x.accession = r.accession AND x.content_hash = r.content_hash
                  WHERE x.first <> d.kept)
            SELECT o.accession, d.kept.o, list(o.at.o ORDER BY o.at),
              list_sort(list_distinct(flatten(list(list_transform(
                list_filter(o.fields, f -> NOT list_contains(k.fields, f))
                || list_filter(k.fields, f -> NOT list_contains(o.fields, f)),
                f -> f.name)))))
            FROM o JOIN d USING (accession)
            JOIN c k ON k.accession = d.accession AND k.first = d.kept
            GROUP BY o.accession, d.kept ORDER BY o.accession"""
    ).fetchall()
    if rows:
        logger.warning(
            "kept one copy of %d %s records whose copies differ, e.g. %s",
            len(rows),
            folder.name,
            rows[0][0],
        )
    return [
        gaps.Note(
            str(accession),
            gaps.COPIES_DIFFER,
            f"kept {kept}; {', '.join(others)} "
            f"{'differs' if len(others) == 1 else 'differ'} in: {', '.join(names) or 'text only'}",
        )
        for accession, kept, others, names in rows
    ]


def _wanted_snapshots(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    work: Path,
    first: int,
    chunk_bytes: int,
    workers: int,
) -> list[_parse.Task]:
    """Write the snapshot texts the links name, and return the tasks that parse them.

    BioProjects are the targets of ``BioProject`` links and the ``[si]``-searched accessions;
    BioSamples the targets of ``BioSample`` links.
    """
    links = _read(work / "links")
    searched = root / "raw" / raw.PUBMED_SI
    projects = f"SELECT \"to\" FROM {links} WHERE relation = 'BioProject'"
    if any(searched.glob("*.parquet")):
        projects += f" UNION SELECT accession FROM {_read(searched)}"
    wanted = {
        snapshots.BIOPROJECT: (projects, "PRJ"),
        snapshots.BIOSAMPLE: (f"SELECT \"to\" FROM {links} WHERE relation = 'BioSample'", ""),
    }
    tasks: list[_parse.Task] = []
    for source, (query, prefix) in wanted.items():
        folder, date = extract.done_dir(root, source)
        out = work / "snapshots" / source
        out.mkdir(parents=True)
        accessions = out / "wanted.parquet"
        con.execute(
            f"""COPY (SELECT DISTINCT w FROM ({query}) q(w) WHERE starts_with(w, '{prefix}')
                ORDER BY w) TO {sql_string(str(accessions))} (FORMAT parquet)"""
        )
        # A sorted batch named literally lets DuckDB skip row groups by their accession range.
        wanted_file = pq.ParquetFile(accessions)
        for n, batch in enumerate(wanted_file.iter_batches(batch_size=_LOOKUP_BATCH)):
            listed = ", ".join(sql_string(a) for a in batch.column(0).to_pylist())
            path = out / f"{n:06d}.parquet"
            con.execute(
                f"""COPY (SELECT accession, raw_text FROM read_parquet({extract.parts(folder)})
                    WHERE accession IN ({listed})) TO {sql_string(str(path))}
                    (FORMAT parquet, ROW_GROUP_SIZE {_SNAPSHOT_ROW_GROUP})"""
            )
        wanted_file.close()
        files = sorted(out.glob("[0-9]*.parquet"))
        for inputs in _runs(files, _TASK_CHUNKS * chunk_bytes, workers):
            tasks.append(_parse.Task(source, inputs, work, first + len(tasks), chunk_bytes, date))
    return tasks


def _range_bytes(con: duckdb.DuckDBPyConnection) -> int:
    """Return the decompressed record bytes one sorted write takes: a share of DuckDB's cap."""
    row = con.execute("SELECT current_setting('memory_limit')").fetchone()
    shown = "" if row is None else str(row[0])
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)\s*", shown)
    if match is None or match[2] not in _CAP_UNITS:
        raise StoreBuildError(f"cannot read DuckDB's memory_limit {shown!r} as a size")
    return max(int(float(match[1]) * _CAP_UNITS[match[2]] * _RANGE_SHARE), 1)


def _decompressed_bytes(folder: Path) -> int:
    """Return the bytes of a folder's parts as DuckDB holds them, not as stored."""
    size = 0
    for path in folder.glob("*.parquet"):
        meta = pq.read_metadata(path)
        size += sum(meta.row_group(i).total_byte_size for i in range(meta.num_row_groups))
    return size


def _write_records(con: duckdb.DuckDBPyConnection, root: Path, folder: Path, out: Path) -> None:
    """Write one record type, keeping each record's copy as ``_kept_order`` ranks it.

    A large type is written in accession ranges, each sorted on its own and streamed out in
    order, so DuckDB never sorts more than one range's texts at once.
    """
    count = -(-_decompressed_bytes(folder) // _range_bytes(con))
    bounds: list[str | None] = [None, None]
    if count > 1:
        quantiles = ", ".join(str(i / count) for i in range(1, count))
        cuts = con.execute(
            f"SELECT unnest(quantile_disc(accession, [{quantiles}])) FROM {_read(folder)}"
        ).fetchall()
        bounds = [None, *dict.fromkeys(str(c[0]) for c in cuts), None]
    ranges = []
    for lo, hi in itertools.pairwise(bounds):
        terms = ["TRUE"]
        if lo is not None:
            terms.append(f"accession >= {sql_string(lo)}")
        if hi is not None:
            terms.append(f"accession < {sql_string(hi)}")
        ranges.append(_record_range(root, folder, " AND ".join(terms)))
    layout.write(con, RECORDS, out, ranges)
    logger.info("wrote %s in %d accession ranges", out.stem, len(ranges))


def _record_range(root: Path, folder: Path, where: str) -> str:
    """Return the query of one accession range; SRA records take typed columns from the tab."""
    if folder.name.startswith("sra-"):
        tab, _ = extract.done_dir(root, snapshots.SRA_ACCESSIONS)
        fields = ", ".join(f"{name}: {name}" for name in _TYPED)
        typed_join = (
            f"LEFT JOIN (SELECT accession, min({{{fields}}}) AS x "
            f"FROM read_parquet({extract.parts(tab)}) "
            "WHERE accession IN (SELECT accession FROM r) GROUP BY accession) t USING (accession)"
        )
        typed = ", ".join(f"t.x.{name} AS {name}" for name in _TYPED)
    else:
        typed_join = ""
        typed = ", ".join(f"r.{name}" for name in _TYPED)
    key, dated, join = _kept_order(folder)
    return f"""WITH r AS (SELECT * FROM {_read(folder)} WHERE {where}),
            {dated}
            k AS (SELECT min({{'k': {key}, 's': seq}}).s AS seq
                  FROM r {join} GROUP BY accession)
            SELECT r.accession, r.type, {typed}, r.fields, r.raw_text, r.content_hash,
              r.snapshot_date
            FROM r JOIN k USING (seq) {typed_join}"""


def _write_links(con: duckdb.DuckDBPyConnection, work: Path, out: Path) -> None:
    distinct = work / "links.parquet"
    con.execute(
        f"""COPY (SELECT DISTINCT "from", "to", relation, source
            FROM {_read(work / "links")})
            TO {sql_string(str(distinct))} (FORMAT parquet)"""
    )
    for table in (LINKS_BY_FROM, LINKS_BY_TO):
        layout.write(con, table, table.path(out), f"SELECT * FROM {_read(distinct)}")


def _write_holdings(con: duckdb.DuckDBPyConnection, out: Path) -> None:
    records = RECORDS.path(out)
    query = (
        f"SELECT accession, type FROM {_read(records)}"
        if any(records.glob("*.parquet"))
        else "SELECT NULL::VARCHAR AS accession, NULL::VARCHAR AS type WHERE false"
    )
    layout.write(con, HOLDINGS, HOLDINGS.path(out), query)


def _unknown_labels(con: duckdb.DuckDBPyConnection, links: Path) -> dict[str, dict[str, int | str]]:
    """Count the links under each GEO relation label not in the fixed list, and log each."""
    listed = ", ".join(sql_string(r) for r in RELATIONS)
    rows = con.execute(
        f"""SELECT relation, count(*), min("from") FROM {_read(links)}
            WHERE source = {sql_string(SOFT)} AND relation NOT IN ({listed})
            GROUP BY relation ORDER BY relation"""
    ).fetchall()
    out: dict[str, dict[str, int | str]] = {}
    for label, count, example in rows:
        out[str(label)] = {"links": int(count), "example": str(example)}
        logger.warning(
            "kept unknown GEO relation label %r verbatim: %d links, e.g. %s", label, count, example
        )
    return out


def _no_package_notes(root: Path, experiments: list[str]) -> list[gaps.Note]:
    """Note each SRA experiment saved with no package, with its ``SRA_Accessions.tab`` status."""
    if not experiments:
        return []
    _, rows = extract.lookup(root, snapshots.SRA_ACCESSIONS, experiments)
    status = {str(row[0]): row[1] for row in rows}
    return [
        gaps.Note(
            srx, gaps.NO_PACKAGE, f"SRA_Accessions.tab status: {status.get(srx) or 'not listed'}"
        )
        for srx in sorted(set(experiments))
    ]


def _switch(root: Path, build_id: str) -> None:
    """Point the marker at the new build and record when the old one was switched away from."""
    settings = read_marker(root)
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    old = settings.get(BUILD_KEY)
    if old and old != build_id:
        settings.setdefault(SWITCHED_AWAY_KEY, {})[str(old)] = now
    settings[BUILD_KEY] = build_id
    settings[SWITCHED_KEY] = now
    write_marker(root, settings)


def write_marker(root: Path, settings: dict[str, Any]) -> None:
    """Write the marker whole: its scalar settings, then each table."""
    scalars = [toml_line(k, v) for k, v in settings.items() if not isinstance(v, dict)]
    tables = [
        f"\n[{name}]\n" + "".join(f"{_quoted(k)} = {_quoted(v)}\n" for k, v in rows.items())
        for name, rows in settings.items()
        if isinstance(rows, dict)
    ]
    write_atomic(root / MARKER, "".join(f"{line}\n" for line in scalars) + "".join(tables))


def _quoted(key: str) -> str:
    """Return ``key`` as a quoted TOML key."""
    return toml_line("k", key)[len("k = ") :]
