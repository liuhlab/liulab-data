"""Gaps, build notes and the coverage check of the target Series list.

The GEO fetch records each target Series it could not save as a ``raw/geo-gaps/`` row made
by :func:`gap_row`. The build writes ``gaps.parquet``: every target Series it does not hold,
with the recorded reason, or ``unexplained`` when none was recorded. A Series held later
leaves the gaps whatever its old row says. Everything here reads and writes Parquet files
through DuckDB, so memory is bounded by DuckDB's cap, not by the store.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import extract, raw, targets

#: The download site answered 404 for the Series' family SOFT.
SOFT_MISSING = "soft-missing"
#: Retries were exhausted; a rerun clears it.
FETCH_FAILED = "fetch-failed"
#: Not held and no known reason.
UNEXPLAINED = "unexplained"
#: Every gap reason. A new reason is added here, on purpose.
REASONS = (SOFT_MISSING, FETCH_FAILED, UNEXPLAINED)

#: Note kind: a target Series whose SOFT ``!Series_type`` names no sequencing.
TYPE_DISAGREES = "series type disagrees"
#: Note kind: a GEO relation label not in the fixed list, kept verbatim.
UNKNOWN_LABEL = "unknown GEO relation label"
#: Note kind: a record whose copies differ; the lowest-numbered copy is kept.
COPIES_DIFFER = "copies differ"
#: Note kind: an SRA experiment NCBI served no package for.
NO_PACKAGE = "no SRA package"
#: Note kind: an SRA experiment whose saved package is not well-formed XML; it adds no record.
UNPARSABLE_PACKAGE = "unparsable SRA package"
#: Note kind: a listed SRA experiment whose package fetch failed; a rerun asks again.
#: It is not a Series gap.
PACKAGE_FETCH_FAILED = FETCH_FAILED

#: The Parquet key-value metadata key of ``gaps.parquet`` holding the snapshot date.
DATE_KEY = "snapshot_date"
#: The key holding the round's sample fraction; absent for the whole list.
FRACTION_KEY = "fraction"
#: How many failing Series :func:`coverage` names.
NAMED = 50

NOTE_SCHEMA = pa.schema(
    [("accession", pa.string()), ("kind", pa.string()), ("detail", pa.string())]
)


@dataclass(frozen=True, slots=True)
class Note:
    """One build note: the accession it is about, its kind and a one-line detail."""

    accession: str
    kind: str
    detail: str


@dataclass(frozen=True, slots=True)
class Coverage:
    """Check 1 over one build.

    Parameters
    ----------
    snapshot_date : str
        The ``geo-series`` snapshot the target list comes from.
    target, held : int
        Target Series, and how many of them the build holds.
    gaps : dict of str to int
        Gap rows by reason, every reason listed.
    failing : tuple of (str, str)
        Up to ``NAMED`` (accession, problem) pairs, sorted.
    failures : int
        Every failing Series, named or not.
    failed_experiments : int
        SRA experiments whose package fetch failed, from the build notes; not Series gaps.
    """

    snapshot_date: str
    target: int
    held: int
    gaps: dict[str, int]
    failing: tuple[tuple[str, str], ...]
    failures: int
    failed_experiments: int = 0

    @property
    def passed(self) -> bool:
        """Whether target = held + gaps and no gap is ``unexplained``."""
        return self.failures == 0


def gap_row(series: str, reason: str, fetch_date: str, detail: str = "") -> raw.RawRow:
    """Return the ``raw/geo-gaps/`` row recording why the fetch could not save a Series.

    Parameters
    ----------
    series : str
        The target Series.
    reason : str
        ``SOFT_MISSING`` or ``FETCH_FAILED``.
    fetch_date : str
        The fetch date.
    detail : str
        One line, such as the last error; optional.

    Raises
    ------
    ValueError
        If the reason is not a recordable one.

    Examples
    --------
    >>> gap_row("GSE1", SOFT_MISSING, "2026-10-14").raw_text
    'soft-missing'
    """
    if reason not in (SOFT_MISSING, FETCH_FAILED):
        raise ValueError(f"not a recordable gap reason: {reason!r}")
    line = " ".join(detail.split())
    return raw.RawRow(series, f"{reason}\n{line}" if line else reason, fetch_date)


def write_gaps(
    root: Path, build_dir: Path, target_list: Path, date: str, fraction: float | None = None
) -> dict[str, object]:
    """Write ``gaps.parquet`` from the target list, ``holdings.parquet`` and ``raw/geo-gaps/``.

    ``fraction`` names the round's sample the target list is, so coverage re-reads the same.

    Returns
    -------
    dict
        ``snapshot_date``, ``target`` and ``held`` counts, and ``gaps``: the count by reason,
        every reason listed; ``fraction`` too for a round's sample.
    """
    out = build_dir / "gaps.parquet"
    kv = f"{DATE_KEY}: {extract.sql_string(date)}"
    if fraction is not None:
        kv += f", {FRACTION_KEY}: {extract.sql_string(repr(fraction))}"
    reasons = ", ".join(extract.sql_string(r) for r in REASONS[:-1])
    with extract.connect(root) as con:
        con.execute(
            f"""COPY (
            WITH g AS ({_gap_rows(root)})
            SELECT t.accession,
              CASE WHEN g.reason IN ({reasons}) THEN g.reason ELSE '{UNEXPLAINED}' END
                AS reason,
              CASE WHEN g.reason IS NULL OR g.reason IN ({reasons}) THEN g.detail
                ELSE 'recorded reason: ' || g.reason END AS detail
            FROM (SELECT DISTINCT accession FROM {_read(target_list)}) t
            LEFT JOIN g USING (accession)
            WHERE t.accession NOT IN ({_held_series(build_dir)})
            ORDER BY CAST(substr(t.accession, 4) AS BIGINT)
            ) TO {extract.sql_string(str(out))}
            (FORMAT parquet, KV_METADATA {{{kv}}})"""
        )
        counts = dict(
            con.execute(f"SELECT reason, count(*) FROM {_read(out)} GROUP BY 1").fetchall()
        )
        target = _count(con, f"SELECT count(DISTINCT accession) FROM {_read(target_list)}")
    by_reason = {reason: int(counts.get(reason, 0)) for reason in REASONS}
    summary: dict[str, object] = {"snapshot_date": date}
    if fraction is not None:
        summary["fraction"] = fraction
    return summary | {
        "target": target,
        "held": target - sum(by_reason.values()),
        "gaps": by_reason,
    }


def write_notes(
    root: Path,
    build_dir: Path,
    target_list: Path,
    extra: Iterable[Note] = (),
    experiments: Path | None = None,
) -> dict[str, int]:
    """Write ``notes.parquet``: type disagreements, failed package fetches, then ``extra``.

    A failed package fetch is an experiment on the E-utilities lane's list ``experiments``
    that ``raw/sra-xml/`` does not hold.

    Returns
    -------
    dict of str to int
        The note count by kind.
    """
    out = build_dir / "notes.parquet"
    series = build_dir / "records" / "geo-series.parquet"
    rows = list(extra)
    extra_table = pa.table(
        {
            "accession": [n.accession for n in rows],
            "kind": [n.kind for n in rows],
            "detail": [n.detail for n in rows],
        },
        schema=NOTE_SCHEMA,
    )
    disagree = (
        f"""SELECT t.accession, '{TYPE_DISAGREES}' AS kind,
              'snapshot: ' || t.gdstype || ' | SOFT: ' || coalesce(s.types, 'no type') AS detail
            FROM {_read(target_list)} t
            JOIN (SELECT accession FROM {_read(series)}) r USING (accession)
            LEFT JOIN (
              SELECT accession, string_agg(DISTINCT f.value, '; ' ORDER BY f.value) AS types,
                bool_or(contains(lower(f.value), {extract.sql_string(targets.SEQUENCING)}))
                  AS sequencing
              FROM (SELECT accession, unnest(fields) AS f FROM {_read(series)})
              WHERE f.name = 'Series_type' GROUP BY accession
            ) s USING (accession)
            WHERE NOT coalesce(s.sequencing, false)"""
        if series.is_file()
        else "SELECT * FROM extra WHERE false"
    )
    unfetched = "SELECT * FROM extra WHERE false"
    saved = root / "raw" / raw.SRA_XML
    if experiments is not None and experiments.is_file():
        held = (
            f"SELECT accession FROM {_read(saved / '*.parquet')}"
            if any(saved.glob("*.parquet"))
            else "SELECT NULL::VARCHAR AS accession WHERE false"
        )
        unfetched = (
            f"SELECT accession, '{PACKAGE_FETCH_FAILED}' AS kind, "
            f"'no package saved; a rerun asks again' AS detail FROM {_read(experiments)} "
            f"WHERE accession NOT IN ({held})"
        )
    with extract.connect(root) as con:
        con.register("extra", extra_table)
        con.execute(
            f"COPY (SELECT * FROM ({disagree} UNION ALL {unfetched} UNION ALL "
            "SELECT * FROM extra) "
            f"ORDER BY kind, accession, detail) TO {extract.sql_string(str(out))} (FORMAT parquet)"
        )
        counts = con.execute(f"SELECT kind, count(*) FROM {_read(out)} GROUP BY 1 ORDER BY 1")
        return {str(kind): int(n) for kind, n in counts.fetchall()}


def coverage(root: Path, build_dir: Path) -> Coverage:
    """Check one build against the target list of the snapshot its gaps were made from.

    The target list is re-read from the snapshot, so a stale or partial ``gaps.parquet``
    fails rather than agreeing with itself. A round's build is checked against its sample.

    Raises
    ------
    StoreBuildError
        If the build has no ``gaps.parquet`` or its snapshot is gone.
    """
    gaps_file = build_dir / "gaps.parquet"
    if not gaps_file.is_file():
        raise StoreBuildError(
            f"{build_dir} has no gaps.parquet; rebuild with `labdata store build`"
        )
    meta = pq.read_metadata(gaps_file).metadata or {}
    date = meta[DATE_KEY.encode()].decode()
    fraction = meta.get(FRACTION_KEY.encode())
    target_list = targets.target_list(root, date, None if fraction is None else float(fraction))
    with extract.connect(root) as con:
        con.execute(f"CREATE VIEW t AS SELECT DISTINCT accession FROM {_read(target_list)}")
        con.execute(f"CREATE VIEW h AS {_held_series(build_dir)}")
        con.execute(f"CREATE VIEW g AS SELECT accession, reason, detail FROM {_read(gaps_file)}")
        target = _count(con, "SELECT count(*) FROM t")
        held = _count(con, "SELECT count(*) FROM t WHERE accession IN (SELECT accession FROM h)")
        by_reason = dict(con.execute("SELECT reason, count(*) FROM g GROUP BY 1").fetchall())
        problems = f"""
            SELECT accession, 'not held and no gap row' FROM t
              WHERE accession NOT IN (SELECT accession FROM h)
              AND accession NOT IN (SELECT accession FROM g)
            UNION ALL SELECT accession, 'gap row for a held Series' FROM g
              WHERE accession IN (SELECT accession FROM h)
            UNION ALL SELECT accession, 'gap row for a Series not on the target list' FROM g
              WHERE accession NOT IN (SELECT accession FROM t)
            UNION ALL SELECT accession, '{UNEXPLAINED}' || coalesce(': ' || detail, '') FROM g
              WHERE reason = '{UNEXPLAINED}'"""
        failures = _count(con, f"SELECT count(*) FROM ({problems})")
        failing = con.execute(f"SELECT * FROM ({problems}) ORDER BY 1, 2 LIMIT {NAMED}").fetchall()
        notes = build_dir / "notes.parquet"
        failed_experiments = (
            _count(
                con,
                f"SELECT count(*) FROM {_read(notes)} WHERE kind = '{PACKAGE_FETCH_FAILED}'",
            )
            if notes.is_file()
            else 0
        )
    return Coverage(
        snapshot_date=date,
        target=target,
        held=held,
        gaps={reason: int(by_reason.get(reason, 0)) for reason in (*REASONS, *by_reason)},
        failing=tuple((str(a), str(p)) for a, p in failing),
        failures=failures,
        failed_experiments=failed_experiments,
    )


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def _read(path: Path) -> str:
    return f"read_parquet({extract.sql_string(str(path))})"


def _held_series(build_dir: Path) -> str:
    return (
        f"SELECT accession FROM {_read(build_dir / 'holdings.parquet')} WHERE type = 'GEO Series'"
    )


def _gap_rows(root: Path) -> str:
    """Return SQL for the recorded gap rows: accession, reason and detail (or NULL)."""
    folder = root / "raw" / raw.GEO_GAPS
    if not any(folder.glob("*.parquet")):
        return (
            "SELECT NULL::VARCHAR AS accession, NULL::VARCHAR AS reason, "
            "NULL::VARCHAR AS detail WHERE false"
        )
    nl = "chr(10)"
    return (
        f"SELECT accession, split_part(raw_text, {nl}, 1) AS reason, "
        f"CASE WHEN strpos(raw_text, {nl}) > 0 "
        f"THEN substr(raw_text, strpos(raw_text, {nl}) + 1) END AS detail "
        f"FROM {_read(folder / '*.parquet')}"
    )
