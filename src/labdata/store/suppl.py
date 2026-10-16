"""Build ``supplementary_files.parquet`` from family SOFT and each Series' ``filelist.txt``.

Names and URLs come from SOFT ``!…_supplementary_file`` lines. Sizes, dates, types and
archive contents come from ``filelist.txt``, where each ``File`` row belongs to the
``Archive`` row above it. A file several Series list takes its facts from the lowest-numbered
Series. No network; the table is one DuckDB query over the build's parts.
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import duckdb

_SUPPL_FIELD = re.compile(r"^(?:Series|Sample|Platform)_supplementary_file(?:_\d+)?$")

#: One ``filelist.txt`` entry: size, date, type and the archive holding it.
Listed = tuple[int, str, str, str | None]


def filelist(text: str) -> dict[str, Listed]:
    """Parse one ``filelist.txt`` into each file name's size, date, type and archive."""
    out: dict[str, Listed] = {}
    archive: str | None = None
    for line in text.splitlines():
        cells = line.split("\t")
        if line.startswith("#") or len(cells) < 5:
            continue
        kind, name, time, size, file_type = cells[:5]
        date = dt.datetime.strptime(time, "%m/%d/%Y %H:%M:%S").isoformat()
        if kind == "Archive":
            out[name] = (int(size), date, file_type, None)
            archive = name
        else:
            out[name] = (int(size), date, file_type, archive)
    return out


def write(
    con: duckdb.DuckDBPyConnection, out: Path, geo_records: list[Path], filelists: list[Path]
) -> None:
    """Write ``supplementary_files.parquet``, sorted by owner accession, then name and URL.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        A connection from :func:`labdata.store.extract.connect`.
    out : pathlib.Path
        The table's path.
    geo_records : list of pathlib.Path
        The build's GEO record tables, whose SOFT fields name the files.
    filelists : list of pathlib.Path
        Parts of ``filelist.txt`` entries: Series, its number, name, size, date, type, archive.
    """
    empty_named = "SELECT NULL::VARCHAR AS owner, NULL::VARCHAR AS url WHERE false"
    named = (
        f"""SELECT DISTINCT accession AS owner, url FROM (
              SELECT accession, regexp_replace(f.value, '^\\s+|\\s+$', '', 'g') AS url, f.name
              FROM (SELECT accession, unnest(fields) AS f FROM read_parquet({_list(geo_records)})))
            WHERE regexp_full_match(name, '{_SUPPL_FIELD.pattern[1:-1]}')
              AND url <> '' AND url <> 'NONE'"""
        if geo_records
        else empty_named
    )
    listed = (
        f"""SELECT name, arg_min({{'series': series, 'size': size, 'date': date,
              'type': type, 'archive': archive}}, number) AS l
            FROM read_parquet({_list(filelists)}) GROUP BY name"""
        if filelists
        else "SELECT NULL::VARCHAR AS name, NULL::STRUCT(series VARCHAR, size BIGINT, "
        "date VARCHAR, type VARCHAR, archive VARCHAR) AS l WHERE false"
    )
    con.execute(
        f"""COPY (SELECT * FROM (
        WITH n AS (SELECT owner, regexp_extract(url, '[^/]*$') AS name, url FROM ({named})),
        l AS ({listed})
        SELECT owner, name, url, l.size::BIGINT AS size, l.date AS date, l.type AS type,
          l.archive AS archive
        FROM n LEFT JOIN l USING (name)
        UNION ALL
        SELECT l.series, name, NULL, l.size::BIGINT, l.date, l.type, l.archive FROM l
        WHERE name NOT IN (SELECT name FROM n)
        ) ORDER BY owner, name, coalesce(url, '')) TO '{_escape(str(out))}' (FORMAT parquet)"""
    )


def _list(paths: list[Path]) -> str:
    return "[" + ", ".join(f"'{_escape(str(p))}'" for p in paths) + "]"


def _escape(text: str) -> str:
    return text.replace("'", "''")
