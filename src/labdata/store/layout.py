"""The table layout: every build table's file, columns, sort key and writer settings.

It is the only code that writes a build table. A writer hands :func:`write` a DuckDB query;
the rows go out sorted by the table's key, in row groups of the declared size, with the
declared compression. Importing this module does not import pyarrow, so the build reader
takes its column names from here and keeps its lookups free of pyarrow.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

#: Column type names of a declaration, each an Arrow type.
STRING = "string"
INT64 = "int64"
#: A record's source fields, in source order: a list of name and value pairs.
FIELDS = "fields"

#: Rows a record table's row group holds; a lookup reads whole row groups.
RECORD_ROW_GROUP = 4096
#: Rows any other table's row group holds.
ROW_GROUP = 122_880

_BATCH_ROWS = 8192


@dataclass(frozen=True, slots=True)
class Table:
    """One build table as declared.

    Attributes
    ----------
    name : str
        The file name in the build folder; for records, the folder of one file per type.
    columns : tuple of tuple of str
        Each column's name and type name (``STRING``, ``INT64`` or ``FIELDS``), in order.
    sort_key : tuple of str
        The DuckDB ``ORDER BY`` terms the rows are written in.
    compression : str
        The Parquet codec of every column.
    compression_level : int
        The codec's level.
    row_group_rows : int
        Rows per row group; only the last row group holds fewer.
    """

    name: str
    columns: tuple[tuple[str, str], ...]
    sort_key: tuple[str, ...]
    compression: str = "zstd"
    compression_level: int = 3
    row_group_rows: int = ROW_GROUP

    @property
    def column_names(self) -> tuple[str, ...]:
        """The column names, in order."""
        return tuple(name for name, _ in self.columns)

    def schema(self) -> pa.Schema:
        """Return the table's Arrow schema; this call imports pyarrow."""
        import pyarrow as pa

        types = {
            STRING: pa.string(),
            INT64: pa.int64(),
            FIELDS: pa.list_(pa.struct([("name", pa.string()), ("value", pa.string())])),
        }
        return pa.schema([(name, types[kind]) for name, kind in self.columns])

    def path(self, build: Path) -> Path:
        """Return the table's path inside a build folder."""
        return build / self.name


def _strings(*names: str) -> tuple[tuple[str, str], ...]:
    return tuple((name, STRING) for name in names)


RECORDS = Table(
    "records",
    (
        *_strings("accession", "type", "status", "submitted", "updated", "published"),
        ("fields", FIELDS),
        *_strings("raw_text", "content_hash", "snapshot_date"),
    ),
    ("accession",),
    row_group_rows=RECORD_ROW_GROUP,
)
_LINK_COLUMNS = _strings("from", "to", "relation", "source")
LINKS_BY_FROM = Table(
    "links_by_from.parquet", _LINK_COLUMNS, ('"from"', '"to"', "relation", "source")
)
LINKS_BY_TO = Table("links_by_to.parquet", _LINK_COLUMNS, ('"to"', '"from"', "relation", "source"))
HOLDINGS = Table("holdings.parquet", _strings("accession", "type"), ("accession",))
KNOWN = Table(
    "known.parquet", _strings("accession", "source", "snapshot_date"), ("accession", "source")
)
SUPPLEMENTARY_FILES = Table(
    "supplementary_files.parquet",
    (*_strings("owner", "name", "url"), ("size", INT64), *_strings("date", "type", "archive")),
    ("owner", "name", "coalesce(url, '')"),
)
GAPS = Table(
    "gaps.parquet",
    _strings("accession", "reason", "detail"),
    ("CAST(substr(accession, 4) AS BIGINT)",),
)
NOTES = Table(
    "notes.parquet", _strings("accession", "kind", "detail"), ("kind", "accession", "detail")
)

#: Every build table.
TABLES = (
    RECORDS,
    LINKS_BY_FROM,
    LINKS_BY_TO,
    HOLDINGS,
    KNOWN,
    SUPPLEMENTARY_FILES,
    GAPS,
    NOTES,
)


def record_stem(record_type: str) -> str:
    """Return a record type's file stem, such as ``sra-experiment``."""
    return record_type.lower().replace(" ", "-")


def records_path(build: Path, record_type: str) -> Path:
    """Return the path of one record type's table inside a build folder."""
    return RECORDS.path(build) / f"{record_stem(record_type)}.parquet"


def sql_string(text: str) -> str:
    """Return ``text`` as a quoted SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def write(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    out: Path,
    queries: str | Iterable[str],
    metadata: dict[str, str] | None = None,
) -> None:
    """Write ``table`` to ``out`` from the rows of ``queries``, as declared.

    Each query's rows are sorted by the table's key and appended in turn, so several queries
    must cover increasing, disjoint key ranges. The writer streams: it holds one row group and
    one batch at a time, whatever the table's size.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        The connection the queries run on.
    table : Table
        The declared table.
    out : pathlib.Path
        The file to write.
    queries : str or iterable of str
        DuckDB queries selecting at least the table's columns, by name.
    metadata : dict of str to str or None
        Key-value metadata stored in the file.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = table.schema().with_metadata(metadata)
    columns = ", ".join(_ident(name) for name in table.column_names)
    size = table.row_group_rows
    held: list[pa.RecordBatch] = []
    count = 0
    with pq.ParquetWriter(
        out, schema, compression=table.compression, compression_level=table.compression_level
    ) as writer:
        for query in [queries] if isinstance(queries, str) else queries:
            reader = con.execute(
                f"SELECT {columns} FROM ({query}) ORDER BY {', '.join(table.sort_key)}"
            ).to_arrow_reader(min(size, _BATCH_ROWS))
            for batch in reader:
                held.append(batch.cast(schema))
                count += batch.num_rows
                while count >= size:
                    rows = pa.Table.from_batches(held, schema)
                    writer.write_table(rows.slice(0, size), row_group_size=size)
                    held = rows.slice(size).to_batches()
                    count -= size
        if count:
            writer.write_table(pa.Table.from_batches(held, schema), row_group_size=size)


def check(build: Path) -> list[str]:
    """Return how each table of a build differs from its declaration; empty if none does.

    Each problem names the file and the setting: columns, compression, row groups or order.
    """
    import duckdb
    import pyarrow.parquet as pq

    problems: list[str] = []
    con = duckdb.connect(":memory:")
    try:
        for table in TABLES:
            path = table.path(build)
            files = sorted(path.glob("*.parquet")) if table is RECORDS else [path]
            for file in files:
                name = str(file.relative_to(build))
                if not file.is_file():
                    problems.append(f"{name}: missing")
                    continue
                parquet = pq.ParquetFile(file)
                problems += [f"{name}: {p}" for p in _differences(table, parquet)]
                if not _sorted(con, table, file):
                    problems.append(f"{name}: rows not sorted by {', '.join(table.sort_key)}")
    finally:
        con.close()
    return problems


def _differences(table: Table, parquet: pq.ParquetFile) -> list[str]:
    out: list[str] = []
    schema = parquet.schema_arrow.remove_metadata()
    declared = table.schema()
    if not schema.equals(declared):
        out.append(f"columns are {_columns(schema)}, declared {_columns(declared)}")
    meta = parquet.metadata
    groups = [meta.row_group(i) for i in range(meta.num_row_groups)]
    codecs = sorted(
        {g.column(c).compression for g in groups for c in range(g.num_columns)} - {"UNCOMPRESSED"}
    )
    if codecs and codecs != [table.compression.upper()]:
        out.append(f"compression is {', '.join(codecs)}, declared {table.compression.upper()}")
    rows = [g.num_rows for g in groups]
    size = table.row_group_rows
    if any(n != size for n in rows[:-1]) or any(n > size for n in rows[-1:]):
        out.append(f"row groups hold {rows} rows, declared {size}")
    return out


def _sorted(con: duckdb.DuckDBPyConnection, table: Table, file: Path) -> bool:
    """Return whether the file's rows are in the order of the table's sort key."""
    row = con.execute(
        f"""SELECT count(*) FROM (
              SELECT file_row_number,
                row_number() OVER (ORDER BY {", ".join(table.sort_key)}, file_row_number) - 1
                  AS rank
              FROM read_parquet({sql_string(str(file))}, file_row_number = true))
            WHERE file_row_number <> rank"""
    ).fetchone()
    return row is not None and row[0] == 0


def _columns(schema: pa.Schema) -> str:
    return "(" + ", ".join(f"{f.name}: {f.type}" for f in schema) + ")"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
