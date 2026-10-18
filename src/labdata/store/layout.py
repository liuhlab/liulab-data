"""The table layout: every build table's file, columns, sort key and writer settings.

It is the only code that writes a build table. A writer hands :func:`write` a DuckDB query;
the rows go out sorted by the table's key, in row groups of the declared size, with the
declared compression. Importing this module does not import pyarrow, so the build reader
takes its column names from here and keeps its lookups free of pyarrow.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, NamedTuple

from labdata.exceptions import StoreBuildError

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

#: Column type names of a declaration, each an Arrow type.
STRING = "string"
INT64 = "int64"
DATE = "date"
TIMESTAMP = "timestamp"
#: A record's source fields, in source order: a list of name and value pairs.
FIELDS = "fields"


class _Kind(NamedTuple):
    duckdb: str
    arrow: Callable[[ModuleType], pa.DataType]
    #: Whether a query's text is parsed as the type on write.
    parsed: bool = False


_KINDS = {
    STRING: _Kind("VARCHAR", lambda pa: pa.string()),
    INT64: _Kind("BIGINT", lambda pa: pa.int64()),
    DATE: _Kind("DATE", lambda pa: pa.date32(), parsed=True),
    TIMESTAMP: _Kind("TIMESTAMP", lambda pa: pa.timestamp("us"), parsed=True),
    FIELDS: _Kind(
        "STRUCT(name VARCHAR, value VARCHAR)[]",
        lambda pa: pa.list_(pa.struct([("name", pa.string()), ("value", pa.string())])),
    ),
}

# docs/adr/0003-build-table-layout.md records why the writer settings below were chosen.
#: Rows a record table's row group holds; a lookup reads whole row groups.
RECORD_ROW_GROUP = 2048
#: The zstd level of record tables; every other table takes the default level.
RECORD_LEVEL = 9
#: Rows any other table's row group holds.
ROW_GROUP = 122_880

#: The file in each build folder that lists the build's files and inputs; raw and snapshot
#: folders name theirs the same.
MANIFEST = "manifest.json"

_BATCH_ROWS = 8192


@dataclass(frozen=True, slots=True)
class Table:
    """One build table as declared.

    Attributes
    ----------
    name : str
        The file name in the build folder; for records, the folder of one file per type.
    columns : tuple of tuple of str
        Each column's name and type name (``STRING``, ``INT64``, ``DATE``, ``TIMESTAMP`` or
        ``FIELDS``), in order. A query's text dates are cast to ``DATE`` and ``TIMESTAMP``.
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

    def empty(self) -> str:
        """Return a DuckDB query of no rows with the table's columns and types."""
        columns = ", ".join(
            f"NULL::{_KINDS[kind].duckdb} AS {sql_ident(name)}" for name, kind in self.columns
        )
        return f"SELECT {columns} WHERE false"

    def schema(self) -> pa.Schema:
        """Return the table's Arrow schema; this call imports pyarrow."""
        import pyarrow as pa

        return pa.schema([(name, _KINDS[kind].arrow(pa)) for name, kind in self.columns])

    def path(self, build: Path) -> Path:
        """Return the table's path inside a build folder."""
        return build / self.name


def _strings(*names: str) -> tuple[tuple[str, str], ...]:
    return tuple((name, STRING) for name in names)


def _dates(*names: str) -> tuple[tuple[str, str], ...]:
    return tuple((name, DATE) for name in names)


RECORDS = Table(
    "records",
    (
        *_strings("accession", "type", "status"),
        *_dates("submitted", "updated", "published"),
        ("fields", FIELDS),
        *_strings("raw_text", "content_hash"),
        *_dates("snapshot_date"),
    ),
    ("accession",),
    compression_level=RECORD_LEVEL,
    row_group_rows=RECORD_ROW_GROUP,
)
_LINK_COLUMNS = _strings("from", "to", "relation", "source")
LINKS_BY_FROM = Table(
    "links_by_from.parquet", _LINK_COLUMNS, ('"from"', '"to"', "relation", "source")
)
LINKS_BY_TO = Table("links_by_to.parquet", _LINK_COLUMNS, ('"to"', '"from"', "relation", "source"))
HOLDINGS = Table("holdings.parquet", _strings("accession", "type"), ("accession",))
KNOWN = Table(
    "known.parquet",
    (*_strings("accession", "source"), *_dates("snapshot_date")),
    ("accession", "source"),
)
SUPPLEMENTARY_FILES = Table(
    "supplementary_files.parquet",
    (
        *_strings("owner", "name", "url"),
        ("size", INT64),
        ("date", TIMESTAMP),
        *_strings("type", "archive"),
    ),
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
    """Return the path of one record type's table inside a build folder.

    ``record_type`` may also be the type's file stem, such as ``geo-series``.
    """
    return RECORDS.path(build) / f"{record_stem(record_type)}.parquet"


def record_files(build: Path, record_types: Iterable[str]) -> list[Path]:
    """Return the tables of ``record_types`` inside a build folder, leaving out absent ones."""
    return [p for t in record_types if (p := records_path(build, t)).is_file()]


def manifest_path(build: Path) -> Path:
    """Return the path of a build folder's manifest."""
    return build / MANIFEST


def sql_string(text: str) -> str:
    """Return ``text`` as a quoted SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def sql_ident(name: str) -> str:
    """Return ``name`` as a quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


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

    Raises
    ------
    StoreBuildError
        If a date column holds text that does not parse as its type; the message names the
        file, the column and the value. Only NULL and empty text become NULL.
    """
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = table.schema().with_metadata(metadata)
    columns = ", ".join(_select(name, kind) for name, kind in table.columns)
    size = table.row_group_rows
    held: list[pa.RecordBatch] = []
    count = 0
    with pq.ParquetWriter(
        out, schema, compression=table.compression, compression_level=table.compression_level
    ) as writer:
        try:
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
        except duckdb.ConversionException as error:
            raise StoreBuildError(f"{out.name}: {error}") from error
        if count:
            writer.write_table(pa.Table.from_batches(held, schema), row_group_size=size)


def check(build: Path) -> list[str]:
    """Return how each table of a build differs from its declaration; empty if none does.

    Parquet records each column's codec but not its level, so the level goes unchecked.

    Parameters
    ----------
    build : pathlib.Path
        The build folder.

    Returns
    -------
    list of str
        One line per problem, naming the file and the setting: a missing file, columns,
        compression, row groups or order.
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


def _select(name: str, kind: str) -> str:
    """Return the select term of one column; text dates are cast to the declared type."""
    if _KINDS[kind].parsed:
        # Empty text is the one value that becomes NULL; any other text must parse.
        cast = f"CAST(nullif({sql_ident(name)}, '') AS {_KINDS[kind].duckdb})"
        return f"{cast} AS {sql_ident(name)}"
    return sql_ident(name)
