"""The build reader: the one way to find a build and read its tables.

It owns where a build lives under the store root, a DuckDB connection capped in memory and
threads, and the decoding of table rows into domain objects by column name. Lookups come back
through ``fetchall``, so they import neither pandas nor pyarrow; only ``table`` returns Arrow.
DuckDB's temp files go in the reader's own cache folder, never the store.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Collection, Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from labdata._cache import liulab_data_cache_dir
from labdata.exceptions import NotAStoreError
from labdata.store._limits import duckdb_memory, step_threads
from labdata.store.model import RawText, Record, SupplementaryFile
from labdata.store.root import MARKER, read_marker

if TYPE_CHECKING:
    import pyarrow as pa

#: The folder under the store root that holds one folder per build.
BUILDS = "builds"

#: The folder of one build holding one record table per record type.
RECORDS = "records"
LINKS_BY_FROM = "links_by_from.parquet"
LINKS_BY_TO = "links_by_to.parquet"
HOLDINGS = "holdings.parquet"
KNOWN = "known.parquet"
SUPPLEMENTARY_FILES = "supplementary_files.parquet"
GAPS = "gaps.parquet"
NOTES = "notes.parquet"

#: The record-table columns a ``Record`` is decoded from, by name.
RECORD_COLUMNS = (
    "accession",
    "type",
    "status",
    "submitted",
    "updated",
    "published",
    "fields",
    "content_hash",
    "snapshot_date",
)
#: The link-table columns, in the order of a link row.
LINK_COLUMNS = ("from", "to", "relation", "source")
#: The supplementary-file columns, by ``SupplementaryFile`` field name.
SUPPLEMENTARY_COLUMNS = ("owner", "name", "url", "size", "date", "type", "archive")

#: A link row: from, to, relation, source.
LinkRow = tuple[str, str, str, str]


def build_path(root: Path, build_id: str) -> Path:
    """Return the folder of one build under the store root, which may not exist."""
    return root / BUILDS / build_id


def build_folders(root: Path) -> list[Path]:
    """Return every finished build folder under the root, sorted; temp folders are left out."""
    return sorted(p for p in (root / BUILDS).glob("*") if p.is_dir() and not p.name.startswith("."))


def table_path(build: Path, name: str) -> Path:
    """Return the path of one table file, such as ``HOLDINGS``, inside a build folder."""
    return build / name


def records_path(build: Path, record_type: str) -> Path:
    """Return the path of one record type's table inside a build folder."""
    return build / RECORDS / f"{record_type.lower().replace(' ', '-')}.parquet"


def current_build(root: Path) -> str | None:
    """Return the build id the marker names; ``None`` with no marker or no build yet."""
    if not (root / MARKER).is_file():
        return None
    build = read_marker(root).get("build")
    return str(build) if build else None


class BuildReader:
    """Read one build folder, one query per call.

    Every lookup takes a collection, so a call costs one query whatever the batch size.
    ``calls`` counts the queries by method.

    Parameters
    ----------
    path : pathlib.Path
        The build folder.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.calls: Counter[str] = Counter()
        self._con: duckdb.DuckDBPyConnection | None = None

    @classmethod
    def current(cls, root: Path) -> BuildReader:
        """Return a reader over the build the store's marker names.

        Raises
        ------
        NotAStoreError
            If the root has no marker, or the marker names no build.
        """
        build = read_marker(root).get("build")
        if not build:
            raise NotAStoreError(f"{root / MARKER} names no build; run `labdata store build`")
        return cls(build_path(root, str(build)))

    @property
    def build_id(self) -> str:
        """The build's id, its folder name."""
        return self.path.name

    def table_path(self, name: str) -> Path:
        """Return the path of one table file of this build, such as ``HOLDINGS``."""
        return table_path(self.path, name)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The reader's DuckDB connection, opened on first use.

        Its memory cap and threads follow ``LABDATA_MEMORY_MB`` and ``LABDATA_THREADS``.
        """
        if self._con is None:
            self._con = duckdb.connect(
                ":memory:",
                config={
                    "temp_directory": str(liulab_data_cache_dir() / "duckdb"),
                    "memory_limit": duckdb_memory(),
                    "threads": step_threads(),
                },
            )
        return self._con

    def records(self, accessions: Collection[str]) -> dict[str, Record]:
        """Return the held records among ``accessions``, without links; absent ones are left out."""
        self.calls["records"] += 1
        rows = self._dicts(
            f"SELECT {_columns(RECORD_COLUMNS)} FROM read_parquet({self._records()}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        )
        return {row["accession"]: _record(row) for row in rows}

    def links(
        self, accessions: Collection[str], relations: Collection[str] | None = None
    ) -> list[LinkRow]:
        """Return every link row with either end in ``accessions``, sorted.

        ``relations`` limits the rows to those forward names.
        """
        self.calls["links"] += 1
        accs = _list(accessions)
        where_rel = (
            "" if relations is None else f" AND relation IN (SELECT unnest({_list(relations)}))"
        )
        cols = _columns(LINK_COLUMNS)
        rows = self._dicts(
            f"SELECT {cols} FROM read_parquet({self._file(LINKS_BY_FROM)}) "
            f'WHERE "from" IN (SELECT unnest({accs})){where_rel} '
            f"UNION SELECT {cols} FROM read_parquet({self._file(LINKS_BY_TO)}) "
            f'WHERE "to" IN (SELECT unnest({accs})){where_rel} '
            f"ORDER BY {cols}"
        )
        return [(r["from"], r["to"], r["relation"], r["source"]) for r in rows]

    def held(self, accessions: Collection[str]) -> set[str]:
        """Return which of ``accessions`` the build holds."""
        self.calls["held"] += 1
        rows = self._dicts(
            f"SELECT accession FROM read_parquet({self._file(HOLDINGS)}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        )
        return {row["accession"] for row in rows}

    def known(self, accessions: Collection[str]) -> dict[str, str]:
        """Return which of ``accessions`` a snapshot lists, each with its snapshot date."""
        self.calls["known"] += 1
        rows = self._dicts(
            "SELECT accession, max(snapshot_date) AS snapshot_date "
            f"FROM read_parquet({self._file(KNOWN)}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)})) GROUP BY accession"
        )
        return {row["accession"]: row["snapshot_date"] for row in rows}

    def snapshot_dates(self) -> dict[str, str]:
        """Return each snapshot source's date, read from the known table's metadata."""
        self.calls["snapshot_dates"] += 1
        rows = self._dicts(
            f"SELECT decode(value) AS dates FROM parquet_kv_metadata({self._file(KNOWN)}) "
            "WHERE decode(key) = 'snapshot_dates'"
        )
        return json.loads(rows[0]["dates"]) if rows else {}

    def raw(self, accessions: Collection[str]) -> dict[str, RawText]:
        """Return each held accession's raw text and content hash."""
        self.calls["raw"] += 1
        rows = self._dicts(
            "SELECT accession, raw_text, content_hash "
            f"FROM read_parquet({self._records()}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        )
        return {row["accession"]: RawText(row["raw_text"], row["content_hash"]) for row in rows}

    def supplementary_files(self, owners: Collection[str]) -> list[SupplementaryFile]:
        """Return the supplementary files of ``owners``, sorted by owner, name and URL."""
        self.calls["supplementary_files"] += 1
        rows = self._dicts(
            f"SELECT {_columns(SUPPLEMENTARY_COLUMNS)} "
            f"FROM read_parquet({self._file(SUPPLEMENTARY_FILES)}) "
            f"WHERE owner IN (SELECT unnest({_list(owners)})) ORDER BY owner, name, url"
        )
        return [SupplementaryFile(**row) for row in rows]

    def table(self, record_type: str, fields: Sequence[str], where: str | None = None) -> pa.Table:
        """Return one record type's typed columns and the values of ``fields``, by accession.

        Each field becomes a list column holding every value of that name, in source order.
        ``where`` is a DuckDB condition over the columns. This call imports pyarrow.
        """
        self.calls["table"] += 1
        path = records_path(self.path, record_type)
        source = _quote(str(path)) if path.is_file() else self._records()
        columns = [
            _columns(c for c in RECORD_COLUMNS if c != "fields"),
            *(
                f"list_transform(list_filter(fields, lambda x: x.name = {_quote(name)}), "
                f"lambda x: x.value) AS {_ident(name)}"
                for name in fields
            ),
        ]
        condition = f"type = {_quote(record_type)}" + ("" if where is None else f" AND ({where})")
        return self.connection.execute(
            f"SELECT {', '.join(columns)} FROM read_parquet({source}) "
            f"WHERE {condition} ORDER BY accession"
        ).to_arrow_table()

    def _records(self) -> str:
        return _quote(str(self.path / RECORDS / "*.parquet"))

    def _file(self, name: str) -> str:
        return _quote(str(self.table_path(name)))

    def _dicts(self, sql: str) -> list[dict[str, Any]]:
        """Run ``sql`` and return its rows as dicts keyed by column name."""
        cursor = self.connection.execute(sql)
        names = [d[0] for d in cursor.description or ()]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _record(row: dict[str, Any]) -> Record:
    """Decode one record row, keyed by column name, into a ``Record`` with no links."""
    return Record(
        accession=row["accession"],
        type=row["type"],
        status=row["status"],
        submitted=row["submitted"],
        updated=row["updated"],
        published=row["published"],
        fields=tuple((pair["name"], pair["value"]) for pair in row["fields"]),
        content_hash=row["content_hash"],
        snapshot_date=row["snapshot_date"],
        links=(),
    )


def _columns(names: Iterable[str]) -> str:
    return ", ".join(_ident(n) for n in names)


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _list(values: Collection[str]) -> str:
    """Return a SQL VARCHAR list literal.

    Values are inlined, not bound: binding a parameter makes DuckDB import pandas.
    """
    return "[" + ", ".join(_quote(v) for v in sorted(values)) + "]::VARCHAR[]"


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
