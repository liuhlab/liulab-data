"""The build reader: the one way to find a build and read its tables.

It owns where a build lives under the store root, a DuckDB connection capped in memory and
threads, and the decoding of table rows into domain objects by column name. Lookups come back
through ``fetchall``, so they import neither pandas nor pyarrow; only ``table`` returns Arrow.
DuckDB's temp files go in the reader's own cache folder, never the store.
"""

from __future__ import annotations

import datetime as dt
import json
from collections import Counter
from collections.abc import Collection, Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from labdata._cache import liulab_data_cache_dir
from labdata.exceptions import NotAStoreError
from labdata.store._limits import duckdb_memory, step_threads
from labdata.store.layout import (
    HOLDINGS,
    KNOWN,
    LINKS_BY_FROM,
    LINKS_BY_TO,
    RECORDS,
    SUPPLEMENTARY_FILES,
    Table,
    records_path,
    sql_string,
)
from labdata.store.model import RawText, Record, SupplementaryFile, kind_of
from labdata.store.root import MARKER, read_marker

if TYPE_CHECKING:
    import pyarrow as pa

#: The folder under the store root that holds one folder per build.
BUILDS = "builds"

#: The record columns a ``Record`` is decoded from: all but the raw text.
_RECORD_COLUMNS = tuple(c for c in RECORDS.column_names if c != "raw_text")

#: Keys per lookup query. A query reads each row group its keys fall in, whatever the batch
#: size; a batch bounds the query's text and the rows it holds at once.
_BATCH = 10_000

#: A link row: from, to, relation, source.
LinkRow = tuple[str, str, str, str]


def build_path(root: Path, build_id: str) -> Path:
    """Return the folder of one build under the store root, which may not exist."""
    return root / BUILDS / build_id


def build_folders(root: Path) -> list[Path]:
    """Return every finished build folder under the root, sorted; temp folders are left out."""
    return sorted(p for p in (root / BUILDS).glob("*") if p.is_dir() and not p.name.startswith("."))


def current_build(root: Path) -> str | None:
    """Return the build id the marker names; ``None`` with no marker or no build yet."""
    if not (root / MARKER).is_file():
        return None
    build = read_marker(root).get("build")
    return str(build) if build else None


class BuildReader:
    """Read one build folder.

    Every lookup takes a collection and reads only the row groups that hold its keys; an
    accession lookup reads only the record file its prefix names. ``calls`` counts the calls
    by method.

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
        # The fields come back as one JSON text of names and values: DuckDB hands a list of
        # structs to Python several times slower than json.loads decodes it.
        select = (
            f"{_columns(c for c in _RECORD_COLUMNS if c != 'fields')}, "
            "to_json([list_transform(fields, lambda x: x.name), "
            "list_transform(fields, lambda x: x.value)]) AS fields"
        )
        return {
            row["accession"]: _record(row)
            for file, keys in self._record_files(accessions).items()
            for row in self._lookup(f"SELECT {select} FROM read_parquet({file})", "accession", keys)
        }

    def links(
        self, accessions: Collection[str], relations: Collection[str] | None = None
    ) -> list[LinkRow]:
        """Return every link row with either end in ``accessions``, sorted.

        ``relations`` limits the rows to those forward names.
        """
        self.calls["links"] += 1
        tail = "" if relations is None else f" AND relation IN ({_in_list(relations)})"
        cols = _columns(LINKS_BY_FROM.column_names)
        rows = {
            (r["from"], r["to"], r["relation"], r["source"])
            for table, end in ((LINKS_BY_FROM, "from"), (LINKS_BY_TO, "to"))
            for r in self._lookup(
                f"SELECT {cols} FROM read_parquet({self._file(table)})", end, accessions, tail
            )
        }
        return sorted(rows)

    def held(self, accessions: Collection[str]) -> set[str]:
        """Return which of ``accessions`` the build holds."""
        self.calls["held"] += 1
        select = f"SELECT accession FROM read_parquet({self._file(HOLDINGS)})"
        return {row["accession"] for row in self._lookup(select, "accession", accessions)}

    def known(self, accessions: Collection[str]) -> dict[str, dt.date]:
        """Return which of ``accessions`` a snapshot lists, each with its snapshot date."""
        self.calls["known"] += 1
        rows = self._lookup(
            "SELECT accession, max(snapshot_date) AS snapshot_date "
            f"FROM read_parquet({self._file(KNOWN)})",
            "accession",
            accessions,
            " GROUP BY accession",
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
        return {
            row["accession"]: RawText(row["raw_text"], row["content_hash"])
            for file, keys in self._record_files(accessions).items()
            for row in self._lookup(
                f"SELECT accession, raw_text, content_hash FROM read_parquet({file})",
                "accession",
                keys,
            )
        }

    def supplementary_files(self, owners: Collection[str]) -> list[SupplementaryFile]:
        """Return the supplementary files of ``owners``, sorted by owner, name and URL."""
        self.calls["supplementary_files"] += 1
        rows = self._lookup(
            f"SELECT {_columns(SUPPLEMENTARY_FILES.column_names)} "
            f"FROM read_parquet({self._file(SUPPLEMENTARY_FILES)})",
            "owner",
            owners,
            " ORDER BY owner, name, url",
        )
        return [SupplementaryFile(**row) for row in rows]

    def table(self, record_type: str, fields: Sequence[str], where: str | None = None) -> pa.Table:
        """Return one record type's typed columns and the values of ``fields``, by accession.

        Each field becomes a list column holding every value of that name, in source order.
        ``where`` is a DuckDB condition over the columns. This call imports pyarrow.
        """
        self.calls["table"] += 1
        path = records_path(self.path, record_type)
        source = sql_string(str(path)) if path.is_file() else self._records()
        columns = [
            _columns(c for c in _RECORD_COLUMNS if c != "fields"),
            *(
                f"list_transform(list_filter(fields, lambda x: x.name = {sql_string(name)}), "
                f"lambda x: x.value) AS {_ident(name)}"
                for name in fields
            ),
        ]
        condition = f"type = {sql_string(record_type)}" + (
            "" if where is None else f" AND ({where})"
        )
        return self.connection.execute(
            f"SELECT {', '.join(columns)} FROM read_parquet({source}) "
            f"WHERE {condition} ORDER BY accession"
        ).to_arrow_table()

    def _records(self) -> str:
        return sql_string(str(RECORDS.path(self.path) / "*.parquet"))

    def _file(self, table: Table) -> str:
        return sql_string(str(table.path(self.path)))

    def _record_files(self, accessions: Iterable[str]) -> dict[str, list[str]]:
        """Group ``accessions`` by the record file their prefix names; absent files are left out."""
        files: dict[str, str | None] = {}
        out: dict[str, list[str]] = {}
        for accession in accessions:
            kind = kind_of(accession)
            if kind not in files:
                path = records_path(self.path, kind)
                files[kind] = sql_string(str(path)) if path.is_file() else None
            if (file := files[kind]) is not None:
                out.setdefault(file, []).append(accession)
        return out

    def _lookup(
        self, select: str, key: str, keys: Collection[str], tail: str = ""
    ) -> list[dict[str, Any]]:
        """Run ``select`` over sorted batches of ``keys`` and return every batch's rows.

        Each batch is a literal ``IN`` list on the ``key`` column, followed by ``tail``. Unlike
        a subquery, a literal list lets DuckDB skip each row group whose key range holds none
        of the batch. Batches go in key order, so rows ordered by ``key`` stay in order.
        """
        ordered = sorted(keys)
        return [
            row
            for start in range(0, len(ordered), _BATCH)
            for row in self._dicts(
                f"{select} WHERE {_ident(key)} IN "
                f"({_in_list(ordered[start : start + _BATCH])}){tail}"
            )
        ]

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
        fields=tuple(zip(*json.loads(row["fields"]), strict=True)),
        content_hash=row["content_hash"],
        snapshot_date=row["snapshot_date"],
        links=(),
    )


def _columns(names: Iterable[str]) -> str:
    return ", ".join(_ident(n) for n in names)


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _in_list(values: Iterable[str]) -> str:
    """Return the values as the items of a SQL ``IN`` list.

    Values are inlined, not bound: binding a parameter makes DuckDB import pandas.
    """
    return ", ".join(sql_string(v) for v in values)
