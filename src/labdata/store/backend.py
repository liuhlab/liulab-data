"""The DuckDB backend: one query per call over one build's sorted Parquet tables.

Every method takes a collection. Results come back as plain tuples through
``fetchall``, so this path imports neither pandas nor pyarrow; only ``table`` returns
Arrow. DuckDB's temp files go in the reader's own cache folder, never the store.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Collection, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from labdata._cache import liulab_data_cache_dir

if TYPE_CHECKING:
    import pyarrow as pa

#: A record row: accession, type, status, submitted, updated, published, fields,
#: content_hash, snapshot_date.
RecordRow = tuple[Any, ...]
#: A link row: from, to, relation, source.
LinkRow = tuple[str, str, str, str]


class DuckDbBackend:
    """Read one build folder.

    Parameters
    ----------
    build : pathlib.Path
        The ``builds/<build-id>/`` folder.
    """

    def __init__(self, build: Path) -> None:
        self.build = build
        self.calls: Counter[str] = Counter()
        temp = liulab_data_cache_dir() / "duckdb"
        self._con = duckdb.connect(":memory:", config={"temp_directory": str(temp)})
        self._records = _quote(str(build / "records" / "*.parquet"))
        self._by_from = _quote(str(build / "links_by_from.parquet"))
        self._by_to = _quote(str(build / "links_by_to.parquet"))
        self._holdings = _quote(str(build / "holdings.parquet"))
        self._known = _quote(str(build / "known.parquet"))
        self._suppl = _quote(str(build / "supplementary_files.parquet"))

    def get_many(self, accessions: Collection[str]) -> dict[str, RecordRow]:
        """Return the held records among ``accessions``; absent ones are left out."""
        self.calls["get_many"] += 1
        rows = self._con.execute(
            "SELECT accession, type, status, submitted, updated, published, fields, "
            f"content_hash, snapshot_date FROM read_parquet({self._records}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        ).fetchall()
        return {row[0]: row for row in rows}

    def links_many(
        self, accessions: Collection[str], relations: Collection[str] | None = None
    ) -> list[LinkRow]:
        """Return every link row with either end in ``accessions``, sorted.

        ``relations`` limits the rows to those forward names.
        """
        self.calls["links_many"] += 1
        accs = _list(accessions)
        where_rel = (
            "" if relations is None else f" AND relation IN (SELECT unnest({_list(relations)}))"
        )
        rows = self._con.execute(
            f'SELECT "from", "to", relation, source FROM read_parquet({self._by_from}) '
            f'WHERE "from" IN (SELECT unnest({accs})){where_rel} '
            f'UNION SELECT "from", "to", relation, source FROM read_parquet({self._by_to}) '
            f'WHERE "to" IN (SELECT unnest({accs})){where_rel} '
            'ORDER BY "from", "to", relation, source'
        ).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

    def held_many(self, accessions: Collection[str]) -> set[str]:
        """Return which of ``accessions`` the build holds."""
        self.calls["held_many"] += 1
        rows = self._con.execute(
            f"SELECT accession FROM read_parquet({self._holdings}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        ).fetchall()
        return {row[0] for row in rows}

    def known_many(self, accessions: Collection[str]) -> dict[str, str]:
        """Return which of ``accessions`` a snapshot lists, each with its snapshot date."""
        self.calls["known_many"] += 1
        rows = self._con.execute(
            f"SELECT accession, max(snapshot_date) FROM read_parquet({self._known}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)})) GROUP BY accession"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def snapshot_dates(self) -> dict[str, str]:
        """Return each snapshot source's date, read from ``known.parquet``'s metadata."""
        self.calls["snapshot_dates"] += 1
        rows = self._con.execute(
            f"SELECT decode(value) FROM parquet_kv_metadata({self._known}) "
            "WHERE decode(key) = 'snapshot_dates'"
        ).fetchall()
        return json.loads(rows[0][0]) if rows else {}

    def raw_many(self, accessions: Collection[str]) -> dict[str, tuple[str, str]]:
        """Return each held accession's raw text and content hash."""
        self.calls["raw_many"] += 1
        rows = self._con.execute(
            f"SELECT accession, raw_text, content_hash FROM read_parquet({self._records}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        ).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}

    def table(self, record_type: str, fields: Sequence[str], where: str | None = None) -> pa.Table:
        """Return one record type's typed columns and the values of ``fields``, by accession.

        Each field becomes a list column holding every value of that name, in source order.
        ``where`` is a DuckDB condition over the columns. This call imports pyarrow.
        """
        self.calls["table"] += 1
        path = self.build / "records" / f"{record_type.lower().replace(' ', '-')}.parquet"
        source = _quote(str(path)) if path.is_file() else self._records
        columns = [
            "accession, type, status, submitted, updated, published, content_hash, snapshot_date",
            *(
                f"list_transform(list_filter(fields, lambda x: x.name = {_quote(name)}), "
                f'lambda x: x.value) AS "{name.replace('"', '""')}"'
                for name in fields
            ),
        ]
        condition = f"type = {_quote(record_type)}" + ("" if where is None else f" AND ({where})")
        return self._con.execute(
            f"SELECT {', '.join(columns)} FROM read_parquet({source}) "
            f"WHERE {condition} ORDER BY accession"
        ).to_arrow_table()

    def supplementary_many(self, owners: Collection[str]) -> list[tuple[Any, ...]]:
        """Return the supplementary-file rows of ``owners``, sorted by owner, name and URL."""
        self.calls["supplementary_many"] += 1
        return self._con.execute(
            "SELECT owner, name, url, size, date, type, archive "
            f"FROM read_parquet({self._suppl}) WHERE owner IN (SELECT unnest({_list(owners)})) "
            "ORDER BY owner, name, url"
        ).fetchall()


def _list(values: Collection[str]) -> str:
    """Return a SQL VARCHAR list literal.

    Values are inlined, not bound: binding a parameter makes DuckDB import pandas.
    """
    return "[" + ", ".join(_quote(v) for v in sorted(values)) + "]::VARCHAR[]"


def _quote(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"
