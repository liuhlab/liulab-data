"""The DuckDB backend: one query per call over one build's sorted Parquet tables.

Every method takes a collection. Results come back as plain tuples through ``fetchall``, so
this path imports neither pandas nor pyarrow. DuckDB's temp files go in the reader's own
cache folder, never the store.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Collection
from pathlib import Path
from typing import Any

import duckdb

from labdata._cache import liulab_data_cache_dir

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

    def raw_many(self, accessions: Collection[str]) -> dict[str, tuple[str, str]]:
        """Return each held accession's raw text and content hash."""
        self.calls["raw_many"] += 1
        rows = self._con.execute(
            f"SELECT accession, raw_text, content_hash FROM read_parquet({self._records}) "
            f"WHERE accession IN (SELECT unnest({_list(accessions)}))"
        ).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}


def _list(values: Collection[str]) -> str:
    """Return a SQL VARCHAR list literal.

    Values are inlined, not bound: binding a parameter makes DuckDB import pandas.
    """
    return "[" + ", ".join(_quote(v) for v in sorted(values)) + "]::VARCHAR[]"


def _quote(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"
