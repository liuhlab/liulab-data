"""The paper store's four tables: their declarations, and the only code that writes them.

Each table is one Parquet file at the paper store root, sorted by its key columns, in row
groups of :data:`ROW_GROUP_ROWS` rows. A step
reads a table whole, changes its rows and writes it back through :func:`write`, which
replaces the file by a rename, so a reader sees the old table or the new one, never a part.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.store._files import temp_beside

#: Paper statuses: the store holds its XML or PDF; a tier serves it later; only a Paperpile
#: save can serve it; it is not a real paper.
STATUSES = ("fetched", "waiting", "manual", "skipped")

#: File kinds in ``files``.
KINDS = ("xml", "pdf", "figure", "supplement")

#: Rows per Parquet row group. Rows are sorted by ``paper``, so a key lookup decodes the
#: one or two groups whose statistics can hold the key, not the whole table.
ROW_GROUP_ROWS = 10_000

_AUTHOR = pa.struct([("name", pa.string()), ("affiliations", pa.list_(pa.string()))])


@dataclass(frozen=True, slots=True)
class PaperTable:
    """One paper store table: its file name, columns and sort order.

    Parameters
    ----------
    name : str
        The file's stem at the paper store root.
    schema : pyarrow.Schema
        The columns; ``paper`` comes first and is never null.
    sort_by : tuple of str
        The columns rows are sorted by.
    key : tuple of str
        The columns that identify a row; empty when rows may repeat.
    allowed : mapping of str to tuple of str
        The values a column may take, where it is a fixed vocabulary.
    """

    name: str
    schema: pa.Schema
    sort_by: tuple[str, ...]
    key: tuple[str, ...] = ()
    allowed: Mapping[str, tuple[str, ...]] | None = None

    def path(self, root: Path) -> Path:
        """Return the table's file under the paper store ``root``."""
        return root / f"{self.name}.parquet"


#: One row per paper: its IDs, status and which tier supplied its XML and PDF. ``added`` is
#: the day it entered the store, ``looked_up`` the day its IDs, details and OpenAlex lookup
#: were last renewed; ``work_type``, ``openalex_pdf`` and ``host_pdf_urls`` come from that
#: lookup. ``retracted`` is true once any source calls the paper retracted.
PAPERS = PaperTable(
    "papers",
    pa.schema(
        [
            pa.field("paper", pa.string(), nullable=False),
            ("pmid", pa.int64()),
            ("pmcid", pa.string()),
            ("doi", pa.string()),
            ("openalex", pa.string()),
            ("published", pa.date32()),
            ("status", pa.string()),
            ("reason", pa.string()),
            ("xml_tier", pa.string()),
            ("pdf_tier", pa.string()),
            ("added", pa.date32()),
            ("looked_up", pa.date32()),
            ("work_type", pa.string()),
            ("openalex_pdf", pa.bool_()),
            ("host_pdf_urls", pa.list_(pa.string())),
            ("retracted", pa.bool_()),
        ]
    ),
    ("paper",),
    key=("paper",),
    allowed={"status": STATUSES},
)

#: One row per paper: details from PubMed, or OpenAlex for a DOI-only paper.
PAPER_INFO = PaperTable(
    "paper_info",
    pa.schema(
        [
            pa.field("paper", pa.string(), nullable=False),
            ("pmid", pa.int64()),
            ("pmcid", pa.string()),
            ("doi", pa.string()),
            ("title", pa.string()),
            ("journal", pa.string()),
            ("published", pa.date32()),
            ("authors", pa.list_(_AUTHOR)),
            ("abstract", pa.string()),
            ("publication_type", pa.list_(pa.string())),
        ]
    ),
    ("paper",),
    key=("paper",),
)

#: One row per file held or only catalogued: the article's XML and PDF, figures and
#: supplements. ``path`` is relative to the paper store root, ends in ``.zst`` for a file
#: stored compressed, and is empty when the file is not downloaded; ``size`` and ``md5`` are
#: of the plain bytes; ``etag`` is the source's ETag of the copy held.
FILES = PaperTable(
    "files",
    pa.schema(
        [
            pa.field("paper", pa.string(), nullable=False),
            ("kind", pa.string()),
            ("name", pa.string()),
            ("path", pa.string()),
            ("size", pa.int64()),
            ("md5", pa.string()),
            ("url", pa.string()),
            ("tier", pa.string()),
            ("label", pa.string()),
            ("caption", pa.string()),
            ("fetched", pa.date32()),
            ("gone", pa.date32()),
            ("etag", pa.string()),
        ]
    ),
    ("paper", "kind", "name"),
    key=("paper", "kind", "name"),
    allowed={"kind": KINDS},
)

#: One row per tier tried for a paper: the paper job's own state, not for readers.
#: ``account`` names the OpenAlex account a paid try spent, by its email, never its key.
ATTEMPTS = PaperTable(
    "attempts",
    pa.schema(
        [
            pa.field("paper", pa.string(), nullable=False),
            ("tier", pa.string()),
            ("date", pa.date32()),
            ("account", pa.string()),
            ("outcome", pa.string()),
            ("detail", pa.string()),
        ]
    ),
    ("paper", "date", "tier", "account", "outcome", "detail"),
)

#: Every paper store table.
TABLES = (PAPERS, PAPER_INFO, FILES, ATTEMPTS)


def read(root: Path, table: PaperTable) -> list[dict[str, Any]]:
    """Return every row of one table, in its sort order; no rows when the file is missing.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.
    table : PaperTable
        One of :data:`TABLES`.

    Returns
    -------
    list of dict
        One dict per row, keyed by column name; dates are ``datetime.date``.
    """
    path = table.path(root)
    if not path.is_file():
        return []
    return pq.read_table(path, schema=table.schema).to_pylist()


def write(root: Path, table: PaperTable, rows: Iterable[Mapping[str, Any]]) -> Path:
    """Replace one table with ``rows``, sorted, through a temp file and a rename.

    The same rows, in any order, give the same file. A write stopped before the rename
    leaves the old table in place.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.
    table : PaperTable
        One of :data:`TABLES`.
    rows : iterable of mapping
        Every row of the new table, keyed by column name; a missing column is null.

    Returns
    -------
    pathlib.Path
        The table's file.

    Raises
    ------
    ValueError
        If a row names an unknown column, has no ``paper``, repeats a key, or holds a value
        outside a column's fixed vocabulary.
    """
    names = set(table.schema.names)
    checked = []
    for row in rows:
        unknown = set(row) - names
        if unknown:
            raise ValueError(f"{table.name}: unknown columns {sorted(unknown)}")
        if not row.get("paper"):
            raise ValueError(f"{table.name}: a row has no paper: {dict(row)}")
        for column, values in (table.allowed or {}).items():
            if row.get(column) is not None and row[column] not in values:
                raise ValueError(f"{table.name}: {column} {row[column]!r} is not one of {values}")
        checked.append(row)
    data = pa.Table.from_pylist(checked, schema=table.schema).sort_by(
        [(c, "ascending") for c in table.sort_by]
    )
    if table.key:
        keys = list(zip(*(data.column(c).to_pylist() for c in table.key), strict=True))
        if len(set(keys)) != len(keys):
            raise ValueError(f"{table.name}: a row's {', '.join(table.key)} appears twice")
    path = table.path(root)
    tmp = temp_beside(path)
    try:
        pq.write_table(
            data, tmp, row_group_size=ROW_GROUP_ROWS, compression="zstd", compression_level=3
        )
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


@dataclass
class PaperTables:
    """The four tables of one paper store, loaded whole for a pass to change.

    Attributes
    ----------
    papers, info : dict of str to dict
        ``papers`` and ``paper_info`` rows by ``paper``.
    files, attempts : list of dict
        ``files`` and ``attempts`` rows.
    """

    papers: dict[str, dict[str, Any]]
    info: dict[str, dict[str, Any]]
    files: list[dict[str, Any]]
    attempts: list[dict[str, Any]]

    @classmethod
    def load(cls, root: Path) -> PaperTables:
        """Read every table of the paper store at ``root``."""
        return cls(
            papers={r["paper"]: r for r in read(root, PAPERS)},
            info={r["paper"]: r for r in read(root, PAPER_INFO)},
            files=read(root, FILES),
            attempts=read(root, ATTEMPTS),
        )

    def save(self, root: Path) -> None:
        """Write every table back, each through a temp file and a rename."""
        write(root, PAPERS, self.papers.values())
        write(root, PAPER_INFO, self.info.values())
        write(root, FILES, self.files)
        write(root, ATTEMPTS, self.attempts)

    def held(self) -> set[str]:
        """Return the papers whose XML or PDF the store holds."""
        return {f["paper"] for f in self.files if f["kind"] in ("xml", "pdf") and f["path"]}
