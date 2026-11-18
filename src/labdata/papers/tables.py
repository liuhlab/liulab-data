"""The paper store's four tables and the paper files they name: the only code that changes them.

Each table is one Parquet file at the paper store root, sorted by its key columns, in row
groups of :data:`ROW_GROUP_ROWS` rows. A step loads them as :class:`PaperTables` and changes
rows only through its operations, each of which pairs a row change with its file change.
A save writes the four tables or none: it writes each to a stage folder, lists them in
:data:`SAVING`, renames them into place, then deletes the list. Loading finishes such a list,
then whatever else a stopped step left, so every step starts from a whole store.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import os
import shutil
from collections.abc import Collection, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from backports import zstd

from labdata._files import temp_beside, write_atomic
from labdata.papers import ladder, paths
from labdata.papers.folder import PENDING, TEMP, PaperFolder
from labdata.papers.root import paper_dir

#: The tables a save has staged and not yet renamed into place.
SAVING = paths.SAVING.rel

#: The folder a save writes its tables to before it lists them.
_STAGE = f"{TEMP}/tables"

#: Downloads the running daily pass has not yet saved to the tables.
JOURNAL = paths.JOURNAL.rel

#: The ending a compressed paper file gets.
SUFFIX = ".zst"

#: File endings stored compressed: XML, plain text, and images in uncompressed formats.
#: Everything else (PDF, xlsx, docx, zip, jpg, png, gif, ...) is stored as it is.
COMPRESSED = frozenset(
    {
        ".xml",
        ".csv",
        ".tsv",
        ".txt",
        ".tab",
        ".json",
        ".htm",
        ".html",
        ".md",
        ".rtf",
        ".xls",
        ".doc",
        ".ppt",
        ".eps",
        ".ps",
        ".svg",
        ".tif",
        ".tiff",
        ".bmp",
        ".fa",
        ".fasta",
        ".bed",
        ".gtf",
        ".gff",
        ".vcf",
        ".sdf",
        ".pdb",
        ".r",
        ".py",
        ".sh",
    }
)


def compressed(name: str) -> bool:
    """Return whether a paper file of this name is stored compressed."""
    return Path(name).suffix.lower() in COMPRESSED


def open_plain(path: Path) -> io.BufferedIOBase:
    """Open a stored paper file for reading, decompressed when it ends in ``.zst``."""
    if path.name.endswith(SUFFIX):
        return zstd.ZstdFile(path, "rb")
    return path.open("rb")


#: Paper statuses: the store holds its XML or PDF; a tier serves it later; only a Paperpile
#: save can serve it; it is retracted.
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
#: lookup. ``retracted`` is true once any source calls the paper retracted. ``other_dois``
#: are DOIs a record may link for the paper besides ``doi``, such as its preprint's.
PAPERS = PaperTable(
    "papers",
    pa.schema(
        [
            pa.field("paper", pa.string(), nullable=False),
            ("pmid", pa.int64()),
            ("pmcid", pa.string()),
            ("doi", pa.string()),
            ("other_dois", pa.list_(pa.string())),
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


class PaperTables:
    """The four tables of one paper store, loaded whole, and the paper files they name.

    A caller reads rows through :attr:`papers`, :attr:`info`, :attr:`files` and
    :attr:`attempts`, and may set a row's fields; it adds, replaces and removes rows only
    through the methods here. Only :mod:`labdata.papers.ladder` sets a paper's ``status``,
    ``reason``, ``xml_tier`` and ``pdf_tier``, as :meth:`save` settles them. Open one with
    :meth:`load`.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self._papers = {r["paper"]: r for r in read(root, PAPERS)}
        self._info = {r["paper"]: r for r in read(root, PAPER_INFO)}
        self._files = {_file_key(r): r for r in read(root, FILES)}
        self._attempts = read(root, ATTEMPTS)
        self._folder = PaperFolder(root)
        self._journal = False

    @classmethod
    def load(cls, root: Path, today: dt.date, *, journal: bool = False) -> PaperTables:
        """Read the paper store at ``root``, first finishing what a stopped step left.

        It finishes a listed save, finishes or drops file moves and deletes by the saved
        ``files`` table, empties the temp folder, then saves the downloads a stopped daily
        pass journaled. The caller holds the paper store's lock.

        Parameters
        ----------
        root : pathlib.Path
            The paper store root.
        today : datetime.date
            The UTC date of the step, which journaled downloads are settled for.
        journal : bool
            Journal each file and attempt put before it changes the tables, so a step killed
            before its save loses no download; the save drops the journal.

        Returns
        -------
        PaperTables
            The tables, loaded.
        """
        _finish_save(root)
        state = cls(root)
        state._folder.recover(state.named())
        journaled = root / JOURNAL
        if journaled.is_file():
            state._take_in(journaled.read_text(encoding="utf-8"))
            state.save(today)
        state._journal = journal
        return state

    @property
    def papers(self) -> Mapping[str, dict[str, Any]]:
        """``papers`` rows by paper."""
        return self._papers

    @property
    def info(self) -> Mapping[str, dict[str, Any]]:
        """``paper_info`` rows by paper."""
        return self._info

    @property
    def files(self) -> Collection[dict[str, Any]]:
        """``files`` rows."""
        return self._files.values()

    @property
    def attempts(self) -> Sequence[dict[str, Any]]:
        """``attempts`` rows."""
        return self._attempts

    def add_paper(self, row: Mapping[str, Any]) -> None:
        """Add a paper's ``papers`` row.

        Raises
        ------
        ValueError
            If the paper has a row already.
        """
        if row["paper"] in self._papers:
            raise ValueError(f"paper {row['paper']} has a row already")
        self._papers[row["paper"]] = _whole(PAPERS, row)

    def put_info(self, row: Mapping[str, Any]) -> None:
        """Add a paper's ``paper_info`` row, or replace the one it has."""
        self._info[row["paper"]] = _whole(PAPER_INFO, row)

    def drop_papers(self, keys: Collection[str]) -> None:
        """Remove papers from every table, but a paper the ``files`` table lists.

        Its files stay on disk, so its rows stay too: the GCS copy deletes a file its row left.

        Parameters
        ----------
        keys : collection of str
            The papers' keys.
        """
        keys = set(keys) - {f["paper"] for f in self.files}
        for key in keys:
            self._papers.pop(key, None)
            self._info.pop(key, None)
        if keys:
            self._attempts[:] = [a for a in self._attempts if a["paper"] not in keys]

    def writing(
        self, path: str | Path, *, compress: bool = False
    ) -> AbstractContextManager[io.BufferedIOBase]:
        """Open the paper file at ``path`` for writing, for :meth:`put_file` to name.

        The file appears whole, and only if the block ends without an error; with
        ``compress``, it is stored zstd-compressed. Threads may write files at once.
        """
        return self._folder.writing(path, compress=compress)

    def temp_dir(self) -> Path:
        """Return the folder for temporary files, made if missing; the next load empties it."""
        return self._folder.temp_dir()

    def put_file(self, row: Mapping[str, Any]) -> None:
        """Add a ``files`` row, or replace the one of the same paper, kind and name.

        A replaced row's file at another path is deleted once a save stops naming it.

        Parameters
        ----------
        row : mapping
            The ``files`` row; a missing column is null.
        """
        row = _whole(FILES, row)
        self._journal_line("file", row)
        old = self._files.get(_file_key(row))
        if old is not None and old["path"] and old["path"] != row["path"]:
            self._folder.delete(old["path"])
        self._files[_file_key(row)] = row

    def drop_file(self, row: Mapping[str, Any]) -> None:
        """Remove the ``files`` row of ``row``'s paper, kind and name.

        Its file is deleted once a save stops naming it.
        """
        old = self._files.pop(_file_key(row))
        if old["path"]:
            self._folder.delete(old["path"])

    def rekey(self, doi: str, key: str, own_doi: str) -> None:
        """Move a DOI paper to ``key``, the PMID or DOI of the paper it is.

        Its rows move now and its files once the save naming them lands; nothing is fetched
        again. ``doi`` joins the paper's ``other_dois`` unless it is the paper's DOI. When the
        store holds ``key``'s paper already, the DOI paper's files join it, but a file of a
        kind and name it holds is dropped.

        Parameters
        ----------
        doi : str
            The DOI paper's key.
        key : str
            The key it moves to.
        own_doi : str
            The DOI of ``key``'s paper.

        Raises
        ------
        KeyError
            If the tables hold no paper ``doi``.
        """
        old_row = self._papers.pop(doi)
        self._info.pop(doi, None)
        if key not in self._papers:
            pmid = int(key) if key.isdigit() else None
            self._papers[key] = {**old_row, "paper": key, "pmid": pmid, "doi": own_doi}
        target = self._papers[key]
        target["doi"] = target.get("doi") or own_doi
        others = {*(target.get("other_dois") or ()), *(old_row.get("other_dois") or ()), doi}
        target["other_dois"] = sorted(others - {target["doi"]}) or None
        old_dir, new_dir = paper_dir(doi), paper_dir(key)
        taken = {(f["kind"], f["name"]) for f in self.files if f["paper"] == key}
        for f in [f for f in self.files if f["paper"] == doi]:
            del self._files[_file_key(f)]
            name = f["name"]
            if f["kind"] in ("xml", "pdf") and name.startswith(old_dir.name):
                name = new_dir.name + name[len(old_dir.name) :]
            path = f["path"]
            if (f["kind"], name) in taken:
                if path:
                    self._folder.delete(path)
                continue
            if path:
                rel = Path(path).relative_to(old_dir)
                new_path = (
                    new_dir / rel.parent / name if f["kind"] != "supplement" else new_dir / rel
                )
                self._folder.move(path, new_path.as_posix())
                path = new_path.as_posix()
            moved = {**f, "paper": key, "name": name, "path": path}
            self._files[_file_key(moved)] = moved
        for attempt in self._attempts:
            if attempt["paper"] == doi:
                attempt["paper"] = key

    def attempt(self, row: Mapping[str, Any]) -> None:
        """Add an ``attempts`` row."""
        row = _whole(ATTEMPTS, row)
        self._journal_line("attempt", row)
        self._attempts.append(row)

    def save(self, today: dt.date) -> None:
        """Settle every paper on the tier ladder, save the four tables whole, then move files.

        A save stopped part-way leaves every old table, or a list from which the next load
        renames every new one into place. The journal goes once the tables hold its rows.
        ``today`` is the UTC date of the step.
        """
        ladder.settle(self, today)
        self._folder.commit(self._save_tables, self.named())
        (self.root / JOURNAL).unlink(missing_ok=True)

    def named(self) -> set[str]:
        """Return every path the ``files`` table names."""
        return {f["path"] for f in self.files if f["path"]}

    def held(self) -> set[str]:
        """Return the papers whose XML or PDF the store holds."""
        return {f["paper"] for f in self.files if ladder.holds(f)}

    def _save_tables(self) -> None:
        stage = self.root / _STAGE
        shutil.rmtree(stage, ignore_errors=True)
        stage.mkdir(parents=True)
        rows: list[tuple[PaperTable, Iterable[Mapping[str, Any]]]] = [
            (PAPERS, self._papers.values()),
            (PAPER_INFO, self._info.values()),
            (FILES, self._files.values()),
            (ATTEMPTS, self._attempts),
        ]
        for table, table_rows in rows:
            write(stage, table, table_rows)
        write_atomic(self.root / SAVING, json.dumps([t.path(self.root).name for t, _ in rows]))
        _finish_save(self.root)

    def _journal_line(self, kind: str, row: Mapping[str, Any]) -> None:
        if not self._journal:
            return
        with (self.root / JOURNAL).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"kind": kind, "row": row}, default=str) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _take_in(self, text: str) -> None:
        """Put the rows a stopped daily pass journaled, but attempts already held."""
        for line in text.splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue  # a line cut short when the pass stopped
            row = entry["row"]
            for column in ("date", "fetched"):
                if row.get(column):
                    row[column] = dt.date.fromisoformat(row[column])
            if entry["kind"] == "file":
                self.put_file(row)
            elif _whole(ATTEMPTS, row) not in self._attempts:
                self.attempt(row)


def heal(root: Path, today: dt.date) -> None:
    """Finish what a stopped step left, as :meth:`PaperTables.load` does.

    It reads the tables only when a step left file moves or a journal. The caller holds the
    paper store's lock.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.
    today : datetime.date
        The UTC date of the step, as for :meth:`PaperTables.load`.
    """
    _finish_save(root)
    if (root / PENDING).is_file() or (root / JOURNAL).is_file():
        PaperTables.load(root, today)


def _finish_save(root: Path) -> None:
    """Rename into place the staged tables a save listed, then drop the list."""
    listed = root / SAVING
    if not listed.is_file():
        return
    stage = root / _STAGE
    for name in json.loads(listed.read_text(encoding="utf-8")):
        if (stage / name).is_file():
            (stage / name).replace(root / name)
    listed.unlink()
    shutil.rmtree(stage, ignore_errors=True)


def _whole(table: PaperTable, row: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``row`` with every column of ``table``; a missing one is null."""
    return {**dict.fromkeys(table.schema.names), **row}


def _file_key(row: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    return row["paper"], row["kind"], row["name"]
