"""The paper reader: a paper's details, status and files, and the papers a record links.

A record's papers are joined on demand from the GEO/SRA store's current build, read through
its :class:`~labdata.store.reader.BuildReader`, to the paper store's tables. Nothing per
record is stored, so a new build never leaves the answer stale.
"""

from __future__ import annotations

import datetime as dt
import io
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from labdata.exceptions import NotMirrored
from labdata.papers import tables
from labdata.papers._zst import open_plain
from labdata.papers.root import is_placeholder, paper_key
from labdata.store.model import kind_of
from labdata.store.reader import BuildReader

#: The prefixes of a link's far end that name a paper.
_PMID, _DOI = "PMID:", "DOI:"


@dataclass(frozen=True, slots=True)
class Author:
    """One author, with affiliations in the order the paper gives them."""

    name: str
    affiliations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PaperInfo:
    """A paper's details, from PubMed or, for a DOI-only paper, OpenAlex."""

    title: str | None
    journal: str | None
    published: dt.date | None
    authors: tuple[Author, ...]
    abstract: str | None
    publication_type: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PaperFile:
    """One file of a paper, held or only listed.

    ``path`` is absolute, and ``None`` for a file only listed; a file stored compressed ends
    in ``.zst``, and :meth:`open` reads it decompressed. ``size`` and ``md5`` are of the
    plain bytes. ``gone`` is the date its source stopped serving it.
    """

    kind: str
    name: str
    path: Path | None
    size: int | None
    md5: str | None
    url: str | None
    tier: str | None
    label: str | None
    caption: str | None
    fetched: dt.date | None
    gone: dt.date | None

    def open(self) -> io.BufferedIOBase:
        """Open the held file for reading, decompressed.

        Returns
        -------
        io.BufferedIOBase
            A binary file object; close it, or use it in a ``with`` block.

        Raises
        ------
        FileNotFoundError
            If the file is only listed, not held.
        """
        if self.path is None:
            raise FileNotFoundError(f"{self.name} is listed, not held; its URL is {self.url}")
        return open_plain(self.path)


@dataclass(frozen=True, slots=True)
class Paper:
    """One paper: its IDs, status, details and files."""

    key: str
    status: str | None
    reason: str | None
    pmid: int | None
    pmcid: str | None
    doi: str | None
    openalex: str | None
    published: dt.date | None
    xml_tier: str | None
    pdf_tier: str | None
    info: PaperInfo | None
    files: tuple[PaperFile, ...]


@dataclass(frozen=True, slots=True)
class LinkedPaper:
    """A paper a record links, with its status.

    ``status`` is ``None`` when the paper store has not taken the paper in yet.
    """

    key: str
    status: str | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class PaperLinks:
    """The papers one Series or BioProject links, sorted by key."""

    accession: str
    papers: tuple[LinkedPaper, ...]

    @property
    def linked(self) -> bool:
        """Whether the record links any paper; ``False`` means "no paper linked"."""
        return bool(self.papers)

    @property
    def fetched(self) -> bool:
        """Whether any linked paper is fetched."""
        return any(p.status == "fetched" for p in self.papers)


class PaperReader:
    """Read the paper store, joined to one GEO/SRA build's paper links.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.
    build : BuildReader
        The reader of the GEO/SRA store's current build.
    """

    def __init__(self, root: Path, build: BuildReader) -> None:
        self.root = root
        self.build = build

    def paper(self, key: str) -> Paper:
        """Return one paper: its IDs, status, details and files.

        Parameters
        ----------
        key : str
            A PMID, bare or as ``PMID:<n>``, or a DOI; a DOI finds the paper it belongs to
            even when that paper is keyed by its PMID or another DOI.

        Returns
        -------
        Paper
            The paper; ``info`` is ``None`` before its details are fetched.

        Raises
        ------
        ValueError
            If ``key`` is neither a PMID nor a DOI.
        NotMirrored
            If the paper store does not hold the paper.
        """
        wanted = paper_key(key)
        found = self.papers([wanted])
        if wanted not in found:
            raise NotMirrored(f"not in the paper store: {wanted}")
        return found[wanted]

    def papers(self, keys: Iterable[str]) -> dict[str, Paper]:
        """Return many papers, reading each table once; use it to walk many papers.

        Parameters
        ----------
        keys : iterable of str
            PMIDs, bare or as ``PMID:<n>``, or DOIs, as :meth:`paper` takes them.

        Returns
        -------
        dict of str to Paper
            Each held key, as :func:`~labdata.papers.root.paper_key` gives it, in input
            order, mapped to its paper; a key the store does not hold is left out.

        Raises
        ------
        ValueError
            If a key is neither a PMID nor a DOI.
        """
        wanted = list(dict.fromkeys(paper_key(k) for k in keys))
        if not wanted:
            return {}
        dois = {w for w in wanted if not w.isdigit()}
        rows = self._paper_rows(wanted, dois)
        by_key = {r["paper"]: r for r in rows}
        by_doi = _by_doi(rows)
        hits = {w: row for w in wanted if (row := by_key.get(w) or by_doi.get(w))}
        held = {row["paper"] for row in hits.values()}
        info = {r["paper"]: r for r in self._rows(tables.PAPER_INFO, held)} if held else {}
        files: dict[str, list[PaperFile]] = {}
        for f in self._rows(tables.FILES, held) if held else []:
            files.setdefault(f["paper"], []).append(_file(self.root, f))
        return {
            w: Paper(
                key=row["paper"],
                status=row["status"],
                reason=row["reason"],
                pmid=row["pmid"],
                pmcid=row["pmcid"],
                doi=row["doi"],
                openalex=row["openalex"],
                published=row["published"],
                xml_tier=row["xml_tier"],
                pdf_tier=row["pdf_tier"],
                info=_info(info[row["paper"]]) if row["paper"] in info else None,
                files=tuple(files.get(row["paper"], ())),
            )
            for w, row in hits.items()
        }

    def papers_for(self, accessions: Iterable[str]) -> dict[str, PaperLinks]:
        """Map each Series or BioProject to the papers its build links, with their status.

        A DOI link counts only when the paper store holds its paper. A PMID link counts
        before the paper store takes the paper in, unless it is a placeholder.

        Parameters
        ----------
        accessions : iterable of str
            GEO Series and BioProject accessions, in the order wanted back.

        Returns
        -------
        dict of str to PaperLinks
            Each accession, in input order, mapped to its linked papers; an empty
            ``papers`` means "no paper linked".

        Raises
        ------
        AccessionError
            If an accession is malformed.
        """
        order = [a.strip().upper() for a in accessions]
        for accession in order:
            kind_of(accession)
        targets: dict[str, dict[str, set[str]]] = {a: {} for a in order}
        for src, dst, _relation, source in self.build.links(set(order)) if order else []:
            if src in targets and dst.startswith((_PMID, _DOI)) and _is_paper(dst):
                targets[src].setdefault(dst, set()).add(source)
        linked = {t for ts in targets.values() for t in ts}
        pmids = {paper_key(t) for t in linked if t.startswith(_PMID)}
        dois = {paper_key(t) for t in linked if t.startswith(_DOI)}
        rows = self._paper_rows(pmids | dois, dois) if pmids | dois else []
        by_key = {r["paper"]: r for r in rows}
        by_doi = _by_doi(rows)
        out = {}
        for accession in order:
            found: dict[str, LinkedPaper] = {}
            for target, sources in targets[accession].items():
                key = paper_key(target)
                row = by_key.get(key) or by_doi.get(key)
                if row is not None:
                    found[row["paper"]] = LinkedPaper(row["paper"], row["status"], row["reason"])
                elif target.startswith(_PMID) and not is_placeholder(key, sources):
                    found[key] = LinkedPaper(key, None, None)
            out[accession] = PaperLinks(accession, tuple(found[k] for k in sorted(found)))
        return out

    def _paper_rows(self, keys: Iterable[str], dois: set[str]) -> list[dict[str, Any]]:
        """Return the ``papers`` rows keyed by ``keys``, or whose DOIs include one of ``dois``."""
        rows = self._rows(tables.PAPERS, keys, dois=dois)
        rest = dois - set(_by_doi(rows))
        path = tables.PAPERS.path(self.root)
        if rest and path.is_file():
            # Few papers have other DOIs, so this reads few rows.
            aliased = pq.read_table(
                path, schema=tables.PAPERS.schema, filters=pc.field("other_dois").is_valid()
            ).to_pylist()
            rows += [r for r in aliased if rest & set(r["other_dois"])]
        return rows

    def _rows(
        self, table: tables.PaperTable, keys: Iterable[str], dois: Iterable[str] = ()
    ) -> list[dict[str, Any]]:
        """Return the rows of ``table`` whose ``paper`` is in ``keys`` or ``doi`` in ``dois``."""
        path = table.path(self.root)
        if not path.is_file():
            return []
        filters = [[("paper", "in", sorted(keys))]]
        if dois:
            filters.append([("doi", "in", sorted(dois))])
        return pq.read_table(path, schema=table.schema, filters=filters).to_pylist()


def _by_doi(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map each DOI of the ``papers`` rows, ``doi`` and ``other_dois``, to its row."""
    return {d: r for r in rows for d in (r["doi"], *(r["other_dois"] or ())) if d}


def _is_paper(target: str) -> bool:
    """Return whether a link's far end names a PMID or DOI a paper key can be made from."""
    try:
        paper_key(target)
    except ValueError:
        return False
    return True


def _info(row: dict[str, Any]) -> PaperInfo:
    return PaperInfo(
        title=row["title"],
        journal=row["journal"],
        published=row["published"],
        authors=tuple(
            Author(a["name"], tuple(a["affiliations"] or ())) for a in row["authors"] or ()
        ),
        abstract=row["abstract"],
        publication_type=tuple(row["publication_type"] or ()),
    )


def _file(root: Path, row: dict[str, Any]) -> PaperFile:
    return PaperFile(
        kind=row["kind"],
        name=row["name"],
        path=root / row["path"] if row["path"] else None,
        size=row["size"],
        md5=row["md5"],
        url=row["url"],
        tier=row["tier"],
        label=row["label"],
        caption=row["caption"],
        fetched=row["fetched"],
        gone=row["gone"],
    )
