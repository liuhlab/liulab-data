"""A fake web for the paper job: PubMed efetch, the PMC ID converter and OpenAlex.

It stands in at the HTTP transport seam, so the real ``EntrezClient`` and OpenAlex client run
over it, and it records every request it answers.
"""

from __future__ import annotations

import io
import json
import urllib.parse
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from labdata.store import layout


class Reply:
    """One canned HTTP reply."""

    def __init__(self, body: bytes | str, status: int = 200) -> None:
        self._body = io.BytesIO(body.encode() if isinstance(body, str) else body)
        self.status = status
        self.will_close = False

    def getheader(self, name: str) -> str | None:
        return None

    def read(self, amt: int | None = None) -> bytes:
        return self._body.read(amt)

    def isclosed(self) -> bool:
        return True


@dataclass
class Article:
    """A PubMed record, written as efetch's XML."""

    pmid: str
    title: str
    year: int
    month: int = 1
    day: int = 1
    doi: str | None = None
    pmcid: str | None = None
    authors: tuple[tuple[str, str, tuple[str, ...]], ...] = (("Liu", "Hanqing", ("Lab A",)),)
    abstract: tuple[tuple[str | None, str], ...] = ((None, "An abstract."),)
    types: tuple[str, ...] = ("Journal Article",)

    def xml(self) -> str:
        authors = "".join(
            f"<Author><LastName>{last}</LastName><ForeName>{fore}</ForeName>"
            + "".join(
                f"<AffiliationInfo><Affiliation>{a}</Affiliation></AffiliationInfo>" for a in affs
            )
            + "</Author>"
            for last, fore, affs in self.authors
        )
        abstract = "".join(
            f'<AbstractText Label="{label}">{text}</AbstractText>'
            if label
            else f"<AbstractText>{text}</AbstractText>"
            for label, text in self.abstract
        )
        types = "".join(f"<PublicationType>{t}</PublicationType>" for t in self.types)
        ids = f'<ArticleId IdType="pubmed">{self.pmid}</ArticleId>'
        if self.doi:
            ids += f'<ArticleId IdType="doi">{self.doi}</ArticleId>'
        if self.pmcid:
            ids += f'<ArticleId IdType="pmc">{self.pmcid}</ArticleId>'
        return (
            f'<PubmedArticle><MedlineCitation><PMID Version="1">{self.pmid}</PMID>'
            f"<Article><Journal><Title>J Test</Title><JournalIssue><PubDate>"
            f"<Year>{self.year}</Year><Month>{self.month:02d}</Month><Day>{self.day:02d}</Day>"
            f"</PubDate></JournalIssue></Journal><ArticleTitle>{self.title}</ArticleTitle>"
            f"<Abstract>{abstract}</Abstract><AuthorList>{authors}</AuthorList>"
            f"<PublicationTypeList>{types}</PublicationTypeList></Article></MedlineCitation>"
            f"<PubmedData><ArticleIdList>{ids}</ArticleIdList></PubmedData></PubmedArticle>"
        )


def work(
    wid: str,
    *,
    pmid: str | None = None,
    doi: str | None = None,
    kind: str = "article",
    pdf: bool = False,
    pdf_urls: tuple[str, ...] = (),
    published: str = "2024-01-01",
    title: str = "A work",
) -> dict[str, Any]:
    """Return an OpenAlex work object with the fields the paper job selects."""
    ids: dict[str, str] = {"openalex": f"https://openalex.org/{wid}"}
    if pmid:
        ids["pmid"] = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}"
    if doi:
        ids["doi"] = f"https://doi.org/{doi}"
    return {
        "id": f"https://openalex.org/{wid}",
        "doi": f"https://doi.org/{doi}" if doi else None,
        "ids": ids,
        "type": kind,
        "title": title,
        "publication_date": published,
        "has_content": {"pdf": pdf, "grobid_xml": pdf},
        "locations": [{"pdf_url": u, "is_oa": True} for u in pdf_urls]
        + [{"pdf_url": None, "is_oa": False}],
        "primary_location": {"source": {"display_name": "bioRxiv"}},
        "authorships": [
            {
                "author": {"display_name": "Ann Author"},
                "institutions": [{"display_name": "Uni X"}],
                "raw_affiliation_strings": ["Uni X, City"],
            }
        ],
        "abstract_inverted_index": {"Cells": [0], "divide.": [1]},
    }


@dataclass
class FakeWeb:
    """Answers PubMed efetch, ID converter and OpenAlex requests from canned records.

    ``batch_misses`` are works an OpenAlex batch filter leaves out though a single lookup
    finds them.
    """

    articles: Mapping[str, Article] = field(default_factory=dict)
    pmcids: Mapping[str, str] = field(default_factory=dict)
    works: list[dict[str, Any]] = field(default_factory=list)
    batch_misses: set[str] = field(default_factory=set)
    calls: list[str] = field(default_factory=list)

    @contextmanager
    def open(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> Iterator[Reply]:
        parts = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parts.query)
        form = urllib.parse.parse_qs(body.decode()) if body else {}
        if parts.netloc == "eutils.ncbi.nlm.nih.gov":
            self.calls.append("efetch")
            ids = form["id"][0].split(",")
            found = "".join(self.articles[i].xml() for i in ids if i in self.articles)
            yield Reply(f"<PubmedArticleSet>{found}</PubmedArticleSet>")
        elif parts.netloc == "pmc.ncbi.nlm.nih.gov":
            self.calls.append("idconv")
            records = []
            for i in query["ids"][0].split(","):
                if i in self.pmcids:
                    records.append({"pmid": int(i), "pmcid": self.pmcids[i], "requested-id": i})
                else:
                    records.append({"pmid": int(i), "status": "error", "requested-id": i})
            yield Reply(json.dumps({"status": "ok", "records": records}))
        elif parts.netloc == "api.openalex.org":
            yield self._openalex(parts.path, query)
        else:
            raise AssertionError(f"unexpected request: {url}")

    def _openalex(self, path: str, query: dict[str, list[str]]) -> Reply:
        if path == "/works":
            self.calls.append("openalex-batch")
            name, _, values = query["filter"][0].partition(":")
            wanted = set(values.split("|"))
            hits = [
                w
                for w in self.works
                if w["id"].rsplit("/", 1)[1] not in self.batch_misses and _value(w, name) in wanted
            ]
            return Reply(json.dumps({"meta": {"count": len(hits)}, "results": hits}))
        self.calls.append("openalex-single")
        name, _, value = urllib.parse.unquote(path.removeprefix("/works/")).partition(":")
        name = {"pmid": "ids.pmid"}.get(name, name)
        for w in self.works:
            if _value(w, name) == value:
                return Reply(json.dumps(w))
        return Reply(json.dumps({"error": "not found"}), status=404)

    def count(self, kind: str) -> int:
        """Return how many requests of one kind were answered."""
        return self.calls.count(kind)


def _value(w: dict[str, Any], name: str) -> str | None:
    if name == "ids.pmid":
        pmid = w["ids"].get("pmid")
        return pmid.rsplit("/", 1)[1] if pmid else None
    if name == "doi":
        return w["doi"].removeprefix("https://doi.org/") if w["doi"] else None
    raise AssertionError(f"unexpected OpenAlex filter: {name}")


def fixture_build(root: Path, links: list[tuple[str, str, str]]) -> Path:
    """Make a GEO/SRA store at ``root`` whose build ``b1`` holds only links and holdings.

    Each link is ``(from, to, source)``; every ``from`` is held.
    """
    build = root / "builds" / "b1"
    build.mkdir(parents=True)
    (root / "labdata-store.toml").write_text('format_version = 1\nbuild = "b1"\n', encoding="utf-8")
    con = duckdb.connect()
    values = ", ".join(f"('{f}', '{t}', 'PubMed', '{s}')" for f, t, s in links)
    rows = f'SELECT * FROM (VALUES {values}) v("from", "to", relation, source)'
    for table in (layout.LINKS_BY_FROM, layout.LINKS_BY_TO):
        layout.write(con, table, table.path(build), rows)
    held = ", ".join(
        f"('{a}', '{'GEO Series' if a.startswith('GSE') else 'BioProject'}')"
        for a in sorted({f for f, _, _ in links})
    )
    layout.write(
        con,
        layout.HOLDINGS,
        layout.HOLDINGS.path(build),
        f"SELECT * FROM (VALUES {held}) v(accession, type)",
    )
    return root
