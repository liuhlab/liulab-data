"""A fake web for the paper job: PubMed efetch, the PMC ID converter and OpenAlex.

It stands in at the HTTP transport seam, so the real ``EntrezClient`` and OpenAlex client run
over it, and it records every request it answers.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import urllib.parse
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from labdata.store import gaps, layout, targets


class Reply:
    """One canned HTTP reply."""

    def __init__(
        self, body: bytes | str, status: int = 200, headers: Mapping[str, str] | None = None
    ) -> None:
        self._body = io.BytesIO(body.encode() if isinstance(body, str) else body)
        self.status = status
        self.will_close = False
        self._headers = {k.lower(): v for k, v in (headers or {}).items()}

    def getheader(self, name: str) -> str | None:
        return self._headers.get(name.lower())

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
    #: The electronic publication date, year, month, day, when it has one.
    epub: tuple[int, int, int] | None = None

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
        epub = (
            '<ArticleDate DateType="Electronic"><Year>{}</Year><Month>{:02d}</Month>'
            "<Day>{:02d}</Day></ArticleDate>".format(*self.epub)
            if self.epub
            else ""
        )
        return (
            f'<PubmedArticle><MedlineCitation><PMID Version="1">{self.pmid}</PMID>'
            f"<Article><Journal><Title>J Test</Title><JournalIssue><PubDate>"
            f"<Year>{self.year}</Year><Month>{self.month:02d}</Month><Day>{self.day:02d}</Day>"
            f"</PubDate></JournalIssue></Journal><ArticleTitle>{self.title}</ArticleTitle>"
            f"<Abstract>{abstract}</Abstract><AuthorList>{authors}</AuthorList>"
            f"<PublicationTypeList>{types}</PublicationTypeList>{epub}</Article></MedlineCitation>"
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
    retracted: bool = False,
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
        "is_retracted": retracted,
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
    bucket: FakeBucket = field(default_factory=lambda: FakeBucket())

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
        elif parts.netloc == BUCKET_HOST:
            yield self.bucket.reply(parts.path, query)
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


BUCKET_HOST = "pmc-oa-opendata.s3.amazonaws.com"
_INVENTORY = "inventory-reports/pmc-oa-opendata/metadata/"


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


@dataclass
class FakeBucket:
    """The PMC bucket: objects by key, listings, and the daily inventory of JSON records.

    ``bad`` keys are served with bytes that do not match their MD5; ``fail`` is called with
    each key fetched and may raise, to stop a pass mid-way. ``gets`` records every object
    fetched, inventory files included. ``types`` gives an object's Content-Type.
    """

    objects: dict[str, bytes] = field(default_factory=dict)
    day: str = "2026-11-06"
    page: int = 1000
    bad: set[str] = field(default_factory=set)
    gets: list[str] = field(default_factory=list)
    fail: Any = None
    types: dict[str, str] = field(default_factory=dict)

    def put(
        self,
        pmcid: str,
        version: int,
        files: Mapping[str, bytes],
        *,
        manuscript: bool = False,
        retracted: bool = False,
    ) -> str:
        """Add one article version: its files, and a JSON record naming each one's MD5.

        Returns the version's folder, ``PMC<id>.<v>``.
        """
        folder = f"{pmcid}.{version}"
        record: dict[str, Any] = {
            "pmcid": pmcid,
            "version": version,
            "is_manuscript": manuscript,
            "is_retracted": retracted,
            "pdf_url": None,
            "media_urls": [],
        }
        for name, data in files.items():
            url = f"s3://pmc-oa-opendata/{folder}/{name}?md5={md5(data)}"
            if name == f"{folder}.xml":
                record["xml_url"] = url
            elif name == f"{folder}.pdf":
                record["pdf_url"] = url
            elif name == f"{folder}.txt":
                record["text_url"] = url
            else:
                record["media_urls"].append(url)
            self.objects[f"{folder}/{name}"] = data
        self.objects[f"{folder}/{folder}.json"] = json.dumps(record).encode()
        return folder

    def drop(self, folder: str) -> None:
        """Remove one article version from the bucket."""
        for key in [k for k in self.objects if k.startswith(f"{folder}/")]:
            del self.objects[key]

    def fetched(self, key: str) -> int:
        """Return how many times one object was fetched."""
        return self.gets.count(key)

    def _inventory(self) -> dict[str, bytes]:
        lines = "".join(
            f'"pmc-oa-opendata","metadata/{k.split("/")[1]}","2026-07-01T00:00:00.000Z",'
            f'"{md5(v)}"\n'
            for k, v in sorted(self.objects.items())
            if k.endswith(".json") and k.count("/") == 1
        )
        part = gzip.compress(lines.encode(), mtime=0)
        part_key = f"{_INVENTORY}data/part-0.csv.gz"
        manifest = {
            "fileSchema": "Bucket, Key, LastModifiedDate, ETag",
            "files": [{"key": part_key, "size": len(part), "MD5checksum": md5(part)}],
        }
        return {
            f"{_INVENTORY}{self.day}T01-00Z/manifest.json": json.dumps(manifest).encode(),
            part_key: part,
        }

    def reply(self, path: str, query: dict[str, list[str]]) -> Reply:
        everything = {**self.objects, **self._inventory()}
        if path in ("", "/") and query.get("list-type") == ["2"]:
            return Reply(self._listing(everything, query))
        key = urllib.parse.unquote(path.lstrip("/"))
        if key not in everything:
            return Reply("<Error><Code>NoSuchKey</Code></Error>", status=404)
        if self.fail is not None:
            self.fail(key)
        self.gets.append(key)
        data = everything[key]
        headers = {"Content-Type": self.types[key]} if key in self.types else None
        return Reply(data[::-1] + b"x" if key in self.bad else data, headers=headers)

    def _listing(self, everything: dict[str, bytes], query: dict[str, list[str]]) -> str:
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        keys = sorted(k for k in everything if k.startswith(prefix))
        prefixes: list[str] = []
        if delimiter:
            rest = [k for k in keys if delimiter in k[len(prefix) :]]
            prefixes = sorted({k[: k.index(delimiter, len(prefix)) + 1] for k in rest})
            keys = [k for k in keys if k not in rest]
        start = int(query.get("continuation-token", ["0"])[0])
        shown = keys[start : start + self.page]
        more = start + self.page < len(keys)
        contents = "".join(
            f"<Contents><Key>{escape(k)}</Key><LastModified>2026-07-01T00:00:00.000Z"
            f"</LastModified><ETag>&quot;{md5(everything[k])}&quot;</ETag>"
            f"<Size>{len(everything[k])}</Size></Contents>"
            for k in shown
        )
        common = "".join(
            f"<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>" for p in prefixes
        )
        token = (
            f"<NextContinuationToken>{start + self.page}</NextContinuationToken>" if more else ""
        )
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Prefix>{escape(prefix)}</Prefix><KeyCount>{len(shown)}</KeyCount>"
            f"<IsTruncated>{'true' if more else 'false'}</IsTruncated>{token}"
            f"{contents}{common}</ListBucketResult>"
        )


def _value(w: dict[str, Any], name: str) -> str | None:
    if name == "ids.pmid":
        pmid = w["ids"].get("pmid")
        return pmid.rsplit("/", 1)[1] if pmid else None
    if name == "doi":
        return w["doi"].removeprefix("https://doi.org/") if w["doi"] else None
    raise AssertionError(f"unexpected OpenAlex filter: {name}")


def fixture_build(
    root: Path, links: list[tuple[str, str, str]], *, not_targets: Iterable[str] = ()
) -> Path:
    """Make a GEO/SRA store at ``root`` whose build ``b1`` holds links, holdings and gaps.

    Each link is ``(from, to, source)``; every ``from`` is held. Every held Series is on the
    target list of the build's snapshot date, except ``not_targets``.
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
    layout.write(
        con,
        layout.GAPS,
        layout.GAPS.path(build),
        "SELECT * FROM (VALUES ('GSE0', 'x', 'x')) v(accession, reason, detail) WHERE false",
        {gaps.DATE_KEY: SNAPSHOT_DATE},
    )
    skip = set(not_targets)
    series = sorted({f for f, _, _ in links if f.startswith("GSE") and f not in skip})
    out = targets.list_path(root, SNAPSHOT_DATE)
    out.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {"accession": series, "gdstype": ["x"] * len(series), "bioproject": [""] * len(series)},
            schema=targets.SCHEMA,
        ),
        out,
    )
    return root


#: The ``geo-series`` snapshot date a fixture build's gaps name.
SNAPSHOT_DATE = "2026-11-01"
