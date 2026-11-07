"""PubMed details and PMC IDs of papers, through :class:`~labdata.ncbi.entrez.EntrezClient`.

Details come from PubMed efetch, 200 PMIDs a request; PMCIDs and DOIs from the PMC ID
converter, 200 PMIDs a call. No raw PubMed XML is kept.
"""

from __future__ import annotations

import datetime as dt
import json
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from labdata._http import RateLimiter
from labdata.ncbi.entrez import EntrezClient

#: The PMC ID converter.
IDCONV = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"

#: PMIDs per efetch request and per ID converter call.
BATCH = 200

#: Paces ID converter calls, which E-utilities' limiter does not cover.
IDCONV_LIMITER = RateLimiter(3.0, 1)

_MONTH_NAMES = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
_MONTHS = {m: n for n, m in enumerate(_MONTH_NAMES, 1)}


@dataclass(frozen=True, slots=True)
class Details:
    """One PubMed record's details. ``authors`` are ``{name, affiliations}`` dicts."""

    pmid: str
    title: str | None
    journal: str | None
    published: dt.date | None
    authors: tuple[dict[str, Any], ...]
    abstract: str | None
    publication_type: tuple[str, ...]
    doi: str | None
    pmcid: str | None


def pmc_ids(client: EntrezClient, pmids: Iterable[str]) -> dict[str, tuple[str | None, str | None]]:
    """Map each PMID to its PMCID and DOI by the PMC ID converter.

    Parameters
    ----------
    client : EntrezClient
        The NCBI client; its email is sent with each call.
    pmids : iterable of str
        The PMIDs.

    Returns
    -------
    dict of str to tuple
        Every PMID answered, mapped to ``(pmcid, doi)``; both ``None`` when PMC lacks it.

    Raises
    ------
    EntrezError
        If a call fails after its retries.
    """
    wanted = list(dict.fromkeys(pmids))
    out: dict[str, tuple[str | None, str | None]] = {}
    for i in range(0, len(wanted), BATCH):
        query = {
            "ids": ",".join(wanted[i : i + BATCH]),
            "idtype": "pmid",
            "format": "json",
            "tool": "liulab-data",
            "email": client.credentials.email,
        }
        with IDCONV_LIMITER.turn():
            text = client.fetch_text(f"{IDCONV}?{urllib.parse.urlencode(query)}")
        for record in json.loads(text).get("records", []):
            pmid = str(record.get("pmid") or record.get("requested-id") or "")
            if pmid:
                doi = record.get("doi")
                out[pmid] = (record.get("pmcid"), doi.lower() if doi else None)
    return out


def details(client: EntrezClient, pmids: Iterable[str]) -> dict[str, Details]:
    """Return the PubMed details of each PMID PubMed has, by efetch, 200 a request.

    Raises
    ------
    EntrezError
        If a request fails after its retries.
    """
    wanted = list(dict.fromkeys(pmids))
    out: dict[str, Details] = {}
    for i in range(0, len(wanted), BATCH):
        got = client.efetch_records(
            "pubmed", wanted[i : i + BATCH], rettype="abstract", retmode="xml", split=_split
        )
        out |= {pmid: _details(pmid, text) for pmid, text in got.records.items()}
    return out


def _split(reply: str) -> dict[str, str]:
    root = ET.fromstring(reply)
    out = {}
    for article in root.iter("PubmedArticle"):
        pmid = article.findtext("MedlineCitation/PMID")
        if pmid:
            out[pmid.strip()] = ET.tostring(article, encoding="unicode")
    return out


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    text = "".join(element.itertext()).strip()
    return text or None


def _details(pmid: str, text: str) -> Details:
    article = ET.fromstring(text)
    body = article.find("MedlineCitation/Article")
    if body is None:
        body = ET.Element("Article")
    authors = []
    for author in body.iterfind("AuthorList/Author"):
        name = _text(author.find("CollectiveName")) or " ".join(
            filter(None, (_text(author.find("LastName")), _text(author.find("ForeName"))))
        )
        if name:
            affiliations = [_text(a) for a in author.iterfind("AffiliationInfo/Affiliation")]
            authors.append({"name": name, "affiliations": [a for a in affiliations if a]})
    parts = []
    for part in body.iterfind("Abstract/AbstractText"):
        words = _text(part)
        if words:
            label = part.get("Label")
            parts.append(f"{label}: {words}" if label else words)
    ids = {
        a.get("IdType"): (a.text or "").strip()
        for a in article.iterfind("PubmedData/ArticleIdList/ArticleId")
    }
    # The online date is exact; an issue date may be a year or a month alone, or a volume's.
    online = [d for d in map(_pub_date, body.iterfind("ArticleDate")) if d is not None]
    issue = _pub_date(body.find("Journal/JournalIssue/PubDate"))
    return Details(
        pmid=pmid,
        title=_text(body.find("ArticleTitle")),
        journal=_text(body.find("Journal/Title")),
        published=min(online) if online else issue,
        authors=tuple(authors),
        abstract="\n".join(parts) or None,
        publication_type=tuple(
            t for t in (_text(p) for p in body.iterfind("PublicationTypeList/PublicationType")) if t
        ),
        doi=ids["doi"].lower() if ids.get("doi") else None,
        pmcid=ids.get("pmc") or None,
    )


def _pub_date(element: ET.Element | None) -> dt.date | None:
    """Read a PubMed date; a missing month or day counts as the first."""
    if element is None:
        return None
    year = element.findtext("Year")
    month = element.findtext("Month")
    day = element.findtext("Day")
    if year is None:
        # MedlineDate, such as "2019 Jan-Feb".
        words = (element.findtext("MedlineDate") or "").split()
        if not words or not words[0][:4].isdigit():
            return None
        year = words[0][:4]
        month = words[1][:3] if len(words) > 1 else None
    try:
        m = (
            int(month)
            if month and month.isdigit()
            else _MONTHS.get((month or "jan")[:3].lower(), 1)
        )
        return dt.date(int(year), m, int(day) if day and day.isdigit() else 1)
    except ValueError:
        return None
