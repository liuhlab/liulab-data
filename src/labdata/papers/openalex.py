"""OpenAlex: the accounts the paper job spends, and work lookups by PMID or DOI.

Lookups go through the shared HTTP transport. A batch filter takes 100 IDs a call; an ID
the batch leaves out gets one single lookup, as the batch filter can miss a work a single
lookup finds. A key never appears in a log or an error.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import time
import tomllib
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from labdata._cache import ensure_cache_dir, liulab_data_cache_dir
from labdata._http import HttpTransport, RateLimiter, Transport
from labdata.exceptions import LabdataError
from labdata.ncbi.entrez import Traffic
from labdata.store._files import temp_beside, toml_line

#: Environment variable of ``key:email`` pairs, comma-separated; overrides the file.
KEYS_ENV = "OPENALEX_API_KEYS"

#: The accounts file in the cache directory: ``[[accounts]]`` with ``key`` and ``email``.
ACCOUNTS_FILENAME = "openalex.toml"

API = "https://api.openalex.org"

#: IDs per batch filter call.
BATCH = 100

#: Work types the paper job takes for a paper; any other is ``not-article``.
#: A ``data-paper`` is an article that describes a dataset, so it counts.
ARTICLE_TYPES = (
    "article",
    "preprint",
    "review",
    "letter",
    "data-paper",
    "conference-paper",
    "book-chapter",
    "report",
)

_SELECT = (
    "id,doi,ids,type,title,publication_date,has_content,locations,primary_location,"
    "authorships,abstract_inverted_index,is_retracted"
)
_TIMEOUT = 30.0
_TRIES = 5
_RETRY = frozenset({429, 500, 502, 503, 504})
_CONTENT_HOST = "content.openalex.org"

# Bound at import, so tests may stub it.
_sleep = time.sleep


class OpenAlexError(LabdataError):
    """An OpenAlex request failed after its retries."""


@dataclass(frozen=True, slots=True)
class Account:
    """One OpenAlex account: its key and the email sent with it."""

    key: str = field(repr=False)
    email: str


def load_accounts() -> list[Account]:
    """Return the OpenAlex accounts: from ``OPENALEX_API_KEYS``, else ``<cache>/openalex.toml``.

    Returns
    -------
    list of Account
        The accounts in the order given; empty when neither is set.

    Raises
    ------
    LabdataError
        If an entry has no key or no email.
    """
    env = os.environ.get(KEYS_ENV, "").strip()
    if env:
        pairs = [item.strip().partition(":") for item in env.split(",") if item.strip()]
        entries = [{"key": k, "email": e} for k, _, e in pairs]
    else:
        path = accounts_path()
        if not path.is_file():
            return []
        with path.open("rb") as fh:
            entries = tomllib.load(fh).get("accounts", [])
    accounts = []
    for n, entry in enumerate(entries, 1):
        key, email = str(entry.get("key", "")).strip(), str(entry.get("email", "")).strip()
        if not key or not email:
            raise LabdataError(f"OpenAlex account {n} needs both a key and an email")
        accounts.append(Account(key, email))
    return accounts


@dataclass(frozen=True, slots=True)
class Work:
    """The parts of an OpenAlex work the paper job keeps.

    ``pdf_urls`` are the hosts' own PDF links, in OpenAlex's order; ``has_pdf`` says OpenAlex
    sells a copy of its own. ``authors`` are ``{name, affiliations}`` dicts.
    """

    openalex: str
    doi: str | None
    pmid: str | None
    type: str | None
    has_pdf: bool
    pdf_urls: tuple[str, ...]
    published: dt.date | None
    title: str | None
    journal: str | None
    authors: tuple[dict[str, Any], ...]
    abstract: str | None
    retracted: bool = False

    @property
    def is_article(self) -> bool:
        """Whether the work is a paper: an article, preprint, review or the like."""
        return self.type in ARTICLE_TYPES


class OpenAlex:
    """Look works up in OpenAlex by PMID or DOI.

    Parameters
    ----------
    accounts : iterable of Account
        Lookups use the first; none means keyless calls on the shared budget.
    transport : Transport or None
        Sends the requests; kept-open connections by default.
    limiter : RateLimiter or None
        Paces the calls; 5 a second, one at a time, by default.
    """

    def __init__(
        self,
        accounts: Iterable[Account] = (),
        *,
        transport: Transport | None = None,
        limiter: RateLimiter | None = None,
    ) -> None:
        self.accounts = list(accounts)
        self.transport: Transport = transport or HttpTransport()
        self._limiter = limiter or RateLimiter(5.0, 1)
        #: Requests sent and bytes read, for a step's log.
        self.traffic = Traffic()

    def works_by_pmid(self, pmids: Iterable[str]) -> dict[str, Work]:
        """Return the work of each PMID OpenAlex knows, keyed by the PMID asked.

        Raises
        ------
        OpenAlexError
            If a request fails after its retries.
        """
        return self._lookup(list(dict.fromkeys(pmids)), "ids.pmid", "pmid", lambda w: w.pmid)

    def works_by_doi(self, dois: Iterable[str]) -> dict[str, Work]:
        """Return the work of each DOI OpenAlex knows, keyed by the DOI asked, in lower case.

        Raises
        ------
        OpenAlexError
            If a request fails after its retries.
        """
        wanted = list(dict.fromkeys(d.lower() for d in dois))
        return self._lookup(wanted, "doi", "doi", lambda w: w.doi)

    def _lookup(
        self, ids: list[str], batch_filter: str, single: str, id_of: Any
    ) -> dict[str, Work]:
        found: dict[str, Work] = {}
        for i in range(0, len(ids), BATCH):
            chunk = ids[i : i + BATCH]
            # A "|" or "," would split the filter's value, so such an ID goes alone.
            plain = [x for x in chunk if "|" not in x and "," not in x]
            if plain:
                reply = self._get(
                    "/works",
                    {
                        "filter": f"{batch_filter}:{'|'.join(plain)}",
                        "per_page": str(BATCH),
                        "select": _SELECT,
                    },
                )
                wanted = set(chunk)
                for raw in (reply or {}).get("results", []):
                    work = _work(raw)
                    if id_of(work) in wanted:
                        found[id_of(work)] = work
            for missed in (x for x in chunk if x not in found):
                path = f"/works/{single}:{urllib.parse.quote(missed, safe='/:')}"
                raw = self._get(path, {"select": _SELECT})
                if raw is not None:
                    found[missed] = _work(raw)
        return found

    def _get(self, path: str, params: dict[str, str]) -> dict[str, Any] | None:
        """GET one API path; ``None`` for a 404. Retries a 429, a 5xx or a dropped reply."""
        if self.accounts:
            params = {**params, "api_key": self.accounts[0].key, "mailto": self.accounts[0].email}
        url = f"{API}{path}?{urllib.parse.urlencode(params)}"
        host = urllib.parse.urlsplit(API).netloc
        last = ""
        for attempt in range(_TRIES):
            try:
                with (
                    self._limiter.turn(),
                    self.transport.open(
                        "GET",
                        url,
                        body=None,
                        headers={"User-Agent": "liulab-data", "Accept": "application/json"},
                        timeout=_TIMEOUT,
                    ) as response,
                ):
                    self.traffic.count(host, response.status)
                    body = response.read()
                    self.traffic.add_bytes(len(body))
                    status = response.status
            except (OSError, ValueError) as err:
                self.traffic.count(host, type(err).__name__)
                last = type(err).__name__
            else:
                if status == 200:
                    return json.loads(body)
                if status == 404:
                    return None
                last = f"HTTP {status}"
                if status not in _RETRY:
                    break
                if status == 429:
                    self._limiter.halve()
            _sleep(min(60.0, 2.0**attempt) * random.uniform(0.5, 1.0))
        # The path names the lookup; the query, which holds the key, is left out.
        raise OpenAlexError(f"OpenAlex request {path} failed: {last}")


def _work(raw: dict[str, Any]) -> Work:
    ids = raw.get("ids") or {}
    pmid = ids.get("pmid")
    doi = raw.get("doi")
    content = raw.get("has_content") or {}
    urls: list[str] = []
    for location in raw.get("locations") or []:
        url = (location or {}).get("pdf_url")
        if url and urllib.parse.urlsplit(url).netloc != _CONTENT_HOST and url not in urls:
            urls.append(url)
    published = raw.get("publication_date")
    source = ((raw.get("primary_location") or {}).get("source")) or {}
    authors = []
    for authorship in raw.get("authorships") or []:
        affiliations = authorship.get("raw_affiliation_strings") or [
            i["display_name"] for i in authorship.get("institutions") or [] if i.get("display_name")
        ]
        name = (authorship.get("author") or {}).get("display_name")
        if name:
            authors.append({"name": name, "affiliations": list(affiliations)})
    return Work(
        openalex=str(raw["id"]).rsplit("/", 1)[-1],
        doi=_strip_doi(doi) if doi else None,
        pmid=str(pmid).rsplit("/", 1)[-1] if pmid else None,
        type=raw.get("type"),
        has_pdf=bool(content.get("pdf")),
        pdf_urls=tuple(urls),
        published=_date(published),
        title=raw.get("title"),
        journal=source.get("display_name"),
        authors=tuple(authors),
        abstract=_abstract(raw.get("abstract_inverted_index")),
        retracted=bool(raw.get("is_retracted")),
    )


def _strip_doi(doi: str) -> str:
    return doi.lower().removeprefix("https://doi.org/").removeprefix("http://doi.org/")


def _date(text: str | None) -> dt.date | None:
    try:
        return dt.date.fromisoformat(text) if text else None
    except ValueError:
        return None


def _abstract(index: dict[str, list[int]] | None) -> str | None:
    """Rebuild an abstract from OpenAlex's word-to-positions index."""
    if not index:
        return None
    words = sorted((pos, word) for word, positions in index.items() for pos in positions)
    return " ".join(word for _, word in words)


def accounts_path() -> Path:
    """Return the accounts file, ``<cache>/openalex.toml``, which may not exist."""
    return liulab_data_cache_dir() / ACCOUNTS_FILENAME


def save_accounts(accounts: Iterable[Account]) -> Path:
    """Replace ``<cache>/openalex.toml`` with ``accounts``, readable by its owner only.

    Returns
    -------
    pathlib.Path
        The file written.
    """
    ensure_cache_dir()
    path = accounts_path()
    text = "".join(
        f"[[accounts]]\n{toml_line('key', a.key)}\n{toml_line('email', a.email)}\n\n"
        for a in accounts
    )
    tmp = temp_beside(path)
    try:
        # Made owner-only before the keys are written.
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return path
