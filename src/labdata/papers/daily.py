"""The daily pass: OpenAlex's paid copy, free host links, status, and the manual queue.

Each OpenAlex account spends up to :data:`QUOTA` downloads a day, counted from that day's
rows in ``attempts``, so a rerun the same day spends only what is left. Every download is
journaled the moment it lands, and the next load of the tables saves a killed pass's journal,
so no file is fetched twice. A key is sent only in a request header, never in a URL, a log
line or the store.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import http.client
import time
from collections import Counter
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from labdata._files import write_atomic
from labdata._http import (
    MOVED,
    USER_AGENT,
    HttpTransport,
    RateLimiter,
    TooManyRedirects,
    Transport,
    get_following,
)
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import ladder, paths
from labdata.papers.openalex import Account, OpenAlex
from labdata.papers.root import PaperStore, paper_dir
from labdata.papers.sync import store_links
from labdata.papers.tables import PaperTables

#: Downloads each OpenAlex account may spend a day; the budget resets at 00:00 UTC.
QUOTA = 100

#: OpenAlex's own copies of works.
CONTENT = "https://content.openalex.org/works"

#: The manual queue: PubMed MEDLINE records for Paperpile, and one DOI a line.
NBIB = paths.MANUAL_NBIB.rel
DOIS = paths.MANUAL_DOIS.rel

#: The pace of free host links: requests a second, and how many at once.
HOST_PACE = (1.0, 1)

#: Attempt outcomes that spent a download from an account's quota.
SPENT = ("fetched", "error", "failed")

#: PMIDs per efetch request for the manual queue.
_BATCH = 200
_TIMEOUT = 120.0
_CHUNK = 1 << 16
_REDIRECTS = 5
#: The longest ``Retry-After`` a 429 is waited out for, and how many times in a row.
_WAIT_MAX = 300.0
_WAITS = 3
_BLOCKED = frozenset({401, 403, 429, 503})

# Bound at import, so tests may stub it.
_sleep = time.sleep


@dataclass
class DailyReport:
    """What one daily pass did: downloads per tier and per account, and papers by status."""

    fetched: dict[str, int] = field(default_factory=dict)
    quota: dict[str, int] = field(default_factory=dict)
    queued: int = 0
    counts: dict[str, dict[str, int]] = field(default_factory=dict)


def daily_pass(
    store: PaperStore,
    *,
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    transport: Transport | None = None,
    host_limiter: RateLimiter | None = None,
    log: Callable[[str], None] = print,
) -> DailyReport:
    """Run the daily pass over the paper store.

    Spends each OpenAlex account's quota on papers OpenAlex holds a copy of, new papers
    first and then the backlog newest first; tries each free host link once where OpenAlex
    has no copy or failed; settles every paper on the tier ladder; and rewrites the manual
    queue. Only papers the current GEO/SRA build links get downloads; the others keep their
    rows and files. Takes no lock: :func:`labdata.papers.lifecycle.daily` runs it.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    client : EntrezClient
        The NCBI client, for the manual queue's MEDLINE records.
    openalex : OpenAlex
        Its accounts spend the quota.
    today : datetime.date
        The UTC date of the pass.
    transport : Transport or None
        Sends every download; kept-open connections by default.
    host_limiter : RateLimiter or None
        Paces free host links; one a second, one at a time, by default.
    log : callable
        Takes one progress line.

    Returns
    -------
    DailyReport
        Today's downloads and the papers by status and reason.
    """
    transport = transport or HttpTransport()
    host_limiter = host_limiter or RateLimiter(*HOST_PACE)
    state = PaperTables.load(store.root, today, journal=True)
    # The Paperpile step goes here: it files the maintainer's saves before quota is spent.
    linked = set(store_links(store, state))
    fetch = _Fetcher(state, transport, host_limiter, linked, today, log)
    if openalex.accounts:
        fetch.openalex(openalex.accounts)
    else:
        log("no OpenAlex accounts: the OpenAlex tier is skipped; see `labdata papers configure`")
    fetch.hosts()
    state.save(today)
    queued = _manual_queue(store.root, state, client, log)
    report = DailyReport(queued=queued)
    todays = [a for a in state.attempts if a.get("date") == today]
    report.fetched = dict(Counter(a["tier"] for a in todays if a["outcome"] == "fetched"))
    report.quota = {a.email: 0 for a in openalex.accounts} | dict(
        Counter(
            a["account"] for a in todays if a["tier"] == ladder.OPENALEX and a["outcome"] in SPENT
        )
    )
    counts: dict[str, Counter[str]] = {}
    for row in state.papers.values():
        counts.setdefault(row.get("status") or "", Counter())[row.get("reason") or ""] += 1
    report.counts = {s: dict(c) for s, c in counts.items()}
    return report


def _order(state: PaperTables, keys: Iterable[str]) -> list[str]:
    """Sort papers new first, then the backlog newest first."""
    least = dt.date.min

    def rank(key: str) -> tuple[dt.date, dt.date, str]:
        row = state.papers[key]
        return (row.get("added") or least, row.get("published") or least, key)

    return sorted(keys, key=rank, reverse=True)


@dataclass
class _Got:
    """One download's result: ``ok``, or what stopped it."""

    ok: bool = False
    status: int | None = None
    detail: str = ""
    size: int = 0
    md5: str = ""
    retry_after: float | None = None


class _Fetcher:
    """Runs the two tiers of one pass, putting each download in the tables as it lands."""

    def __init__(
        self,
        state: PaperTables,
        transport: Transport,
        host_limiter: RateLimiter,
        linked: set[str],
        today: dt.date,
        log: Callable[[str], None],
    ) -> None:
        self.state = state
        self.transport, self.host_limiter, self.linked = transport, host_limiter, linked
        self.today, self.log = today, log
        self._clock = time.monotonic()

    def openalex(self, accounts: list[Account]) -> None:
        """Spend each account's quota on the papers waiting for OpenAlex's copy."""
        spent: Counter[str] = Counter()
        stopped: set[str] = set()
        tried: set[str] = set()
        for a in self.state.attempts:
            if a["tier"] == ladder.OPENALEX and a.get("date") == self.today:
                if a["outcome"] in SPENT:
                    spent[a["account"]] += 1
                    tried.add(a["paper"])
                elif a["outcome"] == "rate-limited":
                    stopped.add(a["account"])
        queue = self._due(ladder.OPENALEX, tried)
        active = [a for a in accounts if a.email not in stopped and spent[a.email] < QUOTA]
        self.log(f"OpenAlex: {len(queue)} papers waiting, {len(active)} accounts with quota left")
        turn = done = 0
        waits = 0
        while done < len(queue) and active:
            key = queue[done]
            account = active[turn % len(active)]
            url = f"{CONTENT}/{self.state.papers[key]['openalex']}.pdf"
            got = self._get(key, url, {"Authorization": f"Bearer {account.key}"})
            if got.status == 429:
                if got.retry_after is not None and got.retry_after <= _WAIT_MAX and waits < _WAITS:
                    waits += 1
                    _sleep(got.retry_after)
                    continue
                self._attempt(key, ladder.OPENALEX, account.email, "rate-limited", "HTTP 429")
                self.log(f"OpenAlex account {account.email} is out of quota for today")
                active.remove(account)
                waits = 0
                continue
            waits = 0
            if got.ok:
                outcome = "fetched"
            elif got.status is None or got.status >= 500:
                outcome = "error"
            else:
                outcome = "failed"
            self._attempt(key, ladder.OPENALEX, account.email, outcome, got.detail)
            if got.ok:
                self._file(key, ladder.OPENALEX, url, got)
            spent[account.email] += 1
            if spent[account.email] >= QUOTA:
                active.remove(account)
            else:
                turn += 1
            done += 1
            self._progress(f"OpenAlex: {done}/{len(queue)} papers tried")

    def hosts(self) -> None:
        """Try each free host link once, for papers OpenAlex cannot serve."""
        queue = self._due(ladder.HOST)
        self.log(f"host links: {len(queue)} papers to try")
        for n, key in enumerate(queue, 1):
            got, url = _Got(), ""
            for url in self.state.papers[key]["host_pdf_urls"]:
                with self.host_limiter.turn():
                    got = self._get(key, url, {})
                if got.ok:
                    break
            if got.ok:
                self._attempt(key, ladder.HOST, None, "fetched", url)
                self._file(key, ladder.HOST, url, got)
            else:
                blocked = got.status in _BLOCKED or got.detail == "not a PDF"
                outcome = "blocked" if blocked else "failed"
                self._attempt(key, ladder.HOST, None, outcome, f"{got.detail}: {url}")
            self._progress(f"host links: {n}/{len(queue)} papers tried")

    def _due(self, tier: str, tried: Collection[str] = ()) -> list[str]:
        """Return the linked papers the ladder sends to ``tier``, but ``tried``, in turn."""
        due = ladder.standings(self.state, self.today)
        keys = (k for k, s in due.items() if s.tier == tier and k in self.linked and k not in tried)
        return _order(self.state, keys)

    def _get(self, key: str, url: str, headers: dict[str, str]) -> _Got:
        name = paper_dir(key).name + ".pdf"
        return _download(self.transport, url, self.state, paper_dir(key) / name, headers)

    def _attempt(self, key: str, tier: str, account: str | None, outcome: str, detail: str) -> None:
        self.state.attempt(
            {
                "paper": key,
                "tier": tier,
                "date": self.today,
                "account": account,
                "outcome": outcome,
                "detail": detail or None,
            }
        )

    def _file(self, key: str, tier: str, url: str, got: _Got) -> None:
        name = paper_dir(key).name + ".pdf"
        self.state.put_file(
            {
                "paper": key,
                "kind": "pdf",
                "name": name,
                "path": (paper_dir(key) / name).as_posix(),
                "size": got.size,
                "md5": got.md5,
                "url": url,
                "tier": tier,
                "fetched": self.today,
            }
        )

    def _progress(self, line: str) -> None:
        if time.monotonic() - self._clock >= 60:
            self._clock = time.monotonic()
            self.log(line)


def _download(
    transport: Transport, url: str, state: PaperTables, dest: Path, headers: dict[str, str]
) -> _Got:
    """GET a PDF to ``dest`` in the paper store, following redirects; anything but a PDF fails.

    An ``Authorization`` header is not passed on to another host.
    """
    headers = {"User-Agent": USER_AGENT, "Accept": "application/pdf", **headers}
    try:
        with get_following(transport, url, headers=headers, timeout=_TIMEOUT, hops=_REDIRECTS) as (
            _,
            r,
        ):
            if r.status in MOVED:
                r.read()
                return _Got(status=r.status, detail=f"HTTP {r.status} with no Location")
            if r.status != 200:
                r.read()
                return _Got(
                    status=r.status,
                    detail=f"HTTP {r.status}",
                    retry_after=_seconds(r.getheader("Retry-After")),
                )
            return _save_pdf(r, state, dest)
    except TooManyRedirects:
        return _Got(status=310, detail="too many redirects")
    except (OSError, ValueError, http.client.HTTPException) as err:
        return _Got(detail=type(err).__name__)


def _save_pdf(response: Any, state: PaperTables, dest: Path) -> _Got:
    first = response.read(_CHUNK)
    if b"%PDF" not in first[:1024]:
        return _Got(status=200, detail="not a PDF")
    md5 = hashlib.md5(usedforsecurity=False)
    size = 0
    with state.writing(dest) as fh:
        chunk = first
        while chunk:
            fh.write(chunk)
            md5.update(chunk)
            size += len(chunk)
            chunk = response.read(_CHUNK)
    return _Got(ok=True, status=200, size=size, md5=md5.hexdigest())


def _seconds(text: str | None) -> float | None:
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _manual_queue(
    root: Path, state: PaperTables, client: EntrezClient, log: Callable[[str], None]
) -> int:
    """Rewrite the manual queue files, newest first; return the papers in it."""
    least = dt.date.min
    manual = sorted(
        (r for r in state.papers.values() if r.get("status") == "manual"),
        key=lambda r: (r.get("published") or least, r["paper"]),
        reverse=True,
    )
    pmids = [r["paper"] for r in manual if r["paper"].isdigit()]
    records: dict[str, str] = {}
    for i in range(0, len(pmids), _BATCH):
        text = client.efetch("pubmed", pmids[i : i + _BATCH], rettype="medline", retmode="text")
        records |= _medline_records(text)
    missing = len(pmids) - len(records.keys() & set(pmids))
    if missing:
        log(f"manual queue: PubMed returned no record for {missing} PMIDs")
    write_atomic(root / NBIB, "".join(records[p] + "\n\n" for p in pmids if p in records))
    write_atomic(root / DOIS, "".join(r["doi"] + "\n" for r in manual if r.get("doi")))
    return len(manual)


def _medline_records(text: str) -> dict[str, str]:
    """Split MEDLINE text into records by PMID; records are separated by blank lines."""
    out = {}
    for block in text.replace("\r\n", "\n").split("\n\n"):
        block = block.strip("\n")
        if block.startswith("PMID-"):
            out[block.split("\n", 1)[0].removeprefix("PMID-").strip()] = block
    return out
