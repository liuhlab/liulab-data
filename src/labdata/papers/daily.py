"""The daily pass: OpenAlex's paid copy, free host links, status, and the manual queue.

Each OpenAlex account spends up to :data:`QUOTA` downloads a day, counted from that day's
rows in ``attempts``, so a rerun the same day spends only what is left. Every download is
written to a journal beside the tables the moment it lands; a killed pass's journal is
replayed when the next pass starts, so no file is fetched twice. A key is sent only in a
request header, never in a URL, a log line or the store.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import time
import urllib.parse
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from labdata._http import USER_AGENT, HttpTransport, RateLimiter, Transport
from labdata.ncbi.entrez import EntrezClient
from labdata.papers.openalex import Account, OpenAlex
from labdata.papers.root import PaperStore, paper_dir, paper_lock
from labdata.papers.status import read_status, write_status
from labdata.papers.sync import status_of, store_links
from labdata.papers.tables import PaperTables
from labdata.store._files import temp_beside, write_atomic

#: Downloads each OpenAlex account may spend a day; the budget resets at 00:00 UTC.
QUOTA = 100

#: Days an OpenAlex server or network error is tried before the paper moves on.
OPENALEX_TRIES = 3

#: OpenAlex's own copies of works.
CONTENT = "https://content.openalex.org/works"

#: The manual queue: PubMed MEDLINE records for Paperpile, and one DOI a line.
NBIB = "manual-queue.nbib"
DOIS = "manual-queue-dois.txt"

#: Downloads the running pass has not yet written to the tables.
JOURNAL = ".daily-journal.jsonl"

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
    has no copy or failed; recomputes every paper's status; and rewrites the manual queue.
    Only papers the current GEO/SRA build links get downloads; the others keep their rows
    and files. Holds the paper store's lock; ``paper-status.json`` records the pass.

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
    root = store.root
    with paper_lock(root):
        start = _now()
        _record(root, {"kind": "daily", "start": start, "end": None, "outcome": "running"})
        try:
            report = _daily(
                store,
                client,
                openalex,
                today,
                transport or HttpTransport(),
                host_limiter or RateLimiter(*HOST_PACE),
                log,
            )
        except BaseException:
            _record(root, {"kind": "daily", "start": start, "end": _now(), "outcome": "failed"})
            raise
        status = read_status(root)
        day = today.isoformat()
        others = status.fetches.get("by_tier", {}) if status.fetches.get("date") == day else {}
        write_status(
            root,
            replace(
                status,
                last_pass={"kind": "daily", "start": start, "end": _now(), "outcome": "success"},
                last_daily_success=day,
                fetches={"date": day, "by_tier": {**others, **report.fetched}},
                quota={"date": day, "by_key": report.quota},
                counts=report.counts,
            ),
        )
    return report


def _daily(
    store: PaperStore,
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    transport: Transport,
    host_limiter: RateLimiter,
    log: Callable[[str], None],
) -> DailyReport:
    root = store.root
    state = PaperTables.load(root)
    journal = _Journal(root / JOURNAL)
    if journal.replay(state):
        log("took in the downloads a stopped pass left in its journal")
        state.save(root)
        journal.clear()
    # The Paperpile step goes here: it files the maintainer's saves before quota is spent.
    _recompute(state, today)
    linked = set(store_links(store, state))
    fetch = _Fetcher(root, state, journal, transport, host_limiter, linked, today, log)
    if openalex.accounts:
        fetch.openalex(openalex.accounts)
    else:
        log("no OpenAlex accounts: the OpenAlex tier is skipped; see `labdata papers configure`")
    fetch.hosts()
    _recompute(state, today)
    state.save(root)
    journal.clear()
    queued = _manual_queue(root, state, client, log)
    # The paper job copies the store to GCS after the pass (`labdata papers copy`).
    report = DailyReport(queued=queued)
    todays = [a for a in state.attempts if a.get("date") == today]
    report.fetched = dict(Counter(a["tier"] for a in todays if a["outcome"] == "fetched"))
    report.quota = {a.email: 0 for a in openalex.accounts} | dict(
        Counter(a["account"] for a in todays if a["tier"] == "openalex" and a["outcome"] in SPENT)
    )
    counts: dict[str, Counter[str]] = {}
    for row in state.papers.values():
        counts.setdefault(row.get("status") or "", Counter())[row.get("reason") or ""] += 1
    report.counts = {s: dict(c) for s, c in counts.items()}
    return report


def _tries(state: PaperTables) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for a in state.attempts:
        out.setdefault(a["paper"], []).append(a)
    return out


def _openalex_done(row: dict[str, Any], tries: Iterable[dict[str, Any]]) -> bool:
    """Whether OpenAlex has nothing more to give: no copy, a final failure, or 3 errors."""
    if not row.get("openalex_pdf"):
        return True
    outcomes = [a["outcome"] for a in tries if a["tier"] == "openalex"]
    return "failed" in outcomes or outcomes.count("error") >= OPENALEX_TRIES


def _host_tried(tries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [a for a in tries if a["tier"] == "host"]


def _pmc_passed(row: dict[str, Any], tries: Iterable[dict[str, Any]]) -> bool:
    """Whether PMC is out of the way: the paper has no PMCID, or the PMC bucket was tried."""
    return not row.get("pmcid") or any(a["tier"] == "pmc" for a in tries)


def _recompute(state: PaperTables, today: dt.date) -> None:
    """Set every paper's status; one every tier has failed is ``manual``, the last failure why."""
    held = state.held()
    tries = _tries(state)
    for key, row in state.papers.items():
        if row.get("status") == "skipped":
            continue
        status, reason = status_of(row, held=key in held, placeholder=False, today=today)
        if status == "waiting" and reason != "pmc-embargo":
            mine = tries.get(key, [])
            host = _host_tried(mine)
            if _openalex_done(row, mine) and (host or not row.get("host_pdf_urls")):
                if host:
                    status = "manual"
                    reason = "host-blocked" if host[-1]["outcome"] == "blocked" else "host-failed"
                elif any(a["tier"] == "openalex" for a in mine):
                    status, reason = "manual", "openalex-failed"
        row["status"], row["reason"] = status, reason


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
    """Runs the two tiers of one pass, recording each download in the journal as it lands."""

    def __init__(
        self,
        root: Path,
        state: PaperTables,
        journal: _Journal,
        transport: Transport,
        host_limiter: RateLimiter,
        linked: set[str],
        today: dt.date,
        log: Callable[[str], None],
    ) -> None:
        self.root, self.state, self.journal = root, state, journal
        self.transport, self.host_limiter, self.linked = transport, host_limiter, linked
        self.today, self.log = today, log
        self._clock = time.monotonic()

    def openalex(self, accounts: list[Account]) -> None:
        """Spend each account's quota on the papers waiting for OpenAlex's copy."""
        today = self.today
        tries = _tries(self.state)
        spent: Counter[str] = Counter()
        stopped: set[str] = set()
        for a in self.state.attempts:
            if a["tier"] == "openalex" and a.get("date") == today:
                if a["outcome"] in SPENT:
                    spent[a["account"]] += 1
                elif a["outcome"] == "rate-limited":
                    stopped.add(a["account"])
        held = self.state.held()
        queue = _order(
            self.state,
            (
                k
                for k, r in self.state.papers.items()
                if (r.get("status"), r.get("reason")) == ("waiting", "openalex-backlog")
                and k in self.linked
                and not r.get("retracted")
                and k not in held
                and _pmc_passed(r, tries.get(k, []))
                and not _openalex_done(r, tries.get(k, []))
                and not any(
                    a["tier"] == "openalex" and a.get("date") == today and a["outcome"] in SPENT
                    for a in tries.get(k, [])
                )
            ),
        )
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
                self._attempt(key, "openalex", account.email, "rate-limited", "HTTP 429")
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
            self._attempt(key, "openalex", account.email, outcome, got.detail)
            if got.ok:
                self._file(key, "openalex", url, got)
            spent[account.email] += 1
            if spent[account.email] >= QUOTA:
                active.remove(account)
            else:
                turn += 1
            done += 1
            self._progress(f"OpenAlex: {done}/{len(queue)} papers tried")

    def hosts(self) -> None:
        """Try each free host link once, for papers OpenAlex cannot serve."""
        held = self.state.held()
        tries = _tries(self.state)
        queue = _order(
            self.state,
            (
                k
                for k, r in self.state.papers.items()
                if r.get("status") != "skipped"
                and k in self.linked
                and not r.get("retracted")
                and r.get("host_pdf_urls")
                and k not in held
                and not _host_tried(tries.get(k, []))
                and _pmc_passed(r, tries.get(k, []))
                and _openalex_done(r, tries.get(k, []))
            ),
        )
        self.log(f"host links: {len(queue)} papers to try")
        for n, key in enumerate(queue, 1):
            got, url = _Got(), ""
            for url in self.state.papers[key]["host_pdf_urls"]:
                with self.host_limiter.turn():
                    got = self._get(key, url, {})
                if got.ok:
                    break
            if got.ok:
                self._attempt(key, "host", None, "fetched", url)
                self._file(key, "host", url, got)
            else:
                blocked = got.status in _BLOCKED or got.detail == "not a PDF"
                outcome = "blocked" if blocked else "failed"
                self._attempt(key, "host", None, outcome, f"{got.detail}: {url}")
            self._progress(f"host links: {n}/{len(queue)} papers tried")

    def _get(self, key: str, url: str, headers: dict[str, str]) -> _Got:
        name = paper_dir(key).name + ".pdf"
        dest = self.root / paper_dir(key) / name
        return _download(self.transport, url, dest, headers)

    def _attempt(self, key: str, tier: str, account: str | None, outcome: str, detail: str) -> None:
        row = {
            "paper": key,
            "tier": tier,
            "date": self.today,
            "account": account,
            "outcome": outcome,
            "detail": detail or None,
        }
        self.journal.add("attempt", row)
        self.state.attempts.append(row)

    def _file(self, key: str, tier: str, url: str, got: _Got) -> None:
        name = paper_dir(key).name + ".pdf"
        row = {
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
        self.journal.add("file", row)
        _put_file(self.state, row)

    def _progress(self, line: str) -> None:
        if time.monotonic() - self._clock >= 60:
            self._clock = time.monotonic()
            self.log(line)


def _put_file(state: PaperTables, row: dict[str, Any]) -> None:
    """Add a PDF's ``files`` row, replacing one of the same name, and set the paper's tier."""
    same = (row["paper"], row["kind"], row["name"])
    state.files[:] = [f for f in state.files if (f["paper"], f["kind"], f["name"]) != same]
    state.files.append(row)
    if row["paper"] in state.papers:
        state.papers[row["paper"]]["pdf_tier"] = row["tier"]


def _download(transport: Transport, url: str, dest: Path, headers: dict[str, str]) -> _Got:
    """GET a PDF to ``dest``, following redirects; anything but a PDF is a failure.

    An ``Authorization`` header is not passed on to another host. ``dest`` is written through
    a temp file and a rename, so it is whole or absent.
    """
    headers = {"User-Agent": USER_AGENT, "Accept": "application/pdf", **headers}
    for _ in range(_REDIRECTS + 1):
        try:
            with transport.open("GET", url, body=None, headers=headers, timeout=_TIMEOUT) as r:
                if r.status in (301, 302, 303, 307, 308):
                    location = r.getheader("Location")
                    r.read()
                    if not location:
                        return _Got(status=r.status, detail=f"HTTP {r.status} with no Location")
                    new = urllib.parse.urljoin(url, location)
                    if urllib.parse.urlsplit(new).netloc != urllib.parse.urlsplit(url).netloc:
                        headers = {k: v for k, v in headers.items() if k != "Authorization"}
                    url = new
                    continue
                if r.status != 200:
                    r.read()
                    return _Got(
                        status=r.status,
                        detail=f"HTTP {r.status}",
                        retry_after=_seconds(r.getheader("Retry-After")),
                    )
                return _save_pdf(r, dest)
        except (OSError, ValueError) as err:
            return _Got(detail=type(err).__name__)
    return _Got(status=310, detail="too many redirects")


def _save_pdf(response: Any, dest: Path) -> _Got:
    first = response.read(_CHUNK)
    if b"%PDF" not in first[:1024]:
        return _Got(status=200, detail="not a PDF")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(dest)
    md5 = hashlib.md5(usedforsecurity=False)
    size = 0
    try:
        with tmp.open("wb") as fh:
            chunk = first
            while chunk:
                fh.write(chunk)
                md5.update(chunk)
                size += len(chunk)
                chunk = response.read(_CHUNK)
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)
    return _Got(ok=True, status=200, size=size, md5=md5.hexdigest())


def _seconds(text: str | None) -> float | None:
    try:
        return float(text) if text else None
    except ValueError:
        return None


class _Journal:
    """One JSON line per download, flushed to disk before the pass moves on."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def add(self, kind: str, row: dict[str, Any]) -> None:
        line = json.dumps({"kind": kind, "row": row}, default=str)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def replay(self, state: PaperTables) -> bool:
        """Apply a stopped pass's rows to ``state``, skipping those already there."""
        if not self.path.is_file():
            return False
        for line in self.path.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue  # a line cut short when the pass stopped
            row = entry["row"]
            for column in ("date", "fetched"):
                if row.get(column):
                    row[column] = dt.date.fromisoformat(row[column])
            if entry["kind"] == "file":
                _put_file(state, row)
            elif row not in state.attempts:
                state.attempts.append(row)
        return True

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)


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


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def _record(root: Path, last_pass: dict[str, Any]) -> None:
    write_status(root, replace(read_status(root), last_pass=last_pass))
