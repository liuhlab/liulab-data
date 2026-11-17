"""The paper store's life cycle: each step runs under the lock and ends finalized when it can.

A step is a daily pass, a PMC sync or a copy, and holds the paper store's lock once, for the
pass and its copy together. A pass records its outcome, then finalizes, whatever the outcome,
when the ``files`` table and the copy record on the work disk name different paper files or a
copy is pending. Finalizing takes in a stopped daily pass's journal, marks a copy pending and
the store not finalized, then copies to GCS; the copy sends the status record last, marked
finalized and no longer pending. A copy that is skipped, fails or is cut off leaves it pending,
for the paper job's next idle tick.

This module is the only writer of ``paper-status.json``.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from labdata._http import RateLimiter, Transport
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import copy as copy_mod
from labdata.papers import daily as daily_mod
from labdata.papers import sync as sync_mod
from labdata.papers.copy import COPY_TO_KEY, CopyResult, PaperCopyError, destination
from labdata.papers.openalex import OpenAlex
from labdata.papers.root import MARKER, PaperStore, paper_lock
from labdata.papers.status import STATUS, PaperStatus, encode, read_status
from labdata.store._files import write_atomic
from labdata.store.reader import current_build
from labdata.store.targets import check_fraction

_Log = Callable[[str], None]


def daily(
    store: PaperStore,
    *,
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    transport: Transport | None = None,
    host_limiter: RateLimiter | None = None,
    log: _Log = print,
) -> daily_mod.DailyReport:
    """Run the daily pass, record it, and finalize; see :func:`labdata.papers.daily.daily_pass`.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    client, openalex, today, transport, host_limiter
        Passed to the pass.
    log : callable
        Takes one progress line, the copy's outcome among them.

    Returns
    -------
    daily_mod.DailyReport
        Today's downloads and the papers by status and reason.

    Raises
    ------
    Exception
        What the pass raised, after the copy that follows it.
    """
    day = today.isoformat()

    def run() -> daily_mod.DailyReport:
        return daily_mod.daily_pass(
            store,
            client=client,
            openalex=openalex,
            today=today,
            transport=transport,
            host_limiter=host_limiter,
            log=log,
        )

    def succeeded(status: PaperStatus, report: daily_mod.DailyReport) -> PaperStatus:
        same_day = status.fetches.get("date") == day
        others = status.fetches.get("by_tier", {}) if same_day else {}
        return replace(
            status,
            last_daily_success=day,
            fetches={"date": day, "by_tier": {**others, **report.fetched}},
            quota={"date": day, "by_key": report.quota},
            counts=report.counts,
        )

    return _pass(store, {"kind": "daily"}, run, succeeded, dict, log)


def pmc_sync(
    store: PaperStore,
    *,
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    transport: Transport | None = None,
    sample: float | None = None,
    log: _Log = print,
) -> sync_mod.SyncReport:
    """Run the PMC sync, record it, and finalize; see :func:`labdata.papers.sync.pmc_sync`.

    Only a sync over all papers records its date, and the GEO/SRA build current when it
    started, as the last successful PMC sync.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    client, openalex, today, transport, sample
        Passed to the sync.
    log : callable
        Takes one progress line, the copy's outcome among them.

    Returns
    -------
    sync_mod.SyncReport
        Counts of what the sync did, and papers by status and reason.

    Raises
    ------
    Exception
        What the sync raised, after the copy that follows it.
    """
    sample = check_fraction(sample)
    report = sync_mod.SyncReport()
    build = None

    def run() -> sync_mod.SyncReport:
        nonlocal build
        # Read first: a build that becomes current during the sync makes the next one due.
        build = current_build(store.store.root)
        return sync_mod.pmc_sync(
            store,
            client=client,
            openalex=openalex,
            today=today,
            transport=transport,
            sample=sample,
            log=log,
            report=report,
        )

    def succeeded(status: PaperStatus, report: sync_mod.SyncReport) -> PaperStatus:
        if sample is not None:  # a backfill round, never the month's sync
            return replace(status, counts=report.counts)
        return replace(
            status,
            last_pmc_sync_success=today.isoformat(),
            last_pmc_sync_build=build,
            counts=report.counts,
        )

    def ended() -> dict[str, Any]:
        return {
            "rekeyed": report.rekeyed,
            "turned_away": report.turned_away,
            "bucket_retries": report.bucket.retries,
            "bucket_throttled": report.bucket.throttled,
        }

    return _pass(store, {"kind": "pmc-sync", "sample": sample}, run, succeeded, ended, log)


def copy(
    store: PaperStore, to: str | None = None, *, full: bool = False, accept_deletes: int = 0
) -> CopyResult:
    """Copy the paper store to GCS by hand, whatever changed; see :func:`labdata.papers.copy.send`.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    to : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.
    full, accept_deletes
        Passed to the copy.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when the copy was skipped; the copy stays
        pending.

    Raises
    ------
    PaperCopyError
        If no destination is set, or ``full`` comes with ``accept_deletes``; or the copy's
        own refusals and failures, which leave the copy pending.
    """
    dest = to or destination(store)
    if not dest:
        raise PaperCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{COPY_TO_KEY} in {store.root / MARKER}"
        )
    if full and accept_deletes:
        raise PaperCopyError("--full deletes nothing, so it takes no --accept-deletes")
    with paper_lock(store.root):
        daily_mod.take_in(store.root)
        return _send(store, dest, full=full, accept_deletes=accept_deletes)


def finalize(store: PaperStore, log: _Log = print) -> CopyResult | None:
    """Copy to GCS when a copy is pending or the ``files`` table and the copy record differ.

    The paper job calls it at an idle tick while a copy is pending.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    log : callable
        Takes the copy's outcome, a failure among them.

    Returns
    -------
    CopyResult or None
        The copy's outcome; None when nothing was sent: no copy was owed, there is no
        destination, or the copy failed.
    """
    with paper_lock(store.root):
        return _finalize(store, log)


def _pass[R](
    store: PaperStore,
    record: dict[str, Any],
    run: Callable[[], R],
    succeeded: Callable[[PaperStatus, R], PaperStatus],
    ended: Callable[[], dict[str, Any]],
    log: _Log,
) -> R:
    """Run one pass under the lock, record its outcome, then finalize, whatever the outcome."""
    root = store.root
    with paper_lock(root):
        record = {**record, "start": _now(), "end": None, "outcome": "running"}
        _write(root, replace(read_status(root), last_pass=record))

        def outcome(name: str) -> dict[str, Any]:
            return {**record, **ended(), "end": _now(), "outcome": name}

        try:
            report = run()
        except BaseException as err:
            _write(root, replace(read_status(root), last_pass=outcome("failed")))
            if isinstance(err, Exception):  # a stop by hand leaves the copy to the next step
                _finalize(store, log)
            raise
        _write(root, succeeded(replace(read_status(root), last_pass=outcome("success")), report))
        _finalize(store, log)
    return report


def _finalize(store: PaperStore, log: _Log) -> CopyResult | None:
    """Copy when the work disk holds a change GCS lacks; a failure leaves it pending, logged."""
    root = store.root
    dest = destination(store)
    if not dest:
        daily_mod.take_in(root)
        if copy_mod.unsent(root):
            status = read_status(root)
            if status.finalized:
                _write(root, replace(status, finalized=False))
            log(f"no {COPY_TO_KEY} in {MARKER}: the GCS copy is skipped")
        return None
    try:
        daily_mod.take_in(root)
        if not (read_status(root).copy_pending or copy_mod.unsent(root)):
            return None
        result = _send(store, dest)
    except Exception as err:
        _write(root, replace(read_status(root), copy_pending=True, finalized=False))
        log(f"GCS copy failed; copy_pending is set: {err}")
        return None
    log(result.message if result.skipped else f"copied the paper store to {result.destination}")
    return result


def _send(
    store: PaperStore, dest: str, *, full: bool = False, accept_deletes: int = 0
) -> CopyResult:
    """Mark a copy pending, then copy; a copy that works sends the record marked finalized."""
    root = store.root
    owed = replace(read_status(root), copy_pending=True, finalized=False)
    _write(root, owed)
    start = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    done = replace(owed, copy_pending=False, finalized=True, last_copy_success=start)
    return copy_mod.send(store, dest, encode(done), full=full, accept_deletes=accept_deletes)


def _write(root: Path, status: PaperStatus) -> None:
    write_atomic(root / STATUS, encode(status).decode())


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
