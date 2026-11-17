"""The paper job: one long-running process that runs each pass when it is due.

From 00:30 UTC each day, the daily pass is due when ``paper-status.json`` records no
daily-pass success that day. The PMC sync is due when it records no PMC sync success this
month, from the 3rd, or records a GEO/SRA build other than the store's current one. The file
and the store's marker are read afresh before each check, so after a restart only what is
due runs. Every pass ends with a copy to GCS. At start the job keeps ``QUEUED`` jobs queued
behind it, each to start once the jobs before it end for any reason, so one that fails at
start does not end the chain. A running job cannot see its successor fail, as the successor
starts only once it ends.
"""

from __future__ import annotations

import datetime as dt
import posixpath
import re
import subprocess
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress

from labdata._http import HttpTransport, Transport
from labdata.ncbi.entrez import EntrezClient
from labdata.papers.copy import copy, destination
from labdata.papers.daily import daily_pass
from labdata.papers.openalex import OpenAlex
from labdata.papers.root import PaperStore
from labdata.papers.status import PaperStatus, read_status
from labdata.papers.sync import pmc_sync
from labdata.store._files import write_atomic
from labdata.store.reader import current_build

#: The daily pass waits this long after midnight UTC, past OpenAlex's quota reset.
DAILY_AT = dt.time(0, 30)

#: The PMC sync's day of the month.
SYNC_DAY = 3

#: Seconds between checks while idle; each prints one line.
TICK = 60.0

#: A failed pass is tried again after this long.
RETRY = dt.timedelta(hours=1)

#: Records the chain: a job, then the jobs queued behind it.
SUCCESSOR = ".successor"

#: Jobs kept queued behind the running one.
QUEUED = 2


def _submit(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run ``sbatch``; the one seam through which the job submits its successor."""
    return subprocess.run(args, capture_output=True, text=True, check=False)


def _show_job(job_id: str) -> str:
    """Return ``scontrol show job`` output, or ``""``; the seam that reads a job's state."""
    try:
        done = subprocess.run(
            ["scontrol", "show", "job", job_id], capture_output=True, text=True, check=False
        )
    except OSError:
        return ""
    return done.stdout if done.returncode == 0 else ""


def due(
    store: PaperStore, now: dt.datetime, log: Callable[[str], None] = lambda _line: None
) -> list[str]:
    """Return the passes due at ``now``, daily first: ``daily``, ``pmc-sync``, or both.

    A new GEO/SRA build makes the PMC sync due only when the last full sync recorded the
    build it read. Only the GEO/SRA store's marker is read, never a build. A store that
    will not open makes no new build: ``log`` gets one line, and the date rule still holds.

    Parameters
    ----------
    store : PaperStore
        The paper store; its ``paper-status.json`` says what last succeeded.
    now : datetime.datetime
        The time, in UTC.
    log : callable
        Takes the line that says the GEO/SRA store will not open.

    Returns
    -------
    list of str
        The kinds of pass due.
    """
    return _due(store, read_status(store.root), now, log)


def _due(
    store: PaperStore, status: PaperStatus, now: dt.datetime, log: Callable[[str], None]
) -> list[str]:
    today = now.date()
    out = []
    if now.time() >= DAILY_AT and status.last_daily_success != today.isoformat():
        out.append("daily")
    last_sync = status.last_pmc_sync_success or ""
    monthly = today.day >= SYNC_DAY and last_sync[:7] != today.isoformat()[:7]
    if now.time() >= DAILY_AT and (monthly or _new_build(store, status.last_pmc_sync_build, log)):
        out.append("pmc-sync")
    return out


def _new_build(store: PaperStore, synced: str | None, log: Callable[[str], None]) -> bool:
    """Return whether the GEO/SRA store's marker names a build other than ``synced``."""
    if synced is None:
        return False
    try:
        current = current_build(store.store.root)
    except Exception as err:  # the job outlives a store it cannot open
        log(f"GEO/SRA store not readable, so no new build is seen: {err}")
        return False
    return current is not None and current != synced


def serve(
    store: PaperStore,
    *,
    client: EntrezClient,
    openalex: Callable[[], OpenAlex],
    clock: Callable[[], dt.datetime],
    transport: Transport | None = None,
    sleep: Callable[[float], None] = time.sleep,
    until: dt.datetime | None = None,
    job_id: str | None = None,
    successor: str | None = None,
    log: Callable[[str], None] = print,
) -> None:
    """Run each pass when it is due, one at a time, until ``until`` or forever.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    client : EntrezClient
        The NCBI client the passes use.
    openalex : callable
        Returns the OpenAlex client for one pass; called at each pass, so saved accounts
        count from the next pass.
    clock : callable
        Returns the time, in UTC.
    transport : Transport or None
        Sends the passes' downloads; kept-open connections by default.
    sleep : callable
        Waits a number of seconds.
    until : datetime.datetime or None
        Stop at this time; ``None`` runs until the job is stopped.
    job_id : str or None
        This Slurm job's ID; with ``successor``, jobs are kept queued behind it.
    successor : str or None
        The batch script the queued jobs run.
    log : callable
        Takes one line for the job log; a line it fails to write with ``OSError`` is dropped.
    """
    log = _drop_failed_lines(log)
    if job_id and successor:
        _submit_successor(store, job_id, successor, lambda line: log(f"{_stamp(clock())} {line}"))
    transport = transport or HttpTransport()
    retry_at: dict[str, dt.datetime] = {}
    while until is None or clock() < until:
        now = clock()
        stamp, today = _stamp(now), now.date()
        status = read_status(store.root)
        ready = [
            k
            for k in _due(store, status, now, lambda line, stamp=stamp: log(f"{stamp} {line}"))
            if retry_at.get(k, now) <= now
        ]
        if not ready:
            _idle(status, now, log)
            sleep(TICK)
            continue
        kind = ready[0]
        try:
            with _ticker(kind, log):
                alex = openalex()
                if kind == "daily":
                    daily_pass(
                        store,
                        client=client,
                        openalex=alex,
                        today=today,
                        transport=transport,
                        log=log,
                    )
                else:
                    report = pmc_sync(
                        store,
                        client=client,
                        openalex=alex,
                        today=today,
                        transport=transport,
                        log=log,
                    )
                    log(f"{stamp} {kind} pass {today}: {report.summary()}")
        except Exception:
            log(traceback.format_exc().rstrip())
            log(f"{stamp} {kind} pass {today}: failed; tried again after {RETRY}")
            retry_at[kind] = now + RETRY
            continue
        log(f"{stamp} {kind} pass {today}: success")
        _copy(store, log)


def _copy(store: PaperStore, log: Callable[[str], None]) -> None:
    if not destination(store):
        log("no copy_to in labdata-papers.toml: the GCS copy is skipped")
        return
    try:
        result = copy(store)
    except Exception as err:
        log(f"GCS copy failed; copy_pending is set: {err}")
        return
    log(result.message if result.skipped else f"copied the paper store to {result.destination}")


def _submit_successor(
    store: PaperStore, job_id: str, script: str, log: Callable[[str], None]
) -> None:
    """Keep ``QUEUED`` jobs behind this one, so one failing at start does not end the chain.

    The record lists the chain; a job after this one in it counts while Slurm shows it
    pending or running, or shows nothing.
    """
    record = store.root / SUCCESSOR
    chain = record.read_text(encoding="utf-8").split() if record.is_file() else []
    after_me = chain[chain.index(job_id) + 1 :] if job_id in chain else []
    queued = [next_id for next_id in after_me if _waiting(next_id)]
    if len(queued) >= QUEUED:
        log(f"successors {' and '.join(queued)} already queued behind job {job_id}")
        return
    log_dir = _log_dir(job_id)
    while len(queued) < QUEUED:
        after = [job_id, *queued]
        args = ["sbatch", "--parsable", f"--dependency=afterany:{':'.join(after)}", script]
        if log_dir is not None:
            # Otherwise the successor's log lands in its submit directory: the repo checkout.
            args.insert(-1, f"--output={posixpath.join(log_dir, 'labdata-papers.%j.log')}")
        try:
            done = _submit(args)
        except OSError as err:
            log(f"successor not submitted: {err}; submit it by hand")
            return
        if done.returncode:
            log(f"successor not submitted: {done.stderr.strip()}; submit it by hand")
            return
        queued.append(done.stdout.strip().split(";")[0])
        log(f"successor job {queued[-1]} submitted, to start after job {' and '.join(after)}")
        try:
            write_atomic(record, " ".join([job_id, *queued]) + "\n")
        except OSError as err:
            log(f"successor record not written: {err}; no more successors submitted")
            return


def _waiting(job_id: str) -> bool:
    """Return whether Slurm shows ``job_id`` pending or running, or shows nothing."""
    state = re.search(r"\bJobState=(\S+)", _show_job(job_id))
    return state is None or state[1] in {"PENDING", "RUNNING"}


def _log_dir(job_id: str) -> str | None:
    """Return the folder of this job's log, as Slurm reports it, or ``None``."""
    for line in _show_job(job_id).splitlines():
        key, _, value = line.strip().partition("=")
        if key == "StdOut" and value:
            return posixpath.dirname(value)
    return None


def _idle(status: PaperStatus, now: dt.datetime, log: Callable[[str], None]) -> None:
    log(
        f"{_stamp(now)} idle; last daily pass {status.last_daily_success or 'never'}, "
        f"last PMC sync {status.last_pmc_sync_success or 'never'}, "
        f"copy pending: {'yes' if status.copy_pending else 'no'}"
    )


@contextmanager
def _ticker(kind: str, log: Callable[[str], None]) -> Iterator[None]:
    """Log a line a minute while a pass runs, so a quiet step still shows the job alive."""
    stop = threading.Event()
    started = time.monotonic()

    def tick() -> None:
        while not stop.wait(TICK):
            log(f"{kind} pass running for {(time.monotonic() - started) / 60:.0f} min")

    thread = threading.Thread(target=tick, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


def _drop_failed_lines(log: Callable[[str], None]) -> Callable[[str], None]:
    """Wrap ``log`` so a full or failing disk drops the line instead of stopping the job."""

    def write(line: str) -> None:
        with suppress(OSError):
            log(line)

    return write


def _stamp(now: dt.datetime) -> str:
    return now.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%MZ")
