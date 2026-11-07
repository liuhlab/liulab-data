"""The paper job: one long-running process that runs each pass when it is due.

From 00:30 UTC each day, the daily pass is due when ``paper-status.json`` records no
daily-pass success that day, and from the 3rd of a month the PMC sync is due when it records
no PMC sync success that month. Both are
read afresh before each check, so after a restart only what is due runs. Every pass ends
with a copy to GCS. At start the job submits its own successor, which Slurm starts once this
job ends for any reason.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from labdata._http import HttpTransport, Transport
from labdata.ncbi.entrez import EntrezClient
from labdata.papers.copy import copy, destination
from labdata.papers.daily import daily_pass
from labdata.papers.openalex import OpenAlex
from labdata.papers.root import PaperStore
from labdata.papers.status import read_status
from labdata.papers.sync import pmc_sync
from labdata.store._files import write_atomic

#: The daily pass waits this long after midnight UTC, past OpenAlex's quota reset.
DAILY_AT = dt.time(0, 30)

#: The PMC sync's day of the month.
SYNC_DAY = 3

#: Seconds between checks while idle; each prints one line.
TICK = 60.0

#: A failed pass is tried again after this long.
RETRY = dt.timedelta(hours=1)

#: Records which successor this job submitted, so a requeued job submits no second one.
SUCCESSOR = ".successor"


def _submit(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run ``sbatch``; the one seam through which the job submits its successor."""
    return subprocess.run(args, capture_output=True, text=True, check=False)


def due(store: PaperStore, now: dt.datetime) -> list[str]:
    """Return the passes due at ``now``, daily first: ``daily``, ``pmc-sync``, or both.

    Parameters
    ----------
    store : PaperStore
        The paper store; its ``paper-status.json`` says what last succeeded.
    now : datetime.datetime
        The time, in UTC.

    Returns
    -------
    list of str
        The kinds of pass due.
    """
    status = read_status(store.root)
    today = now.date()
    out = []
    if now.time() >= DAILY_AT and status.last_daily_success != today.isoformat():
        out.append("daily")
    last_sync = status.last_pmc_sync_success or ""
    if now.time() >= DAILY_AT and today.day >= SYNC_DAY and last_sync[:7] != today.isoformat()[:7]:
        out.append("pmc-sync")
    return out


def serve(
    store: PaperStore,
    *,
    client: EntrezClient,
    openalex: OpenAlex,
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
    openalex : OpenAlex
        The OpenAlex client the passes use.
    clock : callable
        Returns the time, in UTC.
    transport : Transport or None
        Sends the passes' downloads; kept-open connections by default.
    sleep : callable
        Waits a number of seconds.
    until : datetime.datetime or None
        Stop at this time; ``None`` runs until the job is stopped.
    job_id : str or None
        This Slurm job's ID; with ``successor``, the next job is submitted to start after it.
    successor : str or None
        The batch script the next job runs.
    log : callable
        Takes one line for the job log.
    """
    if job_id and successor:
        _submit_successor(store, job_id, successor, lambda line: log(f"{_stamp(clock())} {line}"))
    transport = transport or HttpTransport()
    retry_at: dict[str, dt.datetime] = {}
    while until is None or clock() < until:
        now = clock()
        ready = [k for k in due(store, now) if retry_at.get(k, now) <= now]
        if not ready:
            _idle(store, now, log)
            sleep(TICK)
            continue
        kind = ready[0]
        stamp, today = _stamp(now), now.date()
        try:
            with _ticker(kind, log):
                if kind == "daily":
                    daily_pass(
                        store,
                        client=client,
                        openalex=openalex,
                        today=today,
                        transport=transport,
                        log=log,
                    )
                else:
                    pmc_sync(
                        store,
                        client=client,
                        openalex=openalex,
                        today=today,
                        transport=transport,
                        log=log,
                    )
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
    """Submit the next job once per job ID, to start when this one ends."""
    record = store.root / SUCCESSOR
    if record.is_file() and record.read_text(encoding="utf-8").split()[:1] == [job_id]:
        log(f"successor already submitted by job {job_id}")
        return
    args = ["sbatch", "--parsable", f"--dependency=afterany:{job_id}", script]
    try:
        done = _submit(args)
    except OSError as err:
        log(f"successor not submitted: {err}; submit it by hand")
        return
    if done.returncode:
        log(f"successor not submitted: {done.stderr.strip()}; submit it by hand")
        return
    next_id = done.stdout.strip().split(";")[0]
    write_atomic(record, f"{job_id} {next_id}\n")
    log(f"successor job {next_id} submitted, to start after job {job_id}")


def _idle(store: PaperStore, now: dt.datetime, log: Callable[[str], None]) -> None:
    status = read_status(store.root)
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


def _stamp(now: dt.datetime) -> str:
    return now.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%MZ")
