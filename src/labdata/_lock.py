"""The run lock: one holder at a time, and the steps it starts hold it too.

The lock is an owner record made by an atomic create. A start that finds it held takes it over
when Slurm or the owner's host says the owner is gone, and otherwise refuses unless told the
owner is gone. Each store gives its own lock path and the error a refused start raises.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from labdata import _slurm
from labdata._files import create

logger = logging.getLogger(__name__)

#: The environment variable naming the lock's holder, ``host:pid``, to the steps it starts.
RUN_OWNER = "LABDATA_RUN_OWNER"


@contextmanager
def run_lock(
    lock: Path, holder: str, *, error: type[Exception], owner_gone: bool = False
) -> Iterator[None]:
    """Hold the run lock at ``lock``, so one holder runs at a time.

    A step the holder starts inherits :data:`RUN_OWNER` in its environment, so it holds the
    lock too. A lock left by an owner that is gone is taken over.

    Parameters
    ----------
    lock : pathlib.Path
        The lock file; its folder is made if missing.
    holder : str
        What takes the lock, for the record, such as an update or a command's name.
    error : type of Exception
        Raised, with a message, when the lock cannot be taken.
    owner_gone : bool
        The holder has ended, though this host cannot check it.

    Raises
    ------
    Exception
        ``error``, if the holder still runs, or this host cannot check it and ``owner_gone``
        is False.
    """
    held = _read(lock)
    if held is not None and os.environ.get(RUN_OWNER) == _token(held):
        yield
        return
    mine = _take(lock, holder, error, owner_gone)
    outer = os.environ.get(RUN_OWNER)
    os.environ[RUN_OWNER] = _token(mine)
    try:
        yield
    finally:
        if outer is None:
            os.environ.pop(RUN_OWNER, None)
        else:
            os.environ[RUN_OWNER] = outer
        if _read(lock) == mine:
            lock.unlink(missing_ok=True)


def _take(lock: Path, holder: str, error: type[Exception], owner_gone: bool) -> dict[str, Any]:
    """Create the lock, taking it over from a holder that is gone; return its record."""
    mine = {"update": holder, **_slurm.Owner.me().record()}
    lock.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(3):
        if create(lock, json.dumps(mine, indent=2, sort_keys=True) + "\n"):
            return mine
        held = _read(lock)
        if held is None:
            continue
        owner = _slurm.Owner.from_record(held)
        alive = None if owner is None else _slurm.alive(owner)
        if alive or (alive is None and not owner_gone):
            raise error(_busy(lock, held, owner, alive))
        logger.warning("the run lock's holder (%s) is gone, so this %s takes it", owner, holder)
        if _read(lock) == held:
            lock.unlink(missing_ok=True)
    raise error(f"another update took the run lock {lock} as this {holder} started")


def _read(lock: Path) -> dict[str, Any] | None:
    """Return the lock's record: None with no lock, empty when it cannot be read."""
    try:
        text = lock.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        record = json.loads(text)
    except ValueError:
        return {}
    return record if isinstance(record, dict) else {}


def _token(record: Mapping[str, Any]) -> str:
    return f"{record.get('host')}:{record.get('pid')}"


def _busy(
    lock: Path, held: Mapping[str, Any], owner: _slurm.Owner | None, alive: bool | None
) -> str:
    if owner is None:
        return f"the run lock {lock} cannot be read; if no update runs, run again with --owner-gone"
    holds = f"a {held.get('update', 'run')} holds this store's run lock ({lock}): {owner}"
    if alive:
        return f"{holds}; wait for it to end"
    return (
        f"{holds}, and this host cannot check whether it still runs; if it has ended, run "
        "again with --owner-gone"
    )
