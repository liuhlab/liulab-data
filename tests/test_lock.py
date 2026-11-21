"""The shared run lock, used at a path other than the GEO/SRA store's."""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path

import pytest

from labdata import _lock


class _BusyError(Exception):
    pass


#: A process ID no live process has.
_GONE = 2**22 + 1


def _leave(lock: Path, pid: int) -> None:
    """Leave the lock as a process ``pid`` on this host holds it."""
    lock.parent.mkdir(parents=True, exist_ok=True)
    owner = {"update": "load", "job": None, "host": socket.gethostname(), "pid": pid}
    lock.write_text(json.dumps(owner | {"started": "2026-11-21T00:00:00Z"}), encoding="utf-8")


def test_a_held_lock_at_a_second_path_refuses_a_second_start_and_lets_its_steps_in(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "other" / "run.lock"

    with _lock.run_lock(lock, "load", error=_BusyError):
        held = lock.read_bytes()
        assert json.loads(held)["update"] == "load"
        # A step the holder starts inherits its token, so it holds the lock too.
        with _lock.run_lock(lock, "step", error=_BusyError):
            assert lock.read_bytes() == held
        # A start that is not the holder's step is refused while the holder runs.
        os.environ.pop(_lock.RUN_OWNER)
        with (
            pytest.raises(_BusyError, match="wait for it to end"),
            _lock.run_lock(lock, "x", error=_BusyError),
        ):
            pass

    assert not lock.exists()


def test_a_lock_at_a_second_path_left_by_a_gone_owner_is_taken_over(tmp_path: Path) -> None:
    lock = tmp_path / "other" / "run.lock"
    _leave(lock, _GONE)

    with _lock.run_lock(lock, "load", error=_BusyError):
        assert json.loads(lock.read_text(encoding="utf-8"))["pid"] == os.getpid()

    assert not lock.exists()
