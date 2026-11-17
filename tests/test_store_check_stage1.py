"""``scripts/store/check_stage1.py``: the 13 thin-slice checks, and the every-record pass on a flag."""

from __future__ import annotations

import io
import runpy
import socket
import sys
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

from tests._slices import BuiltStore

SCRIPT = Path(__file__).parents[1] / "scripts" / "store" / "check_stage1.py"


def _run(root: Path, flags: Sequence[str]) -> str:
    """Run the script on ``root`` in this process, as ``check_stage1.py root flags`` would.

    The script blocks the network for its whole process; the block is lifted once it exits.
    """
    out = io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        for name in ("connect", "connect_ex"):
            patch.setattr(socket.socket, name, getattr(socket.socket, name))
        for name in ("create_connection", "getaddrinfo"):
            patch.setattr(socket, name, getattr(socket, name))
        patch.setattr(sys, "argv", [str(SCRIPT), str(root), *flags])
        with redirect_stdout(out), redirect_stderr(io.StringIO()), pytest.raises(SystemExit):
            runpy.run_path(str(SCRIPT), run_name="__main__")
    return out.getvalue()


@pytest.fixture(scope="module")
def runs(built_store: BuiltStore, tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[str]]:
    """Run the script on one store, with no flag and with ``--every-record``.

    Return the name of each check each run reports.
    """
    tmp = tmp_path_factory.mktemp("check")
    root = built_store(tmp / "store", "sra_slice")
    with pytest.MonkeyPatch.context() as patch:
        patch.delenv("LABDATA_STORE", raising=False)
        patch.setenv("XDG_CACHE_HOME", str(tmp / "cache"))
        return {
            flags: [
                line.split(" | ")[1]
                for line in _run(root, flags.split()).splitlines()
                if line.startswith(("PASS", "FAIL"))
            ]
            for flags in ("", "--every-record")
        }


def test_by_default_it_runs_the_13_thin_slice_checks_only(runs: dict[str, list[str]]) -> None:
    names = runs[""]
    assert len(names) == 13, names
    assert not [n for n in names if n.startswith("every record")], names


def test_the_flag_adds_the_every_record_pass(runs: dict[str, list[str]]) -> None:
    names = runs["--every-record"]
    assert len(names) == 15, names
    assert [n for n in names if n.startswith("every record")] == [
        "every record: open all held",
        "every record: content_hash recomputes from store.raw",
    ]
