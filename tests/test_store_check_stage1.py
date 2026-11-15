"""``scripts/store/check_stage1.py``: the 13 thin-slice checks, and the every-record pass on a flag."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests._slices import BuiltStore

SCRIPT = Path(__file__).parents[1] / "scripts" / "store" / "check_stage1.py"


@pytest.fixture(scope="module")
def runs(built_store: BuiltStore, tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[str]]:
    """Run the script on one store, with no flag and with ``--every-record``, side by side.

    Return the name of each check each run reports.
    """
    tmp = tmp_path_factory.mktemp("check")
    root = built_store(tmp / "store", "sra_slice")
    env = {k: v for k, v in os.environ.items() if k != "LABDATA_STORE"}
    env["XDG_CACHE_HOME"] = str(tmp / "cache")
    started = {
        flags: subprocess.Popen(
            [sys.executable, str(SCRIPT), str(root), *flags.split()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        for flags in ("", "--every-record")
    }
    return {
        flags: [
            line.split(" | ")[1]
            for line in run.communicate()[0].splitlines()
            if line.startswith(("PASS", "FAIL"))
        ]
        for flags, run in started.items()
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
