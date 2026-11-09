"""``scripts/store/check_stage1.py``: the 13 thin-slice checks, and the every-record pass on a flag."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests._slices import built_root

SCRIPT = Path(__file__).parents[1] / "scripts" / "store" / "check_stage1.py"


@pytest.fixture(scope="module")
def root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp("check") / "store", "sra_slice")


def _names(root: Path, tmp_path: Path, *flags: str) -> list[str]:
    """Run the script on ``root`` and return the name of each check it reports."""
    env = {k: v for k, v in os.environ.items() if k != "LABDATA_STORE"}
    env["XDG_CACHE_HOME"] = str(tmp_path / "cache")
    done = subprocess.run(
        [sys.executable, str(SCRIPT), str(root), *flags],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    return [
        line.split(" | ")[1]
        for line in done.stdout.splitlines()
        if line.startswith(("PASS", "FAIL"))
    ]


def test_by_default_it_runs_the_13_thin_slice_checks_only(root: Path, tmp_path: Path) -> None:
    names = _names(root, tmp_path)
    assert len(names) == 13, names
    assert not [n for n in names if n.startswith("every record")], names


def test_the_flag_adds_the_every_record_pass(root: Path, tmp_path: Path) -> None:
    names = _names(root, tmp_path, "--every-record")
    assert len(names) == 15, names
    assert [n for n in names if n.startswith("every record")] == [
        "every record: open all held",
        "every record: content_hash recomputes from store.raw",
    ]
