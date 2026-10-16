"""``scripts/store/keep_run_logs.sh``: keep one round's minimal logs under ``support/``."""

from __future__ import annotations

import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts" / "store" / "keep_run_logs.sh"


def _keep(store: Path, round_name: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), str(store), round_name], capture_output=True, text=True, check=False
    )


def test_it_copies_the_rounds_logs_and_only_its_job_logs_and_deletes_nothing(
    tmp_path: Path,
) -> None:
    store = tmp_path / "store"
    run = store / "work" / "run"
    (run / "2026-10-14-all" / "logs").mkdir(parents=True)
    (run / "2026-10-14-all" / "logs" / "geo.log").write_text("geo\n")
    (run / "2026-10-14-all" / "run.json").write_text("{}\n")
    (run / "2026-10-14-all" / ".snakemake").mkdir()
    (run / "labdata-run.7.log").write_text("x labdata.store.pipeline round 2026-10-14-all: s\n")
    (run / "labdata-run.6.log").write_text("x labdata.store.pipeline round 2026-10-14-f0.1: s\n")
    kept = store / "support" / "logs" / "jobs" / "labdata-run.5.log"
    kept.parent.mkdir(parents=True)
    kept.write_text("older\n")

    for _ in range(2):
        result = _keep(store, "2026-10-14-all")
        assert result.returncode == 0, result.stderr

    support = store / "support" / "logs"
    assert sorted(p.relative_to(support).as_posix() for p in support.rglob("*")) == [
        "2026-10-14-all",
        "2026-10-14-all/geo.log",
        "2026-10-14-all/run.json",
        "jobs",
        "jobs/labdata-run.5.log",
        "jobs/labdata-run.7.log",
    ]
    assert (run / "labdata-run.7.log").is_file()


def test_it_refuses_a_round_that_has_no_folder(tmp_path: Path) -> None:
    result = _keep(tmp_path, "2026-10-14-all")
    assert result.returncode != 0
    assert "2026-10-14-all" in result.stderr
