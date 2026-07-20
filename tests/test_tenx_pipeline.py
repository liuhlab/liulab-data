"""Tests for the tenx Snakemake driver (labdata.tenx.pipeline).

The ``snakemake`` subprocess seam (``pipeline._run_snakemake``) is monkeypatched, so
these assert what the driver *hands to* Snakemake (assembled argv + generated config)
and how it reads results back from the ``.<SRR>.tenx.done`` markers — without running
Snakemake.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from labdata import Run
from labdata.exceptions import DownloadError
from labdata.tenx import pipeline


def _tasks(srx_dir: Path) -> list[tuple[Run, Path]]:
    return [(Run("SRR9000001"), srx_dir), (Run("SRR9000002"), srx_dir)]


def _run(
    pipeline_kwargs: dict, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, bool], list[str], dict]:
    """Run ``pipeline.run`` with a recording fake seam; return (result, argv, config)."""
    argv: list[str] = []
    config: dict = {}

    def fake(passed_argv: list[str], *, verbose: bool) -> int:
        argv[:] = passed_argv
        config.update(
            json.loads(Path(passed_argv[passed_argv.index("--configfile") + 1]).read_text())
        )
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(**pipeline_kwargs)
    return result, argv, config


def test_run_empty_tasks_short_circuits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called = False

    def fake(argv: list[str], *, verbose: bool) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        [],
        output_root=tmp_path,
        cores=8,
        threads_per_run=4,
        reads_per_fastq=1000,
        verbose=False,
    )
    assert result == {}
    assert called is False  # no Snakemake invocation for an empty batch


def test_run_assembles_argv_and_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    result, argv, config = _run(
        {
            "tasks": _tasks(srx_dir),
            "output_root": tmp_path,
            "cores": 16,
            "threads_per_run": 6,
            "reads_per_fastq": 2000,
            "verbose": False,
        },
        monkeypatch,
    )

    # Resource knobs and workflow wiring reach Snakemake. (`.index` raises if absent.)
    assert argv[argv.index("--cores") + 1] == "16"
    assert argv[argv.index("--snakefile") + 1].endswith("pipeline.smk")
    assert argv[argv.index("--directory") + 1] == str(tmp_path)
    assert "--keep-going" in argv
    assert "--rerun-incomplete" in argv
    assert "--keep-incomplete" in argv
    assert "--nolock" in argv
    assert argv[argv.index("--rerun-triggers") + 1] == "mtime"
    # No network step -> no ncbi resource is declared.
    assert "--resources" not in argv

    # The generated config maps each run to its experiment dir as a single path
    # component relative to the workdir (output_root).
    assert config["runs"] == {"SRR9000001": "SRX5921017", "SRR9000002": "SRX5921017"}
    assert config["threads_per_run"] == 6
    assert config["reads_per_fastq"] == 2000
    assert config["labdata"].endswith("-m labdata")
    # Both runs are reported present only once their markers exist (none yet).
    assert result == {"SRR9000001": False, "SRR9000002": False}


def test_run_reads_results_from_done_markers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    srx_dir = tmp_path / "SRX5921017"

    def fake(argv: list[str], *, verbose: bool) -> int:
        output_root = Path(argv[argv.index("--directory") + 1])
        # Only the first run finishes (its .tenx.done marker lands under the experiment dir).
        done = output_root / "SRX5921017" / ".SRR9000001.tenx.done"
        done.parent.mkdir(parents=True, exist_ok=True)
        done.touch()
        return 1

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        _tasks(srx_dir),
        output_root=tmp_path,
        cores=8,
        threads_per_run=4,
        reads_per_fastq=1000,
        verbose=False,
    )
    assert result == {"SRR9000001": True, "SRR9000002": False}


def test_snakefile_is_packaged() -> None:
    snakefile = pipeline._snakefile()
    assert snakefile.name == "pipeline.smk"
    assert snakefile.exists()


def test_run_snakemake_missing_binary_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from labdata import _pipeline

    def missing(*args: object, **kwargs: object) -> None:
        raise FileNotFoundError("snakemake")

    # The seam now lives in the shared driver; ``_run_snakemake`` delegates to it.
    monkeypatch.setattr(_pipeline.subprocess, "run", missing)
    with pytest.raises(DownloadError, match="snakemake"):
        pipeline._run_snakemake(["--version"], verbose=False)


# --------------------------------------------------------------------------- #
# whole-pipeline success flag
# --------------------------------------------------------------------------- #


def _fake_completes(argv: list[str], *, verbose: bool) -> int:
    """A fake run that leaves Snakemake state and marks every run done."""
    output_root = Path(argv[argv.index("--directory") + 1])
    (output_root / ".snakemake").mkdir(exist_ok=True)  # Snakemake's internal state
    config = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())
    for srr, srx in config["runs"].items():
        done = output_root / srx / f".{srr}.tenx.done"
        done.parent.mkdir(parents=True, exist_ok=True)
        done.touch()
    return 0


def test_success_flag_written_and_snakemake_wiped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(pipeline, "_run_snakemake", _fake_completes)
    result = pipeline.run(
        _tasks(tmp_path / "SRX5921017"),
        output_root=tmp_path,
        cores=8,
        threads_per_run=4,
        reads_per_fastq=1000,
        verbose=False,
    )
    assert result == {"SRR9000001": True, "SRR9000002": True}
    assert (tmp_path / ".labdata" / "tenx_success.json").exists()
    assert not (tmp_path / ".snakemake").exists()


def test_success_flag_short_circuits_rerun(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pipeline._write_success(tmp_path, {"SRR9000001": "SRX5921017", "SRR9000002": "SRX5921017"})
    called = False

    def fake(argv: list[str], *, verbose: bool) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        _tasks(tmp_path / "SRX5921017"),
        output_root=tmp_path,
        cores=8,
        threads_per_run=4,
        reads_per_fastq=1000,
        verbose=False,
    )
    assert result == {"SRR9000001": True, "SRR9000002": True}
    assert called is False  # the success flag alone is enough — no Snakemake needed


def test_success_flag_runs_only_uncovered_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pipeline._write_success(tmp_path, {"SRR9000001": "SRX5921017"})  # only the first is done
    seen: dict[str, dict] = {}

    def fake(argv: list[str], *, verbose: bool) -> int:
        seen["runs"] = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())["runs"]
        return _fake_completes(argv, verbose=verbose)

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        _tasks(tmp_path / "SRX5921017"),
        output_root=tmp_path,
        cores=8,
        threads_per_run=4,
        reads_per_fastq=1000,
        verbose=False,
    )
    assert seen["runs"] == {"SRR9000002": "SRX5921017"}
    assert result == {"SRR9000001": True, "SRR9000002": True}
