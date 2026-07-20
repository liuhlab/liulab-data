"""Tests for the Snakemake download driver (labdata.geo.pipeline).

The ``snakemake`` subprocess seam (``pipeline._run_snakemake``) is monkeypatched, so
these assert what the driver *hands to* Snakemake (assembled argv + generated config)
and how it reads results back from the per-stage markers — without running Snakemake.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from labdata import Run
from labdata.exceptions import DownloadError
from labdata.geo import pipeline


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
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=False,
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
            "ncbi_parallel": 3,
            "cores": 16,
            "threads_per_run": 4,
            "max_size": "500G",
            "retries": 3,
            "backoff": 5.0,
            "keep_sra": True,
            "prefetch_only": False,
            "verbose": False,
        },
        monkeypatch,
    )

    # Resource knobs and workflow wiring reach Snakemake. (`.index` raises if absent.)
    assert argv[argv.index("--cores") + 1] == "16"
    assert argv[argv.index("--resources") + 1] == "ncbi=3"
    assert argv[argv.index("--snakefile") + 1].endswith("pipeline.smk")
    assert argv[argv.index("--directory") + 1] == str(tmp_path)
    assert "--keep-going" in argv
    assert "--rerun-incomplete" in argv  # resume interrupted jobs
    assert "--keep-incomplete" in argv  # keep a partial .sra so prefetch resumes
    assert "--nolock" in argv  # survive HPC walltime/OOM kills without a manual unlock
    assert argv[argv.index("--rerun-triggers") + 1] == "mtime"

    # The generated config maps each run to its experiment dir as a single path
    # component relative to the workdir (output_root).
    assert config["runs"] == {"SRR9000001": "SRX5921017", "SRR9000002": "SRX5921017"}
    assert config["max_size"] == "500G"
    assert config["threads_per_run"] == 4
    assert config["keep_sra"] is True
    assert config["prefetch_only"] is False
    assert config["labdata"].endswith("-m labdata")
    # Both runs are reported present once their markers exist.
    assert result == {"SRR9000001": False, "SRR9000002": False}


def test_run_reads_results_from_done_markers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    srx_dir = tmp_path / "SRX5921017"

    def fake(argv: list[str], *, verbose: bool) -> int:
        output_root = Path(argv[argv.index("--directory") + 1])
        # Only the first run finishes (its .done marker lands under the experiment dir).
        done = output_root / "SRX5921017" / ".SRR9000001.done"
        done.parent.mkdir(parents=True, exist_ok=True)
        done.touch()
        return 1

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        _tasks(srx_dir),
        output_root=tmp_path,
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=False,
        verbose=False,
    )
    assert result == {"SRR9000001": True, "SRR9000002": False}


def test_prefetch_only_reads_prefetched_markers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    srx_dir = tmp_path / "SRX5921017"

    def fake(argv: list[str], *, verbose: bool) -> int:
        config = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())
        assert config["prefetch_only"] is True
        output_root = Path(argv[argv.index("--directory") + 1])
        for srr, srx in config["runs"].items():
            sra = output_root / srx / srr / f"{srr}.sra"
            sra.parent.mkdir(parents=True, exist_ok=True)
            sra.touch()
        return 0

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        _tasks(srx_dir),
        output_root=tmp_path,
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=True,
        verbose=False,
    )
    # Success under prefetch_only is judged by the fetched .sra, not a .done marker.
    assert result == {"SRR9000001": True, "SRR9000002": True}


def test_run_partitions_original_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    _result, _argv, config = _run(
        {
            "tasks": _tasks(srx_dir),
            "output_root": tmp_path,
            "ncbi_parallel": 4,
            "cores": 8,
            "threads_per_run": 4,
            "max_size": "200G",
            "retries": 3,
            "backoff": 5.0,
            "keep_sra": False,
            "prefetch_only": False,
            "original": {"SRR9000002"},
            "verbose": False,
        },
        monkeypatch,
    )
    # The default run drives the sra chain; the original one is routed separately.
    assert config["runs"] == {"SRR9000001": "SRX5921017"}
    assert config["original_runs"] == {"SRR9000002": "SRX5921017"}


def test_run_reads_original_done_markers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"

    def fake(argv: list[str], *, verbose: bool) -> int:
        output_root = Path(argv[argv.index("--directory") + 1])
        # The original-format run finishes: its .original.done marker lands.
        done = output_root / "SRX5921017" / ".SRR9000002.original.done"
        done.parent.mkdir(parents=True, exist_ok=True)
        done.touch()
        return 1

    monkeypatch.setattr(pipeline, "_run_snakemake", fake)
    result = pipeline.run(
        _tasks(srx_dir),
        output_root=tmp_path,
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=False,
        original={"SRR9000002"},
        verbose=False,
    )
    # Original run present (its .original.done marker); the default run's .done is absent.
    assert result == {"SRR9000001": False, "SRR9000002": True}


def test_snakefile_is_packaged() -> None:
    snakefile = pipeline._snakefile()
    assert snakefile.name == "pipeline.smk"
    assert snakefile.exists()


def test_run_snakemake_missing_binary_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(*args: object, **kwargs: object) -> None:
        raise FileNotFoundError("snakemake")

    monkeypatch.setattr(pipeline.subprocess, "run", missing)
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
        done = output_root / srx / f".{srr}.done"
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
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=False,
        verbose=False,
    )
    assert result == {"SRR9000001": True, "SRR9000002": True}
    # The durable flag is written and Snakemake's internal state is wiped.
    assert (tmp_path / ".labdata" / "success.json").exists()
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
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=False,
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
        ncbi_parallel=4,
        cores=8,
        threads_per_run=4,
        max_size="200G",
        retries=3,
        backoff=5.0,
        keep_sra=False,
        prefetch_only=False,
        verbose=False,
    )
    # Only the uncovered run is handed to Snakemake; both are reported present.
    assert seen["runs"] == {"SRR9000002": "SRX5921017"}
    assert result == {"SRR9000001": True, "SRR9000002": True}
