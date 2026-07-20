"""Tests for the per-run sra-tools stages and the record ``download`` API.

Two seams are faked so the whole download runs without tools or network:
``sratools._run`` is swapped for the recording :class:`FakeTools`, and
``pipeline._run_snakemake`` for :func:`fake_snakemake`, which drives the three stages
in-process while modelling Snakemake's output-based resume and ``temp()`` cleanup. The
stages (:func:`~labdata.geo.sratools.prefetch_run`, ``extract_run``, ``compress_run``)
are also exercised directly. Orchestration details (config, markers, resource flags)
live in ``test_pipeline.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from labdata import BioProject, Experiment, Series, SraDownloader
from labdata.exceptions import AccessionError, DownloadError
from labdata.geo import sratools
from tests import _geodata as g
from tests._download_fakes import FakeTools, fake_snakemake


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeTools:
    """Fake both download seams: the sra-tools subprocess and Snakemake."""
    tools = FakeTools()
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(sratools.pipeline, "_run_snakemake", fake_snakemake)
    return tools


def _experiment() -> Experiment:
    return Experiment(g.SRX, client=g.build_client())


# --------------------------------------------------------------------------- #
# whole-experiment download (stages driven through the faked DAG)
# --------------------------------------------------------------------------- #


def test_download_creates_gzipped_fastq_per_run(fake: FakeTools, tmp_path: Path) -> None:
    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    srx_dir = tmp_path / g.SRX
    assert srx_dir.is_dir()
    for srr in (g.SRR1, g.SRR2):
        assert (srx_dir / f"{srr}_1.fastq.gz").exists()
        assert (srx_dir / f"{srr}_2.fastq.gz").exists()


def test_download_reclaims_intermediates_and_marks_done(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    srx_dir = tmp_path / g.SRX

    # temp(): only gzipped FASTQ survive — no .sra dir, no staging, no bare .fastq.
    assert list(srx_dir.glob("*.sra")) == []
    assert list(srx_dir.glob("**/*.fastq")) == []
    assert not (srx_dir / g.SRR1).exists()
    assert not (srx_dir / f".{g.SRR1}.fq").exists()
    # Each finished run leaves a completion marker.
    for srr in (g.SRR1, g.SRR2):
        assert (srx_dir / f".{srr}.done").exists()


def test_keep_sra_preserves_sra_next_to_fastq(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download(keep_sra=True)
    srx_dir = tmp_path / g.SRX
    for srr in (g.SRR1, g.SRR2):
        # The .sra is stashed up beside the gzipped FASTQ instead of being reclaimed.
        assert (srx_dir / f"{srr}.sra").exists()
        assert (srx_dir / f"{srr}_1.fastq.gz").exists()
        # The prefetch download directory is still cleaned up.
        assert not (srx_dir / srr).exists()


def test_existing_done_marker_skips_the_run(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    srx_dir.mkdir(parents=True)
    (srx_dir / f".{g.SRR1}.done").touch()

    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    # The already-complete run is never prefetched; the other one still is.
    prefetched = [cmd[1] for cmd in fake.commands if cmd[0] == "prefetch"]
    assert g.SRR1 not in prefetched
    assert g.SRR2 in prefetched


def test_prefetched_sra_is_not_refetched_on_resume(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    # Simulate a run whose prefetch finished (its .sra dir is present) but that was
    # interrupted before extraction (no .done marker).
    sra_dir = srx_dir / g.SRR1
    sra_dir.mkdir(parents=True)
    (sra_dir / f"{g.SRR1}.sra").write_bytes(b"sra")

    SraDownloader(_experiment(), tmp_path).download()

    # SRR1 is not prefetched again, but its later steps still run.
    prefetched = [cmd[1] for cmd in fake.commands if cmd[0] == "prefetch"]
    assert g.SRR1 not in prefetched
    extracted = [Path(cmd[1]).name for cmd in fake.commands if cmd[0] == "fasterq-dump"]
    assert f"{g.SRR1}.sra" in extracted
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()


def test_prefetch_only_stops_after_prefetch(fake: FakeTools, tmp_path: Path) -> None:
    result = SraDownloader(_experiment(), tmp_path).download(prefetch_only=True)

    assert result == {g.SRR1: True, g.SRR2: True}
    # Only prefetch ran — no extraction or compression.
    assert fake.tools_used() == {"prefetch"}
    srx_dir = tmp_path / g.SRX
    for srr in (g.SRR1, g.SRR2):
        # The downloaded .sra is left in place; no FASTQ is produced.
        assert (srx_dir / srr / f"{srr}.sra").exists()
        assert list(srx_dir.glob(f"{srr}*.fastq.gz")) == []


def test_prefetch_only_then_full_download_resumes(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download(prefetch_only=True)
    fake.commands.clear()

    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    # The full rerun extracts and gzips without re-fetching the already-prefetched .sra.
    assert "prefetch" not in fake.tools_used()
    assert "fasterq-dump" in fake.tools_used()
    assert (tmp_path / g.SRX / f"{g.SRR1}_1.fastq.gz").exists()


def test_full_download_rerun_short_circuits(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    fake.commands.clear()

    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    # The whole-pipeline success flag short-circuits the rerun — no tools re-run.
    assert fake.commands == []


def test_prefetch_runs_before_fasterq_dump(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    order = fake.order_of("prefetch", "fasterq-dump")
    # For every run, prefetch precedes its fasterq-dump.
    assert order == ["prefetch", "fasterq-dump", "prefetch", "fasterq-dump"]


def test_uses_pigz_when_available(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    assert "pigz" in fake.tools_used()
    assert "gzip" not in fake.tools_used()


def test_falls_back_to_gzip_without_pigz(
    fake: FakeTools, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sratools, "_have", lambda tool: False)
    SraDownloader(_experiment(), tmp_path).download()
    assert "gzip" in fake.tools_used()
    assert "pigz" not in fake.tools_used()


def test_download_all_runs_with_ncbi_parallel(fake: FakeTools, tmp_path: Path) -> None:
    result = SraDownloader(_experiment(), tmp_path).download(ncbi_parallel=2)
    assert result == {g.SRR1: True, g.SRR2: True}


def test_failed_run_records_false_without_aborting(
    fake: FakeTools, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("tool exploded")

    monkeypatch.setattr(sratools, "_run", boom)
    result = SraDownloader(_experiment(), tmp_path).download()
    assert result == {g.SRR1: False, g.SRR2: False}


# --------------------------------------------------------------------------- #
# record-level nesting + experiment selection
# --------------------------------------------------------------------------- #


def test_series_download_nests_under_series_then_experiment(
    fake: FakeTools, tmp_path: Path
) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path)
    assert result == {g.SRR1: True, g.SRR2: True}
    # Layout: <output_dir>/<GSE>/<SRX>/<SRR>*.fastq.gz
    srx_dir = tmp_path / g.GSE / g.SRX
    assert srx_dir.is_dir()
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()


def test_bioproject_download_nests_under_bioproject_then_experiment(
    fake: FakeTools, tmp_path: Path
) -> None:
    result = BioProject(g.PRJNA, client=g.build_client()).download(tmp_path)
    assert result == {g.SRR1: True, g.SRR2: True}
    # Layout matches Series: <output_dir>/<PRJNA>/<SRX>/<SRR>*.fastq.gz
    srx_dir = tmp_path / g.PRJNA / g.SRX
    assert srx_dir.is_dir()
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()
    assert (srx_dir / f"{g.SRR2}_1.fastq.gz").exists()


def test_series_download_select_srx_selects_matching_experiment(
    fake: FakeTools, tmp_path: Path
) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, select_srx=[g.SRX])
    assert result == {g.SRR1: True, g.SRR2: True}
    assert (tmp_path / g.GSE / g.SRX / f"{g.SRR1}_1.fastq.gz").exists()


def test_series_download_select_srx_is_case_insensitive(fake: FakeTools, tmp_path: Path) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, select_srx=[g.SRX.lower()])
    assert result == {g.SRR1: True, g.SRR2: True}


def test_series_download_select_srx_excludes_unlisted_experiments(
    fake: FakeTools, tmp_path: Path
) -> None:
    # An SRX not linked to the Series filters every experiment out: nothing runs.
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, select_srx=["SRX0000000"])
    assert result == {}
    assert fake.tools_used() == set()
    assert not (tmp_path / g.GSE).exists()


def test_series_download_empty_select_srx_downloads_nothing(
    fake: FakeTools, tmp_path: Path
) -> None:
    # An explicit empty whitelist means "no experiments" (distinct from None = all).
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, select_srx=[])
    assert result == {}
    assert fake.tools_used() == set()


def test_series_download_select_srx_rejects_malformed_accession(
    fake: FakeTools, tmp_path: Path
) -> None:
    with pytest.raises(AccessionError):
        Series(g.GSE, client=g.build_client()).download(tmp_path, select_srx=["not-an-srx"])


def test_series_download_select_srx_rejects_non_srx_accession(
    fake: FakeTools, tmp_path: Path
) -> None:
    # A well-formed accession that is not an experiment (here a run) is rejected too.
    with pytest.raises(AccessionError):
        Series(g.GSE, client=g.build_client()).download(tmp_path, select_srx=[g.SRR1])


def test_bioproject_download_select_srx_selects_matching_experiment(
    fake: FakeTools, tmp_path: Path
) -> None:
    result = BioProject(g.PRJNA, client=g.build_client()).download(tmp_path, select_srx=[g.SRX])
    assert result == {g.SRR1: True, g.SRR2: True}
    assert (tmp_path / g.PRJNA / g.SRX / f"{g.SRR1}_1.fastq.gz").exists()


def test_series_download_prefetch_only(fake: FakeTools, tmp_path: Path) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, prefetch_only=True)
    assert result == {g.SRR1: True, g.SRR2: True}
    assert fake.tools_used() == {"prefetch"}
    srx_dir = tmp_path / g.GSE / g.SRX
    assert (srx_dir / g.SRR1 / f"{g.SRR1}.sra").exists()
    assert list(srx_dir.glob("*.fastq.gz")) == []


# --------------------------------------------------------------------------- #
# individual stages: staging, keep-sra, per-file gzip skip, prefetch retries
# --------------------------------------------------------------------------- #


def test_extract_writes_fastq_to_staging(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    run = _experiment().runs[0]
    sratools.prefetch_run(run, srx_dir)
    sratools.extract_run(run, srx_dir)

    staging = srx_dir / f".{g.SRR1}.fq"
    assert sorted(p.name for p in staging.glob("*.fastq")) == [
        f"{g.SRR1}_1.fastq",
        f"{g.SRR1}_2.fastq",
    ]
    # extract keeps the .sra in place (reclaiming it is Snakemake's job, via temp()).
    assert (srx_dir / g.SRR1 / f"{g.SRR1}.sra").exists()


def test_extract_keep_sra_stashes_sra(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    run = _experiment().runs[0]
    sratools.prefetch_run(run, srx_dir)
    sratools.extract_run(run, srx_dir, keep_sra=True)
    # keep_sra moves the .sra up so it survives the temp() reclaim of the prefetch dir.
    assert (srx_dir / f"{g.SRR1}.sra").exists()


def test_compress_skips_already_gzipped_mate(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    srx_dir.mkdir(parents=True)
    staging = srx_dir / f".{g.SRR1}.fq"
    staging.mkdir()
    (staging / f"{g.SRR1}_1.fastq").write_text("@r\nACGT\n+\nIIII\n")
    (staging / f"{g.SRR1}_2.fastq").write_text("@r\nACGT\n+\nIIII\n")
    # One mate is already compressed into the final dir from an earlier, interrupted run.
    (srx_dir / f"{g.SRR1}_1.fastq.gz").write_bytes(b"gz")

    sratools.compress_run(_experiment().runs[0], srx_dir, staging=staging)

    # Only the missing mate is handed to the gzip tool.
    gzipped = [arg for cmd in fake.commands if cmd[0] == "pigz" for arg in cmd[1:]]
    assert any(arg.endswith(f"{g.SRR1}_2.fastq") for arg in gzipped)
    assert not any(arg.endswith(f"{g.SRR1}_1.fastq") for arg in gzipped)
    assert (srx_dir / f"{g.SRR1}_2.fastq.gz").exists()


def test_prefetch_passes_max_size_with_default(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    prefetch = next(cmd for cmd in fake.commands if cmd[0] == "prefetch")
    assert "--max-size" in prefetch
    assert prefetch[prefetch.index("--max-size") + 1] == sratools.DEFAULT_MAX_SIZE


def test_default_max_size_is_200g() -> None:
    assert sratools.DEFAULT_MAX_SIZE == "200G"


def test_download_accepts_max_size_override(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download(max_size="500G")
    for cmd in fake.commands:
        if cmd[0] == "prefetch":
            assert cmd[cmd.index("--max-size") + 1] == "500G"


class FlakyTools(FakeTools):
    """A fake whose ``prefetch`` fails its first ``fail_times`` calls, then succeeds."""

    def __init__(self, fail_times: int) -> None:
        super().__init__()
        self.fail_times = fail_times
        self.prefetch_attempts = 0

    def run(self, cmd: list[str], *, cwd: Path | None = None) -> None:
        if cmd[0] == "prefetch":
            self.prefetch_attempts += 1
            if self.prefetch_attempts <= self.fail_times:
                self.commands.append(cmd)
                raise DownloadError("transient network blip")
        super().run(cmd, cwd=cwd)


def test_prefetch_retries_then_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tools = FlakyTools(fail_times=2)
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)

    run = Experiment(g.SRX, client=g.build_client()).runs[0]
    sratools.prefetch_run(run, tmp_path, retries=3)

    # Two failures + one success for the single run.
    assert tools.prefetch_attempts == 3


def test_prefetch_gives_up_after_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tools = FlakyTools(fail_times=99)  # never recovers
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)

    run = Experiment(g.SRX, client=g.build_client()).runs[0]
    with pytest.raises(DownloadError):
        sratools.prefetch_run(run, tmp_path, retries=3)
    assert tools.prefetch_attempts == 3


def test_extract_is_not_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"fasterq-dump": 0}

    def run(cmd: list[str], *, cwd: Path | None = None) -> None:
        if cmd[0] == "prefetch":
            out = Path(cmd[cmd.index("-O") + 1]) / cmd[1]
            out.mkdir(parents=True, exist_ok=True)
            (out / f"{cmd[1]}.sra").write_bytes(b"sra")
        elif cmd[0] == "fasterq-dump":
            attempts["fasterq-dump"] += 1
            raise DownloadError("extraction failed")

    monkeypatch.setattr(sratools, "_run", run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)

    run_obj = Experiment(g.SRX, client=g.build_client()).runs[0]
    sratools.prefetch_run(run_obj, tmp_path)
    with pytest.raises(DownloadError):
        sratools.extract_run(run_obj, tmp_path)
    # The local extraction step is run once — retries apply only to prefetch.
    assert attempts["fasterq-dump"] == 1


def test_run_seam_raises_download_error_on_missing_tool() -> None:
    # The real _run translates a missing executable into a domain error.
    with pytest.raises(DownloadError, match="not found"):
        sratools._run(["labdata-no-such-tool-xyz"])


# --------------------------------------------------------------------------- #
# disk preflight (rough, non-blocking warning)
# --------------------------------------------------------------------------- #


def _fake_disk_usage(free: int):
    from collections import namedtuple

    usage = namedtuple("usage", ["total", "used", "free"])
    return lambda _path: usage(free * 10, free * 9, free)


def test_low_disk_prints_warning(
    fake: FakeTools,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sratools.shutil, "disk_usage", _fake_disk_usage(free=100))
    Series(g.GSE, client=g.build_client()).download(tmp_path)
    assert "may be needed" in capsys.readouterr().err


def test_ample_disk_is_silent_about_space(
    fake: FakeTools,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sratools.shutil, "disk_usage", _fake_disk_usage(free=10**12))
    Series(g.GSE, client=g.build_client()).download(tmp_path)
    assert "may be needed" not in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# user-facing plan + summary output (per-run progress is emitted by Snakemake)
# --------------------------------------------------------------------------- #


def test_download_prints_plan_and_summary(
    fake: FakeTools, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Series(g.GSE, client=g.build_client()).download(tmp_path)
    err = capsys.readouterr().err
    assert g.GSE in err
    assert "2 runs, 1 experiment" in err
    assert g.SRX in err
    assert "Done: 2 ok, 0 failed." in err


def test_download_plan_reports_already_done_runs(
    fake: FakeTools, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    srx_dir = tmp_path / g.GSE / g.SRX
    srx_dir.mkdir(parents=True)
    (srx_dir / f".{g.SRR1}.done").touch()

    Series(g.GSE, client=g.build_client()).download(tmp_path)
    err = capsys.readouterr().err
    assert "1 of 2 already done" in err
    assert "Done: 2 ok, 0 failed." in err


def test_download_summary_reports_failed_runs(
    fake: FakeTools,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("tool exploded")

    monkeypatch.setattr(sratools, "_run", boom)

    Series(g.GSE, client=g.build_client()).download(tmp_path)
    err = capsys.readouterr().err
    assert "Done: 0 ok, 2 failed." in err


def test_verbose_false_is_silent(
    fake: FakeTools, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, verbose=False)
    assert result == {g.SRR1: True, g.SRR2: True}
    assert capsys.readouterr().err == ""
