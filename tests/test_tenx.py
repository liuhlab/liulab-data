"""Tests for the tenx bamtofastq stage + converter (labdata.tenx.bamtofastq).

The ``bamtofastq`` subprocess seam (``tenx.bamtofastq._run``) is monkeypatched with a
:class:`~tests._download_fakes.FakeTools` recorder, and the Snakemake seam with
:func:`~tests._download_fakes.fake_tenx_snakemake`, so these exercise BAM discovery,
the tool call, the flatten step, and the whole-project driver without the binary.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from labdata import Run, Series
from labdata.exceptions import DownloadError
from labdata.tenx import bamtofastq
from tests import _geodata as g
from tests._download_fakes import FakeTools, fake_tenx_snakemake


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeTools:
    """Monkeypatch both tenx seams with a recording fake; return the tool recorder."""
    tools = FakeTools()
    monkeypatch.setattr(bamtofastq, "_run", tools.run)
    monkeypatch.setattr(bamtofastq.pipeline, "_run_snakemake", fake_tenx_snakemake)
    return tools


def _seed_bam(srx_dir: Path, accession: str, name: str = "possorted_genome_bam.bam") -> Path:
    """Create a fake on-disk original BAM under ``<srx_dir>/<accession>/``."""
    run_dir = srx_dir / accession
    run_dir.mkdir(parents=True, exist_ok=True)
    bam = run_dir / name
    bam.write_bytes(b"bam")
    return bam


# --------------------------------------------------------------------------- #
# auto-detection
# --------------------------------------------------------------------------- #


def test_is_tenx_bam_run_true_for_bam() -> None:
    run = Run(g.SRR_TENX, sdl_client=g.build_sdl_client())
    assert bamtofastq.is_tenx_bam_run(run) is True


def test_is_tenx_bam_run_false_for_fastq_only() -> None:
    # SRR1's original files are FASTQ, not a BAM.
    run = Run(g.SRR1, sdl_client=g.build_sdl_client())
    assert bamtofastq.is_tenx_bam_run(run) is False


# --------------------------------------------------------------------------- #
# the per-run stage
# --------------------------------------------------------------------------- #


def test_bamtofastq_run_flattens_output(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    bam = _seed_bam(srx_dir, g.SRR_TENX)

    moved = bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir, threads=8, reads_per_fastq=1000)

    # The tool got the BAM + a scratch output dir, with the resource flags.
    cmd = next(c for c in fake.commands if c[0] == "bamtofastq")
    assert "--nthreads=8" in cmd
    assert "--reads-per-fastq=1000" in cmd
    assert str(bam) in cmd

    # Output is flattened into the SRX dir, SRR-prefixed, R1/R2/I1 + lane preserved,
    # and globs like the download layout (<SRX>/<SRR>*.fastq.gz).
    names = sorted(p.name for p in moved)
    assert names == sorted(p.name for p in srx_dir.glob(f"{g.SRR_TENX}*.fastq.gz"))
    assert f"{g.SRR_TENX}_S1_L001_R1_001.fastq.gz" in names
    assert f"{g.SRR_TENX}_S1_L002_R2_001.fastq.gz" in names
    assert f"{g.SRR_TENX}_S1_L001_I1_001.fastq.gz" in names
    # The scratch dir is reclaimed.
    assert not (srx_dir / f".{g.SRR_TENX}.b2f").exists()


def test_bamtofastq_run_removes_bam_when_requested(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    bam = _seed_bam(srx_dir, g.SRR_TENX)

    bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir, remove_bam=True)

    assert not bam.exists()  # reclaimed after a successful conversion
    assert list(srx_dir.glob(f"{g.SRR_TENX}_S1_L001_R1_001.fastq.gz"))


def test_bamtofastq_run_keeps_bam_by_default(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    bam = _seed_bam(srx_dir, g.SRR_TENX)
    bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir)
    assert bam.exists()


def test_bamtofastq_run_keeps_bam_on_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # remove_bam must not delete the BAM if conversion fails (no FASTQ produced).
    monkeypatch.setattr(bamtofastq, "_run", lambda cmd, *, cwd=None: None)
    srx_dir = tmp_path / "SRX5921017"
    bam = _seed_bam(srx_dir, g.SRR_TENX)
    with pytest.raises(DownloadError):
        bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir, remove_bam=True)
    assert bam.exists()  # left in place for a resumed rerun


def test_bamtofastq_run_missing_bam_raises(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    (srx_dir / g.SRR_TENX).mkdir(parents=True)  # run dir exists but has no .bam
    with pytest.raises(DownloadError, match="no cellranger BAM"):
        bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir)


def test_bamtofastq_run_multiple_bams_raises(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    _seed_bam(srx_dir, g.SRR_TENX, "a.bam")
    _seed_bam(srx_dir, g.SRR_TENX, "b.bam")
    with pytest.raises(DownloadError, match="multiple BAMs"):
        bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir)


def test_bamtofastq_run_empty_output_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A BAM lacking 10x/@RG headers: the tool "runs" but writes no FASTQ.
    monkeypatch.setattr(bamtofastq, "_run", lambda cmd, *, cwd=None: None)
    srx_dir = tmp_path / "SRX5921017"
    _seed_bam(srx_dir, g.SRR_TENX)
    with pytest.raises(DownloadError, match="produced no FASTQ"):
        bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir)


def test_bamtofastq_run_clears_stale_scratch(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    _seed_bam(srx_dir, g.SRR_TENX)
    stale = srx_dir / f".{g.SRR_TENX}.b2f"
    stale.mkdir()
    (stale / "leftover").write_bytes(b"junk")  # a partial run's remnants

    moved = bamtofastq.bamtofastq_run(Run(g.SRR_TENX), srx_dir)

    assert moved  # succeeds despite the pre-existing scratch dir
    assert not stale.exists()


def test_run_seam_raises_download_error_on_missing_tool() -> None:
    # The real seam: a missing binary is surfaced as DownloadError.
    with pytest.raises(DownloadError, match="not found"):
        bamtofastq._run(["definitely-not-a-real-binary-xyz"])


def test_flatten_output_folds_library_name_when_multiple(tmp_path: Path) -> None:
    # Two library subdirs with the same S/L/R names must stay distinct after flatten.
    scratch = tmp_path / f".{g.SRR_TENX}.b2f"
    for lib in ("libA", "libB"):
        (scratch / lib).mkdir(parents=True)
        (scratch / lib / "bamtofastq_S1_L001_R1_001.fastq.gz").write_bytes(b"gz")

    moved = bamtofastq._flatten_output(scratch, tmp_path, g.SRR_TENX)

    names = sorted(p.name for p in moved)
    assert names == [
        f"{g.SRR_TENX}_libA_S1_L001_R1_001.fastq.gz",
        f"{g.SRR_TENX}_libB_S1_L001_R1_001.fastq.gz",
    ]


def test_print_plan_notes_already_done(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    srx_dir = tmp_path / "SRX5921017"
    srx_dir.mkdir()
    (srx_dir / f".{g.SRR1}.tenx.done").touch()  # one run already converted
    tasks = [(Run(g.SRR1), srx_dir), (Run(g.SRR2), srx_dir)]

    bamtofastq._print_plan("GSE1", tasks, cores=4, output_root=tmp_path)

    err = capsys.readouterr().err
    assert "bamtofastq 2 runs, 1 experiment" in err
    assert "1 of 2 already done" in err


def test_print_plan_empty_is_quiet_line(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    bamtofastq._print_plan("GSE1", [], cores=4, output_root=tmp_path)
    assert "no 10x-BAM runs to convert" in capsys.readouterr().err


def test_convert_select_srx_filters_experiments(fake: FakeTools, tmp_path: Path) -> None:
    series = Series(g.GSE, client=g.build_client())
    # A whitelist that matches no experiment yields no tasks.
    results = bamtofastq.TenxConverter(series, tmp_path).convert(
        select_srx=["SRX9999999"], all_runs=True, verbose=False
    )
    assert results == {}


def test_converter_repr() -> None:
    converter = bamtofastq.TenxConverter(Series(g.GSE, client=g.build_client()), "./data")
    assert repr(converter) == f"TenxConverter({g.GSE!r}, 'data')"


def test_run_seam_raises_download_error_on_nonzero_exit() -> None:
    # The real seam: a tool that exits non-zero is surfaced as DownloadError.
    with pytest.raises(DownloadError, match="failed"):
        bamtofastq._run(["false"])


def test_converter_resolves_accession_kind_from_string() -> None:
    # A bare accession string is resolved to the matching (lazy) record — no network.
    from labdata import BioProject

    assert isinstance(bamtofastq.TenxConverter(g.GSE).record, Series)
    assert isinstance(bamtofastq.TenxConverter(g.PRJNA).record, BioProject)


# --------------------------------------------------------------------------- #
# the whole-project converter
# --------------------------------------------------------------------------- #


def test_convert_all_runs_produces_flattened_fastq(fake: FakeTools, tmp_path: Path) -> None:
    series = Series(g.GSE, client=g.build_client())
    converter = bamtofastq.TenxConverter(series, tmp_path)
    base = tmp_path / g.GSE / g.SRX
    _seed_bam(base, g.SRR1)
    _seed_bam(base, g.SRR2)

    results = converter.convert(all_runs=True, cores=4, verbose=False)

    assert results == {g.SRR1: True, g.SRR2: True}
    assert list(base.glob(f"{g.SRR1}_S1_L001_R1_001.fastq.gz"))
    assert list(base.glob(f"{g.SRR2}_S1_L001_R1_001.fastq.gz"))


def test_convert_no_tenx_runs_is_noop(fake: FakeTools, fake_sdl: None, tmp_path: Path) -> None:
    # The synthetic Series' runs carry FASTQ originals, not BAMs — auto-detect finds none.
    series = Series(g.GSE, client=g.build_client())
    results = bamtofastq.TenxConverter(series, tmp_path).convert(verbose=False)
    assert results == {}


def test_convert_rerun_short_circuits(fake: FakeTools, tmp_path: Path) -> None:
    series = Series(g.GSE, client=g.build_client())
    converter = bamtofastq.TenxConverter(series, tmp_path)
    base = tmp_path / g.GSE / g.SRX
    _seed_bam(base, g.SRR1)
    _seed_bam(base, g.SRR2)
    converter.convert(all_runs=True, verbose=False)

    # A second run needs no bamtofastq work — the success flag alone answers.
    fake.commands.clear()
    results = bamtofastq.TenxConverter(Series(g.GSE, client=g.build_client()), tmp_path).convert(
        all_runs=True, verbose=False
    )
    assert results == {g.SRR1: True, g.SRR2: True}
    assert fake.commands == []
