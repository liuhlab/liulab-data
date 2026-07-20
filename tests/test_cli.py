"""Tests for the Typer CLI (labdata.cli).

The ``geo download`` command is exercised end to end with three seams faked: the
``Series`` symbol the command imports is swapped for one that carries a
:class:`FakeEntrezClient` (no NCBI), ``sratools._run`` is swapped for the recording
:class:`FakeTools` (no sra-tools), and ``pipeline._run_snakemake`` is swapped for
:func:`fake_snakemake` (no Snakemake). The hidden per-stage subcommands the DAG shells
back into are covered directly with just the tools faked.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import labdata.cli as cli_mod
from labdata import BioProject, Series, experiments_for
from labdata.exceptions import DownloadError
from labdata.geo import sratools
from labdata.tenx import bamtofastq
from tests import _geodata as g
from tests._download_fakes import FakeTools, fake_snakemake, fake_tenx_snakemake

runner = CliRunner()


@pytest.fixture
def fake_experiments(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve experiments through the synthetic GEO/SRA graph instead of NCBI."""

    def resolver(accession: str) -> list:
        return experiments_for(accession, client=g.build_client())

    monkeypatch.setattr(cli_mod, "experiments_for", resolver)


@pytest.fixture
def fake_tools(monkeypatch: pytest.MonkeyPatch) -> FakeTools:
    """Fake both download seams: the sra-tools subprocess and Snakemake."""
    tools = FakeTools()
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(sratools.pipeline, "_run_snakemake", fake_snakemake)
    return tools


@pytest.fixture
def fake_series(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the command build a Series wired to the synthetic GEO/SRA graph."""

    def factory(accession: str) -> Series:
        return Series(accession, client=g.build_client())

    monkeypatch.setattr(cli_mod, "Series", factory)


@pytest.fixture
def fake_bioproject(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the command build a BioProject wired to the synthetic GEO/SRA graph."""

    def factory(accession: str) -> BioProject:
        return BioProject(accession, client=g.build_client())

    monkeypatch.setattr(cli_mod, "BioProject", factory)


def test_geo_download_runs_whole_series(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    result = runner.invoke(cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "-j", "2"])

    assert result.exit_code == 0
    srx_dir = tmp_path / g.GSE / g.SRX
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()
    assert (srx_dir / f"{g.SRR2}_1.fastq.gz").exists()
    # The plan and the per-run tally reach the user.
    assert g.GSE in result.output
    assert "Done: 2 ok, 0 failed." in result.output


def test_geo_download_auto_detects_bioproject(
    fake_tools: FakeTools, fake_bioproject: None, tmp_path: Path
) -> None:
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.PRJNA, "-o", str(tmp_path), "-j", "2"]
    )

    assert result.exit_code == 0
    # Same layout as a Series download, but the top dir is named for the project.
    srx_dir = tmp_path / g.PRJNA / g.SRX
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()
    assert (srx_dir / f"{g.SRR2}_1.fastq.gz").exists()
    assert g.PRJNA in result.output
    assert "Done: 2 ok, 0 failed." in result.output


def test_geo_download_select_srx_restricts_to_listed_experiment(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--select-srx", g.SRX]
    )
    assert result.exit_code == 0
    assert (tmp_path / g.GSE / g.SRX / f"{g.SRR1}_1.fastq.gz").exists()
    assert "Done: 2 ok, 0 failed." in result.output


def test_geo_download_select_srx_unknown_downloads_nothing(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    # A well-formed SRX not in the Series filters everything out — no runs, exit 0.
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--select-srx", "SRX0000000"]
    )
    assert result.exit_code == 0
    assert "no SRA runs to download" in result.output
    assert fake_tools.commands == []


def test_geo_download_select_srx_reads_whitelist_file(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    # A single value that is an existing file is read as a one-accession-per-line list.
    whitelist = tmp_path / "srx.txt"
    whitelist.write_text(f"# experiments to fetch\n{g.SRX}\n\n")

    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--select-srx", str(whitelist)]
    )
    assert result.exit_code == 0
    assert (tmp_path / g.GSE / g.SRX / f"{g.SRR1}_1.fastq.gz").exists()


def test_geo_download_select_srx_rejects_bad_accession(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    # A literal value that is neither a file nor an SRX accession is an error.
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--select-srx", "GSM123"]
    )
    assert result.exit_code == 1
    assert "error:" in result.output


def test_geo_download_original_srx_fetches_original_format(
    fake_tools: FakeTools, fake_series: None, fake_sdl: None, tmp_path: Path
) -> None:
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--original-srx", g.SRX]
    )
    assert result.exit_code == 0
    srx_dir = tmp_path / g.GSE / g.SRX
    # Original submitter files land under <SRX>/<SRR>/; no sra-tools FASTQ.
    assert (srx_dir / g.SRR1 / f"{g.SRR1}_original_R1.fastq.gz").exists()
    assert (srx_dir / f".{g.SRR1}.original.done").exists()
    assert fake_tools.tools_used() == {"curl"}
    assert "[original format]" in result.output


def test_geo_download_original_srx_reads_whitelist_file(
    fake_tools: FakeTools, fake_series: None, fake_sdl: None, tmp_path: Path
) -> None:
    whitelist = tmp_path / "original.txt"
    whitelist.write_text(f"# fetch these in original format\n{g.SRX}\n")

    result = runner.invoke(
        cli_mod.app,
        ["geo", "download", g.GSE, "-o", str(tmp_path), "--original-srx", str(whitelist)],
    )
    assert result.exit_code == 0
    assert (tmp_path / g.GSE / g.SRX / g.SRR1 / f"{g.SRR1}_original_R1.fastq.gz").exists()


def test_geo_download_quiet_suppresses_progress(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    result = runner.invoke(cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--quiet"])
    assert result.exit_code == 0
    assert "Done:" not in result.output


def test_geo_download_passes_max_size_override(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--max-size", "500G"]
    )
    prefetch = next(cmd for cmd in fake_tools.commands if cmd[0] == "prefetch")
    assert prefetch[prefetch.index("--max-size") + 1] == "500G"


def test_geo_download_keep_sra_keeps_sra_file(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--keep-sra"]
    )
    assert result.exit_code == 0
    srx_dir = tmp_path / g.GSE / g.SRX
    assert (srx_dir / f"{g.SRR1}.sra").exists()


def test_geo_download_bad_accession_exits_nonzero(tmp_path: Path) -> None:
    result = runner.invoke(
        cli_mod.app, ["geo", "download", "not-an-accession", "-o", str(tmp_path)]
    )
    assert result.exit_code == 1
    assert "error:" in result.output


def test_geo_experiments_prints_the_experiment_accessions(fake_experiments: None) -> None:
    result = runner.invoke(cli_mod.app, ["geo", "experiments", g.GSE])
    assert result.exit_code == 0
    assert result.output.split() == [g.SRX]


def test_geo_experiments_bad_accession_exits_nonzero(fake_experiments: None) -> None:
    result = runner.invoke(cli_mod.app, ["geo", "experiments", "not-an-accession"])
    assert result.exit_code == 1
    assert "error:" in result.output


def test_geo_download_prefetch_only_flag(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    result = runner.invoke(
        cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path), "--prefetch-only"]
    )
    assert result.exit_code == 0
    # Only prefetch ran; the .sra is left in place and no FASTQ is produced.
    assert fake_tools.tools_used() == {"prefetch"}
    srx_dir = tmp_path / g.GSE / g.SRX
    assert (srx_dir / g.SRR1 / f"{g.SRR1}.sra").exists()
    assert list(srx_dir.glob("*.fastq.gz")) == []


def test_geo_download_threads_per_run_reaches_fasterq_dump(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path
) -> None:
    runner.invoke(
        cli_mod.app,
        ["geo", "download", g.GSE, "-o", str(tmp_path), "--threads-per-run", "2", "--cores", "4"],
    )
    fasterq = next(cmd for cmd in fake_tools.commands if cmd[0] == "fasterq-dump")
    assert fasterq[fasterq.index("--threads") + 1] == "2"


def test_geo_download_exits_nonzero_when_a_run_fails(
    fake_tools: FakeTools, fake_series: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("tool exploded")

    monkeypatch.setattr(sratools, "_run", boom)

    result = runner.invoke(cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path)])
    assert result.exit_code == 1
    assert "run(s) failed" in result.output


# --------------------------------------------------------------------------- #
# hidden per-stage subcommands (the bridge the Snakemake DAG shells back into)
# --------------------------------------------------------------------------- #


def test_geo_prefetch_stage_fetches_sra(fake_tools: FakeTools, tmp_path: Path) -> None:
    result = runner.invoke(cli_mod.app, ["geo", "_prefetch", g.SRR1, str(tmp_path)])
    assert result.exit_code == 0
    assert (tmp_path / g.SRR1 / f"{g.SRR1}.sra").exists()


def test_geo_stage_commands_run_prefetch_extract_compress(
    fake_tools: FakeTools, tmp_path: Path
) -> None:
    for stage in ("_prefetch", "_extract", "_compress"):
        result = runner.invoke(cli_mod.app, ["geo", stage, g.SRR1, str(tmp_path)])
        assert result.exit_code == 0
    # The chain produces the gzipped FASTQ in the experiment dir (intermediate cleanup
    # is Snakemake's job via temp(), not the bare subcommands').
    assert (tmp_path / f"{g.SRR1}_1.fastq.gz").exists()


def test_geo_original_stage_downloads_submitter_files(
    fake_tools: FakeTools, fake_sdl: None, tmp_path: Path
) -> None:
    result = runner.invoke(cli_mod.app, ["geo", "_original", g.SRR1, str(tmp_path)])
    assert result.exit_code == 0
    # The download-only stage lands the submitter files under <srx_dir>/<SRR>/.
    assert (tmp_path / g.SRR1 / f"{g.SRR1}_original_R1.fastq.gz").exists()
    assert fake_tools.tools_used() == {"curl"}


def test_geo_stage_command_reports_failure_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("prefetch exploded")

    monkeypatch.setattr(sratools, "_run", boom)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)

    result = runner.invoke(cli_mod.app, ["geo", "_prefetch", g.SRR1, str(tmp_path)])
    assert result.exit_code == 1
    assert "error:" in result.output


# --------------------------------------------------------------------------- #
# tenx: cellranger BAM → FASTQ
# --------------------------------------------------------------------------- #


@pytest.fixture
def fake_tenx_tools(monkeypatch: pytest.MonkeyPatch) -> FakeTools:
    """Fake both tenx seams: the bamtofastq subprocess and Snakemake."""
    tools = FakeTools()
    monkeypatch.setattr(bamtofastq, "_run", tools.run)
    monkeypatch.setattr(bamtofastq.pipeline, "_run_snakemake", fake_tenx_snakemake)
    return tools


@pytest.fixture
def fake_converter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the command build a TenxConverter over a Series wired to the synthetic graph."""

    def factory(accession: str, output_dir: Path) -> bamtofastq.TenxConverter:
        return bamtofastq.TenxConverter(Series(accession, client=g.build_client()), output_dir)

    monkeypatch.setattr(cli_mod, "TenxConverter", factory)


def _seed_bam(base: Path, accession: str) -> None:
    run_dir = base / accession
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "possorted_genome_bam.bam").write_bytes(b"bam")


def test_tenx_bamtofastq_converts_all_runs(
    fake_tenx_tools: FakeTools, fake_converter: None, tmp_path: Path
) -> None:
    base = tmp_path / g.GSE / g.SRX
    _seed_bam(base, g.SRR1)
    _seed_bam(base, g.SRR2)

    result = runner.invoke(
        cli_mod.app, ["tenx", "bamtofastq", g.GSE, "-o", str(tmp_path), "--all-runs"]
    )

    assert result.exit_code == 0
    # Flattened, SRR-prefixed, R1/R2/I1 preserved — globs like the download layout.
    assert (base / f"{g.SRR1}_S1_L001_R1_001.fastq.gz").exists()
    assert (base / f"{g.SRR2}_S1_L002_I1_001.fastq.gz").exists()
    assert "Done: 2 ok, 0 failed." in result.output


def test_tenx_bamtofastq_missing_bam_exits_nonzero(
    fake_tenx_tools: FakeTools, fake_converter: None, tmp_path: Path
) -> None:
    # No BAM seeded on disk → the stage fails for every run.
    result = runner.invoke(
        cli_mod.app, ["tenx", "bamtofastq", g.GSE, "-o", str(tmp_path), "--all-runs"]
    )
    assert result.exit_code == 1
    assert "run(s) failed" in result.output


def test_tenx_bamtofastq_stage_converts_one_run(fake_tenx_tools: FakeTools, tmp_path: Path) -> None:
    _seed_bam(tmp_path, g.SRR_TENX)  # tmp_path plays the SRX dir here
    result = runner.invoke(cli_mod.app, ["tenx", "_bamtofastq", g.SRR_TENX, str(tmp_path)])
    assert result.exit_code == 0
    assert (tmp_path / f"{g.SRR_TENX}_S1_L001_R1_001.fastq.gz").exists()


def test_tenx_bamtofastq_stage_reports_failure_nonzero(tmp_path: Path) -> None:
    # No BAM on disk → the stage exits non-zero with an actionable message.
    result = runner.invoke(cli_mod.app, ["tenx", "_bamtofastq", g.SRR_TENX, str(tmp_path)])
    assert result.exit_code == 1
    assert "error:" in result.output
