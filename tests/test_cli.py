"""Tests for the Typer CLI (labdata.cli).

The ``geo download`` command is exercised end to end with both seams faked: the
``Series`` symbol the command imports is swapped for one that carries a
:class:`FakeEntrezClient` (no NCBI), and ``sratools._run`` is swapped for the
recording :class:`FakeTools` (no sra-tools, no network).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import labdata.cli as cli_mod
from labdata import BioProject, Series, experiments_for
from labdata.exceptions import DownloadError
from labdata.geo import sratools
from tests import _geodata as g
from tests.test_sratools import FakeTools

runner = CliRunner()


@pytest.fixture
def fake_experiments(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve experiments through the synthetic GEO/SRA graph instead of NCBI."""

    def resolver(accession: str) -> list:
        return experiments_for(accession, client=g.build_client())

    monkeypatch.setattr(cli_mod, "experiments_for", resolver)


@pytest.fixture
def fake_tools(monkeypatch: pytest.MonkeyPatch) -> FakeTools:
    """Swap the subprocess seam for the recording fake used by the sratools tests."""
    tools = FakeTools()
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)
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


def test_geo_download_exits_nonzero_when_a_run_fails(
    fake_series: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("tool exploded")

    monkeypatch.setattr(sratools, "_run", boom)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)

    result = runner.invoke(cli_mod.app, ["geo", "download", g.GSE, "-o", str(tmp_path)])
    assert result.exit_code == 1
    assert "run(s) failed" in result.output
