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
from labdata import Series
from labdata.exceptions import DownloadError
from labdata.geo import sratools
from tests import _geodata as g
from tests.test_sratools import FakeTools

runner = CliRunner()


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
