"""Tests for the sra-tools FASTQ downloader (labdata.geo.sratools).

The single subprocess seam (``sratools._run``) is monkeypatched with a fake that
records the commands issued and mimics each tool's filesystem effect — prefetch
drops a ``.sra``, fasterq-dump writes ``.fastq``, pigz/gzip compress them — so the
directory layout, gzip choice, cleanup, success flags, and parallelism are all
exercised without any real tools or network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from labdata import Experiment, Series, SraDownloader
from labdata.exceptions import DownloadError
from labdata.geo import sratools
from tests import _geodata as g


class FakeTools:
    """A stand-in for ``sratools._run`` that records and simulates the tools."""

    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def run(self, cmd: list[str], *, cwd: Path | None = None) -> None:
        self.commands.append(cmd)
        tool = cmd[0]
        if tool == "prefetch":
            accession = cmd[1]
            out = Path(cmd[cmd.index("-O") + 1])
            sra_dir = out / accession
            sra_dir.mkdir(parents=True, exist_ok=True)
            (sra_dir / f"{accession}.sra").write_bytes(b"sra")
        elif tool == "fasterq-dump":
            out = Path(cmd[cmd.index("-O") + 1])
            accession = Path(cmd[1]).name.replace(".sra", "")
            for mate in (1, 2):
                (out / f"{accession}_{mate}.fastq").write_text("@r\nACGT\n+\nIIII\n")
        elif tool in ("pigz", "gzip"):
            for arg in cmd[1:]:
                if arg.endswith(".fastq"):
                    path = Path(arg)
                    path.with_suffix(".fastq.gz").write_bytes(b"gz")
                    path.unlink()

    def tools_used(self) -> set[str]:
        """Return the set of executables that were invoked."""
        return {cmd[0] for cmd in self.commands}

    def order_of(self, *tools: str) -> list[str]:
        """Return the invoked executables, filtered to ``tools``, in call order."""
        return [cmd[0] for cmd in self.commands if cmd[0] in tools]


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeTools:
    """Replace the subprocess seam with a recording fake and default pigz to present."""
    tools = FakeTools()
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)
    return tools


def _experiment() -> Experiment:
    return Experiment(g.SRX, client=g.build_client())


def test_download_creates_gzipped_fastq_per_run(fake: FakeTools, tmp_path: Path) -> None:
    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    srx_dir = tmp_path / g.SRX
    assert srx_dir.is_dir()
    for srr in (g.SRR1, g.SRR2):
        assert (srx_dir / f"{srr}_1.fastq.gz").exists()
        assert (srx_dir / f"{srr}_2.fastq.gz").exists()


def test_download_cleans_up_intermediates_and_marks_success(
    fake: FakeTools, tmp_path: Path
) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    srx_dir = tmp_path / g.SRX

    # Only gzipped FASTQ survive: no .sra, no prefetch dir, no bare .fastq.
    assert list(srx_dir.glob("*.sra")) == []
    assert list(srx_dir.glob("*.fastq")) == []
    assert not (srx_dir / g.SRR1).exists()
    # Each finished run leaves a hidden success flag.
    for srr in (g.SRR1, g.SRR2):
        assert (srx_dir / f".{srr}.success").exists()


def test_existing_success_flag_skips_the_run(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    srx_dir.mkdir(parents=True)
    (srx_dir / f".{g.SRR1}.success").touch()

    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    # The skipped run is never prefetched; the other one still is.
    prefetched = [cmd[1] for cmd in fake.commands if cmd[0] == "prefetch"]
    assert g.SRR1 not in prefetched
    assert g.SRR2 in prefetched


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


def test_parallel_downloads_all_runs(fake: FakeTools, tmp_path: Path) -> None:
    result = SraDownloader(_experiment(), tmp_path).download(n_parallel=2)
    assert result == {g.SRR1: True, g.SRR2: True}


def test_failed_run_records_false_without_aborting(
    fake: FakeTools, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("tool exploded")

    monkeypatch.setattr(sratools, "_run", boom)
    result = SraDownloader(_experiment(), tmp_path).download()
    assert result == {g.SRR1: False, g.SRR2: False}


def test_series_download_nests_under_series_then_experiment(
    fake: FakeTools, tmp_path: Path
) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path)
    assert result == {g.SRR1: True, g.SRR2: True}
    # Layout: <output_dir>/<GSE>/<SRX>/<SRR>*.fastq.gz
    srx_dir = tmp_path / g.GSE / g.SRX
    assert srx_dir.is_dir()
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()


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
    """A fake whose ``prefetch`` fails its first ``fail_times`` calls, then succeeds.

    ``fasterq-dump`` (and everything else) always succeeds, so a test can tell
    network retries apart from local steps.
    """

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

    # One run only, so the attempt count is unambiguous.
    experiment = Experiment(g.SRX, client=g.build_client())
    result = sratools._download_run(experiment.runs[0], tmp_path, retries=3)

    assert result is True
    # Two failures + one success for the single run.
    assert tools.prefetch_attempts == 3


def test_prefetch_gives_up_after_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tools = FlakyTools(fail_times=99)  # never recovers
    monkeypatch.setattr(sratools, "_run", tools.run)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)

    experiment = Experiment(g.SRX, client=g.build_client())
    with pytest.raises(DownloadError):
        sratools._download_run(experiment.runs[0], tmp_path, retries=3)
    # Exactly the configured number of attempts, no more.
    assert tools.prefetch_attempts == 3


def test_fasterq_dump_is_not_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)

    experiment = Experiment(g.SRX, client=g.build_client())
    with pytest.raises(DownloadError):
        sratools._download_run(experiment.runs[0], tmp_path, retries=3)
    # The local extraction step is run once — retries apply only to prefetch.
    assert attempts["fasterq-dump"] == 1


def test_run_seam_raises_download_error_on_missing_tool() -> None:
    # The real _run translates a missing executable into a domain error.
    with pytest.raises(DownloadError, match="not found"):
        sratools._run(["labdata-no-such-tool-xyz"])
