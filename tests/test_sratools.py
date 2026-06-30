"""Tests for the sra-tools FASTQ downloader (labdata.geo.sratools).

The single subprocess seam (``sratools._run``) is monkeypatched with a fake that
records the commands issued and mimics each tool's filesystem effect — prefetch
drops a ``.sra``, fasterq-dump writes ``.fastq``, pigz/gzip compress them — so the
directory layout, gzip choice, cleanup, success flags, and parallelism are all
exercised without any real tools or network.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from labdata import BioProject, Experiment, Series, SraDownloader
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


def test_keep_sra_preserves_sra_next_to_fastq(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download(keep_sra=True)
    srx_dir = tmp_path / g.SRX
    for srr in (g.SRR1, g.SRR2):
        # The .sra is moved up beside the gzipped FASTQ instead of being deleted.
        assert (srx_dir / f"{srr}.sra").exists()
        assert (srx_dir / f"{srr}_1.fastq.gz").exists()
        # The prefetch download directory is still cleaned up.
        assert not (srx_dir / srr).exists()


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


def test_success_flag_records_each_step_as_json(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download()
    flag = tmp_path / g.SRX / f".{g.SRR1}.success"
    state = json.loads(flag.read_text())
    # Every pipeline step is recorded, plus the per-file gzip list.
    assert state["prefetch"] is True
    assert state["fasterq-dump"] is True
    assert state["cleanup"] is True
    assert sorted(state["gzip"]) == [f"{g.SRR1}_1.fastq", f"{g.SRR1}_2.fastq"]


def test_recorded_prefetch_is_skipped_on_rerun(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    srx_dir.mkdir(parents=True)
    # A flag that records only prefetch (e.g. an earlier run died during extraction).
    (srx_dir / f".{g.SRR1}.success").write_text(json.dumps({"prefetch": True}))

    SraDownloader(_experiment(), tmp_path).download()

    # SRR1 is not prefetched again, but its later steps still run.
    prefetched = [cmd[1] for cmd in fake.commands if cmd[0] == "prefetch"]
    assert g.SRR1 not in prefetched
    extracted = [Path(cmd[1]).name for cmd in fake.commands if cmd[0] == "fasterq-dump"]
    assert g.SRR1 in extracted
    assert (srx_dir / f"{g.SRR1}_1.fastq.gz").exists()


def test_recorded_gzip_files_are_not_recompressed(fake: FakeTools, tmp_path: Path) -> None:
    srx_dir = tmp_path / g.SRX
    srx_dir.mkdir(parents=True)
    # prefetch + fasterq-dump done, one mate already gzipped; only the other remains.
    (srx_dir / f".{g.SRR1}.success").write_text(
        json.dumps({"prefetch": True, "fasterq-dump": True, "gzip": [f"{g.SRR1}_1.fastq"]})
    )
    (srx_dir / f"{g.SRR1}_1.fastq.gz").write_bytes(b"gz")
    (srx_dir / f"{g.SRR1}_2.fastq").write_text("@r\nACGT\n+\nIIII\n")

    SraDownloader(_experiment(), tmp_path).download()

    # Only the remaining mate is handed to the gzip tool for SRR1.
    gzipped = [arg for cmd in fake.commands if cmd[0] == "pigz" for arg in cmd[1:]]
    assert any(arg.endswith(f"{g.SRR1}_2.fastq") for arg in gzipped)
    assert not any(arg.endswith(f"{g.SRR1}_1.fastq") for arg in gzipped)


def test_prefetch_only_stops_after_prefetch(fake: FakeTools, tmp_path: Path) -> None:
    result = SraDownloader(_experiment(), tmp_path).download(prefetch_only=True)

    assert result == {g.SRR1: True, g.SRR2: True}
    # Only prefetch ran — no extraction, compression, or cleanup.
    assert fake.tools_used() == {"prefetch"}
    srx_dir = tmp_path / g.SRX
    for srr in (g.SRR1, g.SRR2):
        # The downloaded .sra is left in place; no FASTQ is produced.
        assert (srx_dir / srr / f"{srr}.sra").exists()
        assert list(srx_dir.glob(f"{srr}*.fastq.gz")) == []
        # The flag records only the prefetch step (the run is not yet complete).
        state = json.loads((srx_dir / f".{srr}.success").read_text())
        assert state["prefetch"] is True
        assert "fasterq-dump" not in state


def test_prefetch_only_then_full_download_resumes(fake: FakeTools, tmp_path: Path) -> None:
    SraDownloader(_experiment(), tmp_path).download(prefetch_only=True)
    fake.commands.clear()

    result = SraDownloader(_experiment(), tmp_path).download()

    assert result == {g.SRR1: True, g.SRR2: True}
    # The full rerun extracts and gzips without re-fetching the already-prefetched .sra.
    assert "prefetch" not in fake.tools_used()
    assert "fasterq-dump" in fake.tools_used()
    assert (tmp_path / g.SRX / f"{g.SRR1}_1.fastq.gz").exists()


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


def test_series_download_prefetch_only(fake: FakeTools, tmp_path: Path) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, prefetch_only=True)
    assert result == {g.SRR1: True, g.SRR2: True}
    # Series threads prefetch_only down to the runs: only .sra is fetched, no FASTQ.
    assert fake.tools_used() == {"prefetch"}
    srx_dir = tmp_path / g.GSE / g.SRX
    assert (srx_dir / g.SRR1 / f"{g.SRR1}.sra").exists()
    assert list(srx_dir.glob("*.fastq.gz")) == []


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


# --------------------------------------------------------------------------- #
# user-facing progress output (printed to stderr)
# --------------------------------------------------------------------------- #


def test_download_prints_plan_and_per_run_lines(
    fake: FakeTools, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Series(g.GSE, client=g.build_client()).download(tmp_path)
    err = capsys.readouterr().err
    # The plan announces the destination, run/experiment counts, and lists the SRX.
    assert g.GSE in err
    assert "2 runs, 1 experiment" in err
    assert g.SRX in err
    # One "done" line per freshly downloaded run, plus the final tally.
    assert f"✓ {g.SRR1}" in err
    assert f"✓ {g.SRR2}" in err
    assert "Done: 2 ok, 0 failed." in err


def test_download_reports_skipped_runs(
    fake: FakeTools, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    srx_dir = tmp_path / g.GSE / g.SRX
    srx_dir.mkdir(parents=True)
    (srx_dir / f".{g.SRR1}.success").touch()

    Series(g.GSE, client=g.build_client()).download(tmp_path)
    err = capsys.readouterr().err
    assert "1 of 2 already done" in err
    assert f"• {g.SRR1}" in err
    assert f"✓ {g.SRR2}" in err


def test_download_reports_failed_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def boom(cmd: list[str], *, cwd: Path | None = None) -> None:
        raise DownloadError("tool exploded")

    monkeypatch.setattr(sratools, "_run", boom)
    monkeypatch.setattr(sratools, "_have", lambda tool: True)

    Series(g.GSE, client=g.build_client()).download(tmp_path)
    err = capsys.readouterr().err
    assert f"✗ {g.SRR1}" in err
    assert "Done: 0 ok, 2 failed." in err


def test_verbose_false_is_silent(
    fake: FakeTools, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    result = Series(g.GSE, client=g.build_client()).download(tmp_path, verbose=False)
    assert result == {g.SRR1: True, g.SRR2: True}
    assert capsys.readouterr().err == ""
