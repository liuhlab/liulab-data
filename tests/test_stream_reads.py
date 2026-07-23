"""Tests for the bounded read-preview primitive (:func:`stream_run_reads`).

The subprocess seam :func:`labdata.geo.sratools._run_capture` is faked so the whole
preview runs without sra-tools or network: the fake returns canned interleaved,
read-number-tagged FASTQ (what ``fastq-dump --split-spot --readids`` emits) and records
the command, cwd, and env it was handed. The parsing, per-read-index bucketing, N-bound,
``--skip-technical`` toggle, retry, and no-file-left guarantees are all exercised here.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from labdata import Run, stream_run_reads
from labdata.exceptions import AccessionError, DownloadError
from labdata.geo import sratools

SRR = "SRR9000001"


# --------------------------------------------------------------------------- #
# canned interleaved FASTQ (what ``fastq-dump --split-spot --readids`` streams)
# --------------------------------------------------------------------------- #


def _record(acc: str, spot: int, read_index: int, length: int, base: bytes = b"A") -> bytes:
    """Build one ``--readids``-tagged FASTQ record (``@<acc>.<spot>.<read>``)."""
    tag = f"@{acc}.{spot}.{read_index} {spot} length={length}".encode()
    plus = f"+{acc}.{spot}.{read_index}".encode()
    return b"\n".join((tag, base * length, plus, b"I" * length)) + b"\n"


def _interleaved(acc: str, spots: list[list[tuple[int, int]]]) -> bytes:
    """Concatenate spots' reads in emission order; each read is ``(read_index, length)``."""
    out = bytearray()
    for spot_no, reads in enumerate(spots, start=1):
        for read_index, length in reads:
            out += _record(acc, spot_no, read_index, length)
    return bytes(out)


class FakeCapture:
    """A stand-in for ``_run_capture`` that returns canned bytes and records calls."""

    def __init__(self, stdout: bytes = b"", *, fail_times: int = 0) -> None:
        self.stdout = stdout
        self.fail_times = fail_times
        self.attempts = 0
        self.calls: list[tuple[list[str], Path | None, dict[str, str] | None]] = []

    def __call__(
        self,
        cmd: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> bytes:
        self.attempts += 1
        self.calls.append((list(cmd), cwd, env))
        if self.attempts <= self.fail_times:
            raise DownloadError("boom")
        return self.stdout


@pytest.fixture
def fake_capture(monkeypatch: pytest.MonkeyPatch) -> FakeCapture:
    """Fake the subprocess seam and silence the retry backoff sleep."""
    fake = FakeCapture()
    monkeypatch.setattr(sratools, "_run_capture", fake)
    monkeypatch.setattr(sratools.time, "sleep", lambda _seconds: None)
    return fake


# --------------------------------------------------------------------------- #
# bucketing, geometry, spot counts
# --------------------------------------------------------------------------- #


def test_reads_bucket_by_within_spot_index(fake_capture: FakeCapture) -> None:
    fake_capture.stdout = _interleaved(SRR, [[(1, 28), (2, 94)]] * 3)

    preview = stream_run_reads(SRR, n_spots=3)

    assert preview.accession == SRR
    assert preview.read_indexes() == [1, 2]
    assert len(preview.reads[1]) == 3
    assert len(preview.reads[2]) == 3
    assert {len(r.seq) for r in preview.reads[1]} == {28}
    assert {len(r.seq) for r in preview.reads[2]} == {94}
    assert preview.read_lengths == {1: 28, 2: 94}
    assert preview.n_spots_requested == 3
    assert preview.n_spots_returned == 3
    assert preview.include_technical is True


def test_variable_reads_per_spot_stay_aligned_by_tag(fake_capture: FakeCapture) -> None:
    # The middle spot is missing its read 2 — positional (modulo) bucketing would
    # desync every following mate. Tag-based bucketing keeps each index pure.
    fake_capture.stdout = _interleaved(
        SRR,
        [
            [(1, 50), (2, 16), (3, 49)],
            [(1, 50), (3, 49)],
            [(1, 50), (2, 16), (3, 49)],
        ],
    )

    preview = stream_run_reads(SRR, n_spots=3)

    assert preview.read_indexes() == [1, 2, 3]
    assert [len(r.seq) for r in preview.reads[1]] == [50, 50, 50]
    assert [len(r.seq) for r in preview.reads[2]] == [16, 16]  # only the two full spots
    assert [len(r.seq) for r in preview.reads[3]] == [49, 49, 49]
    assert preview.read_lengths == {1: 50, 2: 16, 3: 49}
    # Every spot emits read 1, so the spot count is read 1's occurrence count.
    assert preview.n_spots_returned == 3


def test_single_read_spot_falls_back_to_index_one(fake_capture: FakeCapture) -> None:
    # A header with no trailing ``.N`` (a single-read spot) buckets under index 1.
    fake_capture.stdout = (
        b"\n".join((b"@SRR9000001.1 1 length=75", b"A" * 75, b"+", b"I" * 75)) + b"\n"
    )

    preview = stream_run_reads(SRR, n_spots=1)

    assert preview.read_indexes() == [1]
    assert preview.read_lengths == {1: 75}
    assert preview.n_spots_returned == 1


def test_empty_stream_returns_an_empty_preview(fake_capture: FakeCapture) -> None:
    fake_capture.stdout = b""

    preview = stream_run_reads(SRR, n_spots=100)

    assert preview.reads == {}
    assert preview.read_indexes() == []
    assert preview.read_lengths == {}
    assert preview.n_spots_returned == 0
    assert preview.n_spots_requested == 100


def test_truncated_trailing_record_is_dropped(fake_capture: FakeCapture) -> None:
    # A well-formed record followed by a header with no seq/plus/qual.
    fake_capture.stdout = _record(SRR, 1, 1, 30) + b"@SRR9000001.2.1 truncated\n"

    preview = stream_run_reads(SRR, n_spots=2)

    assert len(preview.reads[1]) == 1
    assert preview.n_spots_returned == 1


# --------------------------------------------------------------------------- #
# command shape, N-bound, technical toggle, isolation
# --------------------------------------------------------------------------- #


def test_command_is_bounded_streaming_fastq_dump(fake_capture: FakeCapture) -> None:
    stream_run_reads(SRR, n_spots=5)

    cmd, cwd, env = fake_capture.calls[0]
    assert cmd == [
        "fastq-dump",
        "--stdout",
        "--maxSpotId",
        "5",
        "--split-spot",
        "--readids",
        SRR,
    ]
    assert "--skip-technical" not in cmd  # technical reads kept by default
    assert "prefetch" not in cmd[0]  # never prefetch — that would fetch the whole run
    # Everything the tool writes is redirected into the temp cwd and reclaimed.
    assert cwd is not None
    assert env is not None
    assert env["HOME"] == str(cwd)
    assert env["TMPDIR"] == str(cwd)


def test_skip_technical_toggle_adds_flag_before_accession(fake_capture: FakeCapture) -> None:
    stream_run_reads(SRR, n_spots=2, include_technical=False)

    cmd = fake_capture.calls[0][0]
    assert "--skip-technical" in cmd
    assert cmd.index("--skip-technical") < cmd.index(SRR)


def test_no_temp_dir_or_cache_survives_the_call(fake_capture: FakeCapture) -> None:
    fake_capture.stdout = _interleaved(SRR, [[(1, 28), (2, 94)]])

    stream_run_reads(SRR, n_spots=1)

    cwd = fake_capture.calls[0][1]
    assert cwd is not None
    assert not cwd.exists()  # the TemporaryDirectory was reclaimed on exit


# --------------------------------------------------------------------------- #
# accession handling and errors
# --------------------------------------------------------------------------- #


def test_accepts_a_run_object_or_a_string(fake_capture: FakeCapture) -> None:
    fake_capture.stdout = _interleaved(SRR, [[(1, 28)]])

    from_str = stream_run_reads(SRR, n_spots=1)
    from_run = stream_run_reads(Run(SRR), n_spots=1)

    assert from_str.accession == from_run.accession == SRR


def test_a_malformed_accession_raises_before_any_tool_call(fake_capture: FakeCapture) -> None:
    with pytest.raises(AccessionError):
        stream_run_reads("not-an-accession")
    assert fake_capture.attempts == 0  # rejected before shelling out


def test_download_error_propagates_after_exhausting_retries(fake_capture: FakeCapture) -> None:
    fake_capture.fail_times = 3

    with pytest.raises(DownloadError):
        stream_run_reads(SRR, retries=3)
    assert fake_capture.attempts == 3


def test_a_transient_failure_is_retried_then_succeeds(fake_capture: FakeCapture) -> None:
    fake_capture.fail_times = 1
    fake_capture.stdout = _interleaved(SRR, [[(1, 28)]])

    preview = stream_run_reads(SRR, retries=3)

    assert fake_capture.attempts == 2
    assert preview.n_spots_returned == 1


# --------------------------------------------------------------------------- #
# record round-trip helpers
# --------------------------------------------------------------------------- #


def test_to_fastq_bytes_reconstructs_canonical_records(fake_capture: FakeCapture) -> None:
    fake_capture.stdout = _interleaved(SRR, [[(1, 28), (2, 94)]] * 2)

    preview = stream_run_reads(SRR, n_spots=2)

    fastq = preview.to_fastq_bytes(1)
    lines = fastq.splitlines()
    assert len(lines) == 8  # two 4-line records
    assert lines[0].startswith(b"@SRR9000001.1.1")
    assert lines[1] == b"A" * 28
    assert fastq.endswith(b"\n")


def test_content_hash_is_a_stable_sha256_of_the_slice(fake_capture: FakeCapture) -> None:
    fake_capture.stdout = _interleaved(SRR, [[(1, 28), (2, 94)]] * 2)

    preview = stream_run_reads(SRR, n_spots=2)

    expected = hashlib.sha256(preview.to_fastq_bytes(2)).hexdigest()
    assert preview.content_hash(2) == expected
    assert len(preview.content_hash(2)) == 64
    assert preview.content_hash(1) != preview.content_hash(2)  # distinct mates
