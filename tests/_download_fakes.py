"""Fakes for the two FASTQ-download seams: sra-tools and Snakemake.

:class:`FakeTools` stands in for ``sratools._run`` — it records the commands issued
and mimics each tool's filesystem effect (prefetch drops a ``.sra``, fasterq-dump
writes ``.fastq`` into its ``-O`` dir, pigz/gzip compress them).

:func:`fake_snakemake` stands in for ``pipeline._run_snakemake``: it reads the config
the driver wrote and drives the same three stages in-process, faithfully modelling
Snakemake's output-based scheduling — a run whose final target already exists is
skipped whole, and each stage runs only if its own output is missing — plus
``temp()`` reclamation (the ``.sra`` dir is removed once extract consumes it, the
FASTQ staging dir once compress consumes it). A run whose stage raises is left
incomplete and the return code goes non-zero, mirroring ``snakemake --keep-going``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from labdata import Run
from labdata.exceptions import DownloadError
from labdata.geo import sratools


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
            out.mkdir(parents=True, exist_ok=True)
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


def fake_snakemake(argv: list[str], *, verbose: bool) -> int:
    """Drive the download stages in-process, modelling Snakemake scheduling + temp().

    A drop-in for ``pipeline._run_snakemake``: for each configured run it skips the
    whole run if the final target exists, otherwise runs each missing stage and
    reclaims the ``temp()`` intermediates as their consumers complete. Returns ``1``
    if any run's stage failed.
    """
    config = json.loads(Path(argv[argv.index("--configfile") + 1]).read_text())
    output_root = Path(argv[argv.index("--directory") + 1])
    prefetch_only = config["prefetch_only"]
    returncode = 0
    for srr, srx in config["runs"].items():
        srx_dir = output_root / srx
        srx_dir.mkdir(parents=True, exist_ok=True)
        sra_dir = srx_dir / srr
        staging = srx_dir / f".{srr}.fq"
        done = srx_dir / f".{srr}.done"
        run = Run(srr)

        final = sra_dir if prefetch_only else done
        if final.exists():
            continue  # Snakemake: final output present -> run nothing

        try:
            if not sra_dir.exists():
                sratools.prefetch_run(
                    run,
                    srx_dir,
                    max_size=config["max_size"],
                    retries=config["retries"],
                    backoff=config["backoff"],
                )
            if prefetch_only:
                continue
            if not staging.exists():
                sratools.extract_run(
                    run,
                    srx_dir,
                    threads=config["threads_per_run"],
                    staging=staging,
                    keep_sra=config["keep_sra"],
                )
                shutil.rmtree(sra_dir, ignore_errors=True)  # temp(): reclaim the .sra
            sratools.compress_run(run, srx_dir, threads=config["threads_per_run"], staging=staging)
            shutil.rmtree(staging, ignore_errors=True)  # temp(): reclaim the FASTQ staging
            done.touch()
        except DownloadError:
            returncode = 1
    return returncode
