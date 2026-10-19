"""The memory and threads a pipeline step, or a reader, may use.

Nothing here imports pyarrow, so the store's lookup path can cap DuckDB without it.
"""

from __future__ import annotations

import os
from pathlib import Path

#: The memory (MB) and threads a pipeline step was given; set by the pipeline for each step.
MEMORY_ENV = "LABDATA_MEMORY_MB"
THREADS_ENV = "LABDATA_THREADS"

#: The share of a step's memory DuckDB may use.
MEMORY_SHARE = 0.5
#: The share of a step's memory Ray's object store may use. The rest is for Python.
OBJECT_STORE_SHARE = 0.2


def default_workers() -> int:
    """Return ``SLURM_CPUS_ON_NODE`` if set, else the machine's CPU count."""
    return int(os.environ.get("SLURM_CPUS_ON_NODE") or os.cpu_count() or 1)


def step_memory_mb() -> int:
    """Return this step's memory in MB: ``LABDATA_MEMORY_MB``, else :func:`allocation_mb`."""
    given = os.environ.get(MEMORY_ENV, "")
    return int(given) if given.isdigit() and int(given) > 0 else allocation_mb()


def step_threads() -> int:
    """Return this step's threads: ``LABDATA_THREADS``, else :func:`default_workers`."""
    given = os.environ.get(THREADS_ENV, "")
    return int(given) if given.isdigit() and int(given) > 0 else default_workers()


def duckdb_memory() -> str:
    """Return DuckDB's memory cap: ``LABDATA_DUCKDB_MEMORY``, else a share of the step's memory."""
    override = os.environ.get("LABDATA_DUCKDB_MEMORY")
    if override:
        return override
    return f"{max(int(step_memory_mb() * MEMORY_SHARE), 256)}MB"


def allocation_mb() -> int:
    """Return the job's memory in MB: ``SLURM_MEM_PER_NODE`` in a Slurm job.

    Outside one it is the cgroup limit or the machine's memory, whichever is smaller.
    """
    slurm = os.environ.get("SLURM_MEM_PER_NODE", "")
    if slurm.isdigit() and int(slurm) > 0:
        return int(slurm)
    limits = [os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")]
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            text = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if text.isdigit():
            limits.append(int(text))
    return min(limits) >> 20
