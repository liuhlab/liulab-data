"""Every top-level path of a paper store root, with its kind, as the GEO/SRA store declares its.

Other paper modules take their file names from here.
"""

from __future__ import annotations

from labdata import _paths
from labdata._paths import SCRATCH, STATE, Declared, Kind

#: The marker: the GEO/SRA store it reads, and where it is copied.
MARKER = Declared("labdata-papers.toml", STATE)
README = Declared("README.md", STATE)
#: The status record of the paper job's passes.
STATUS = Declared("paper-status.json", STATE)
#: The copy record: the ``files`` rows GCS holds, as the last successful copy left them.
COPY_RECORD = Declared(".copied-files.parquet", STATE)
#: The next copy record, saved here until GCS holds it.
PENDING_RECORD = Declared(".copied-files.parquet.pending", SCRATCH)
#: The four tables, ``<name>.parquet``; :mod:`labdata.papers.tables` declares their columns.
TABLES = tuple(
    Declared(f"{name}.parquet", STATE) for name in ("papers", "paper_info", "files", "attempts")
)
#: The manual queue: PubMed MEDLINE records for Paperpile, and one DOI a line.
MANUAL_NBIB = Declared("manual-queue.nbib", STATE)
MANUAL_DOIS = Declared("manual-queue-dois.txt", STATE)
#: One folder per paper.
PAPERS = Declared("papers/", STATE)
#: Held by a running pass, so two passes never overlap.
LOCK = Declared(".lock", SCRATCH)
#: Downloads the running daily pass has not yet written to the tables.
JOURNAL = Declared(".daily-journal.jsonl", SCRATCH)
#: The paper job's chain: a job, then the jobs queued behind it.
SUCCESSOR = Declared(".successor", SCRATCH)
#: Files being written, and the PMC inventory while a sync reads it.
TEMP = Declared(".tmp/", SCRATCH)
#: Paper file moves and deletes not yet applied.
PENDING = Declared(".pending-files.json", SCRATCH)

#: Every declared path.
LAYOUT = (
    MARKER,
    README,
    STATUS,
    COPY_RECORD,
    PENDING_RECORD,
    *TABLES,
    MANUAL_NBIB,
    MANUAL_DOIS,
    PAPERS,
    LOCK,
    JOURNAL,
    SUCCESSOR,
    TEMP,
    PENDING,
)


def kind(rel: str) -> Kind:
    """Return the kind of a path from the paper store root.

    A hidden name nothing declares is a temp file a write left: scratch.

    Raises
    ------
    ValueError
        If no declared path holds it.
    """
    return _paths.kind(rel, LAYOUT)
