"""Every path of an ontology store root, with its kind, as the other two stores declare theirs.

Other ontology modules take their file names from here.
"""

from __future__ import annotations

from labdata import _paths
from labdata._paths import SCRATCH, STATE, Declared, Kind

#: The marker: the store's format and where it is copied.
MARKER = Declared("labdata-ontologies.toml", STATE)
README = Declared("README.md", STATE)
#: The status record of the last update.
STATUS = Declared("ontology-status.json", STATE)
#: The copy record: the files GCS holds, as the last successful copy left them.
COPY_RECORD = Declared(".copied-files.json", STATE)
#: The next copy record, saved here until GCS holds it.
PENDING_RECORD = Declared(".copied-files.json.pending", SCRATCH)
#: Each vocabulary's current snapshot, written whole; a switch rewrites it.
CURRENT = Declared("current-snapshots.json", STATE)
#: One folder per kept snapshot, ``<vocabulary>-<release>/``.
SNAPSHOTS = Declared("snapshots/", STATE)
#: The run lock and the work in progress.
WORK = Declared("work/", SCRATCH)
#: Held by a running update, so two updates never write the store at once.
LOCK = Declared("work/update.lock", SCRATCH)
#: Source files being downloaded.
DOWNLOADS = Declared("work/downloads/", SCRATCH)
#: Snapshots being written, renamed into ``snapshots/`` once whole; and remade tables files.
BUILDING = Declared("work/building/", SCRATCH)
#: Snapshots being deleted, moved here whole first.
PRUNING = Declared("work/pruning/", SCRATCH)

#: Every declared path.
LAYOUT = (
    MARKER,
    README,
    STATUS,
    COPY_RECORD,
    PENDING_RECORD,
    CURRENT,
    SNAPSHOTS,
    WORK,
    LOCK,
    DOWNLOADS,
    BUILDING,
    PRUNING,
)

#: In a snapshot folder: its term tables, one SQLite file.
TABLES_FILE = "terms.sqlite"
#: In a snapshot folder: the source files, as downloaded.
SOURCE_DIR = "source"


def kind(rel: str) -> Kind:
    """Return the kind of a path from the ontology store root.

    A hidden name nothing declares is a temp file a write left: scratch.

    Raises
    ------
    ValueError
        If no declared path holds it.
    """
    return _paths.kind(rel, LAYOUT)
