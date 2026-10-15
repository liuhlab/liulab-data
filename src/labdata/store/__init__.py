"""The source record store: a shared, offline copy of NCBI metadata records.

Layout under the root: the ``labdata-store.toml`` marker, ``snapshots/<source>/<date>/``,
``raw/<source>/`` and ``builds/<build-id>/``. ``work/`` holds intermediate files that a
build remakes when they are missing.
"""

from labdata.store.model import Partial
from labdata.store.root import Store, init_store, open_store, pointer_path, save_pointer

__all__ = ["Partial", "Store", "init_store", "open_store", "pointer_path", "save_pointer"]
