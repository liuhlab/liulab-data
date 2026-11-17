"""The source record store: a shared, offline copy of NCBI metadata records.

:mod:`labdata.store.paths` declares every path under the root, with its kind.
"""

from labdata.store.model import Partial
from labdata.store.root import Store, init_store, open_store, pointer_path, save_pointer

__all__ = ["Partial", "Store", "init_store", "open_store", "pointer_path", "save_pointer"]
