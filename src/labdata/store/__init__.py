"""The source record store: a shared, offline copy of NCBI metadata records."""

from labdata.store.model import Partial
from labdata.store.root import Store, init_store, open_store, pointer_path, save_pointer

__all__ = ["Partial", "Store", "init_store", "open_store", "pointer_path", "save_pointer"]
