"""NCBI E-utilities access: credential resolution and a thin Entrez client."""

from labdata.ncbi.config import (
    NcbiCredentials,
    load_credentials,
    prompt_and_save,
    save_credentials,
)
from labdata.ncbi.entrez import EntrezClient
from labdata.ncbi.sdl import FileLocation, RemoteFile, SdlClient

__all__ = [
    "EntrezClient",
    "FileLocation",
    "NcbiCredentials",
    "RemoteFile",
    "SdlClient",
    "load_credentials",
    "prompt_and_save",
    "save_credentials",
]
