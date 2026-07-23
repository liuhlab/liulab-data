"""liulab-data: data curation, download, and organization utilities for the Liu Lab."""

from importlib.metadata import PackageNotFoundError, version

from labdata.geo import (
    BioProject,
    Experiment,
    FastqRecord,
    Platform,
    Run,
    RunReadPreview,
    Sample,
    Series,
    SraDownloader,
    experiments_for,
    iter_run_reads,
    stream_run_reads,
)
from labdata.ncbi import FileLocation, RemoteFile, SdlClient
from labdata.tenx import TenxConverter

try:
    __version__ = version("liulab-data")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "BioProject",
    "Experiment",
    "FastqRecord",
    "FileLocation",
    "Platform",
    "RemoteFile",
    "Run",
    "RunReadPreview",
    "Sample",
    "SdlClient",
    "Series",
    "SraDownloader",
    "TenxConverter",
    "__version__",
    "experiments_for",
    "iter_run_reads",
    "stream_run_reads",
]
