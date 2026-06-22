"""liulab-data: data curation, download, and organization utilities for the Liu Lab."""

from importlib.metadata import PackageNotFoundError, version

from labdata.geo import BioProject, Experiment, Platform, Run, Sample, Series, SraDownloader
from labdata.ncbi import FileLocation, RemoteFile, SdlClient

try:
    __version__ = version("liulab-data")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "BioProject",
    "Experiment",
    "FileLocation",
    "Platform",
    "RemoteFile",
    "Run",
    "Sample",
    "SdlClient",
    "Series",
    "SraDownloader",
    "__version__",
]
