"""liulab-data: data curation, download, and organization utilities for the Liu Lab."""

from importlib.metadata import PackageNotFoundError, version

from labdata.geo import Experiment, Platform, Run, Sample, Series

try:
    __version__ = version("liulab-data")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = ["Experiment", "Platform", "Run", "Sample", "Series", "__version__"]
