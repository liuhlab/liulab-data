"""Exception hierarchy for liulab-data.

All package-specific errors derive from :class:`LabdataError`, so callers can
catch the whole family with a single ``except``.
"""

from __future__ import annotations


class LabdataError(Exception):
    """Base class for all liulab-data errors."""


class AccessionError(LabdataError, ValueError):
    """An accession string is malformed or of the wrong type.

    Subclasses :class:`ValueError` so existing ``except ValueError`` callers keep working.
    """


class CredentialsError(LabdataError):
    """NCBI credentials are required but could not be resolved."""


class EntrezError(LabdataError, RuntimeError):
    """An Entrez request failed or returned an unusable response."""


class DownloadError(LabdataError, RuntimeError):
    """An external download tool is missing or a download/extract step failed."""
