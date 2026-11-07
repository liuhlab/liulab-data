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
    """NCBI or OpenAlex credentials are required but could not be resolved."""


class EntrezError(LabdataError, RuntimeError):
    """An Entrez request failed or returned an unusable response."""


class PageNotFound(EntrezError):  # noqa: N818 - pairs with NotFound
    """An NCBI web page answered 404: the page does not exist."""


class SdlError(LabdataError, RuntimeError):
    """An SRA Data Locator (SDL) request failed or returned an unusable response."""


class DownloadError(LabdataError, RuntimeError):
    """An external download tool is missing or a download/extract step failed."""


class StoreNotConfiguredError(LabdataError):
    """No store root is set by argument, environment variable or pointer file."""


class NotAStoreError(LabdataError):
    """A folder is not a store: no ``labdata-store.toml`` or ``labdata-papers.toml`` marker."""


class NotMirrored(LabdataError):  # noqa: N818 - the spec names it
    """A valid accession the store does not hold. The message lists every missing accession."""


class NotFound(LabdataError):  # noqa: N818 - the spec names it
    """A valid accession no snapshot lists as public. The message gives the snapshot date."""


class StoreBuildError(LabdataError):
    """A store fetch or build cannot finish: an input is missing or sources disagree."""


class SnapshotError(LabdataError):
    """A snapshot could not be fetched whole, so none was written."""
