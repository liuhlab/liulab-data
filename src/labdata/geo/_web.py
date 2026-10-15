"""Lightweight HTTP helpers for GEO's FTP-over-HTTP file directories.

E-utilities does not expose the individual supplementary-file names of a GEO
object, but NCBI serves the object's ``suppl/`` directory over HTTP as an
autoindex page. This module fetches that listing through
:meth:`~labdata.ncbi.entrez.EntrezClient.fetch_text` and parses it.
"""

from __future__ import annotations

from html.parser import HTMLParser

from labdata.exceptions import PageNotFound
from labdata.ncbi.entrez import EntrezClient


def list_directory(url: str, client: EntrezClient) -> list[str]:
    """List the file names in an NCBI ``suppl/`` directory.

    Parameters
    ----------
    url : str
        The directory URL (e.g. a series ``.../suppl/`` page).
    client : EntrezClient
        The client that fetches the page.

    Returns
    -------
    list of str
        The file names in the directory. Empty if the directory does not exist
        (HTTP 404), which is how a GEO object with no supplementary files reads.

    Raises
    ------
    EntrezError
        On any HTTP error other than 404, or on network failure.
    """
    try:
        html = client.fetch_text(url)
    except PageNotFound:
        return []
    return parse_directory_listing(html)


def parse_directory_listing(html: str) -> list[str]:
    """Extract file names from an Apache/NCBI autoindex HTML page.

    Keeps only relative anchor targets that name a file in this directory —
    parent links, absolute URLs, and column-sort query links are dropped.

    Parameters
    ----------
    html : str
        The directory listing HTML.

    Returns
    -------
    list of str
        File names, in document order.
    """
    parser = _LinkParser()
    parser.feed(html)
    names: list[str] = []
    for href in parser.hrefs:
        if not href or "/" in href or "?" in href or href.startswith("http"):
            continue
        names.append(href)
    return names


class _LinkParser(HTMLParser):
    """Collect the ``href`` of every anchor tag."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Record anchor ``href`` attributes as they are encountered."""
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)
