"""Lightweight HTTP helpers for GEO's FTP-over-HTTP file directories.

E-utilities does not expose the individual supplementary-file names of a GEO
object, but NCBI serves the object's ``suppl/`` directory over HTTP as an
autoindex page. This module fetches and parses that listing. All network access
is isolated in :func:`fetch_text` so the rest of the package (and tests) can
monkeypatch a single seam.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from html.parser import HTMLParser

_USER_AGENT = "liulab-data (+https://github.com/lhqing/liulab-data)"


def fetch_text(url: str, *, timeout: float = 30.0) -> str:
    """Fetch ``url`` and return its decoded body.

    Parameters
    ----------
    url : str
        The URL to GET.
    timeout : float
        Socket timeout in seconds.

    Returns
    -------
    str
        The response body decoded using the response's charset (UTF-8 fallback).

    Raises
    ------
    urllib.error.URLError
        On network failure or a non-2xx HTTP status.
    """
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def list_directory(url: str) -> list[str]:
    """List the file names in an NCBI ``suppl/`` directory.

    Parameters
    ----------
    url : str
        The directory URL (e.g. a series ``.../suppl/`` page).

    Returns
    -------
    list of str
        The file names in the directory. Empty if the directory does not exist
        (HTTP 404), which is how a GEO object with no supplementary files reads.

    Raises
    ------
    urllib.error.URLError
        On any HTTP error other than 404, or on network failure.
    """
    try:
        html = fetch_text(url)
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return []
        raise
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
