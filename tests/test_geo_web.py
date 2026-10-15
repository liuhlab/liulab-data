"""Tests for labdata.geo._web — directory-listing parse and 404 handling."""

import labdata.geo._web as web
from tests._fakes import FakeEntrezClient

# Trimmed NCBI/Apache autoindex: a parent link, six files, and an external link.
_LISTING = """
<html><body><table>
<tr><td><a href="/geo/series/GSE131nnn/GSE131907/">Parent Directory</a></td></tr>
<tr><td><a href="GSE131907_Feature_Summary.xlsx">GSE131907_Feature_Summary.xlsx</a></td></tr>
<tr><td><a href="GSE131907_cell_annotation.txt.gz">...</a></td></tr>
<tr><td><a href="?C=N;O=D">Name</a></td></tr>
<tr><td><a href="https://www.hhs.gov/vulnerability-disclosure-policy/index.html">HHS</a></td></tr>
</table></body></html>
"""


def test_parse_directory_listing_keeps_only_files() -> None:
    assert web.parse_directory_listing(_LISTING) == [
        "GSE131907_Feature_Summary.xlsx",
        "GSE131907_cell_annotation.txt.gz",
    ]


def test_parse_empty() -> None:
    assert web.parse_directory_listing("<html></html>") == []


def test_list_directory_fetches_and_parses() -> None:
    client = FakeEntrezClient(pages={"https://example/suppl/": _LISTING})
    assert web.list_directory("https://example/suppl/", client) == [
        "GSE131907_Feature_Summary.xlsx",
        "GSE131907_cell_annotation.txt.gz",
    ]


def test_list_directory_404_returns_empty() -> None:
    client = FakeEntrezClient(pages={"https://example/suppl/": None})
    assert web.list_directory("https://example/suppl/", client) == []
