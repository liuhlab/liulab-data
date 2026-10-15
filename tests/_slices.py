"""The thin-slice fixtures as stores: each fixture's fake Entrez client, fetch and build.

Three fixture folders under ``tests/data/`` hold the 11 thin-slice records:
``prjna1027859`` (record 1), ``geo_slice`` (records 2-4 and 7-11) and ``sra_slice``
(records 5 and 6).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import FakeEntrezClient

DATA = Path(__file__).parent / "data"
TODAY = "2026-10-14"

PRJNA1027859 = "PRJNA1027859"
GEO_SERIES = [
    "GSE246310",
    "GSE153830",
    "GSE19456",
    "GSE233931",
    "GSE233685",
    "GSE20301",
    "GSE28770",
    "GSE15556",
]
SRA_PROJECTS = ["PRJNA436361", "PRJNA530347"]


def _soft_url(gse: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/soft/{gse}_family.soft.gz"


def _filelist_url(gse: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/suppl/filelist.txt"


def prjna1027859_client(**drops: int) -> FakeEntrezClient:
    """Serve record 1: its six packages and its ``[si]`` answer."""
    srx = [f"SRX2428313{i}" for i in range(6)]
    return FakeEntrezClient(
        esearch={f"{PRJNA1027859}[si]": ["39637862"]},
        packages={a: (DATA / "sra_package" / f"{a}.xml").read_text() for a in srx},
        drops=drops,
    )


def geo_client() -> FakeEntrezClient:
    """Serve the GEO records: trimmed SOFT, ``filelist.txt``, packages and Entrez answers."""
    folder = DATA / "geo_slice"
    entrez = json.loads((folder / "entrez.json").read_text())
    elink: dict[tuple[str, str, str], list[str]] = {}
    for key, uids in entrez["elink"].items():
        dbfrom, db, uid = key.split()
        elink[dbfrom, db, uid] = uids
    esummary: dict[tuple[str, str], dict[str, Any]] = {}
    for uid, srx in entrez["accessions"]["sra"].items():
        esummary["sra", uid] = {"Id": uid, "ExpXml": f'<Experiment acc="{srx}" ver="1"/>'}
    for uid, prj in entrez["accessions"]["bioproject"].items():
        esummary["bioproject", uid] = {"Id": uid, "Project_Acc": prj}
    pages = {_soft_url(p.stem): p.read_text(encoding="utf-8") for p in (folder / "soft").iterdir()}
    pages |= {_filelist_url(p.stem): p.read_text() for p in (folder / "filelist").iterdir()}
    return FakeEntrezClient(
        pages=pages,
        packages={p.stem: p.read_text(encoding="utf-8") for p in (folder / "packages").iterdir()},
        elink=elink,
        esummary=esummary,
        esearch={f"{acc}[si]": pmids for acc, pmids in entrez["pubmed_si"].items()},
    )


def sra_slice_client() -> FakeEntrezClient:
    """Serve records 5 and 6: their packages and ``[si]`` answers."""
    folder = DATA / "sra_slice"
    si = json.loads((folder / "pubmed_si.json").read_text())
    return FakeEntrezClient(
        esearch={f"{acc}[si]": pmids for acc, pmids in si.items()},
        packages={p.stem: p.read_text(encoding="utf-8") for p in (folder / "packages").iterdir()},
    )


#: Each fixture: its snapshot folder, the accessions fetched and its client.
SLICES = {
    "prjna1027859": (DATA / "prjna1027859" / "snapshots", [PRJNA1027859], prjna1027859_client),
    "geo_slice": (DATA / "geo_slice" / "snapshots", GEO_SERIES, geo_client),
    "sra_slice": (DATA / "sra_slice" / "snapshots", SRA_PROJECTS, sra_slice_client),
}


def new_root(path: Path, slice_name: str) -> Path:
    """Make a store root at ``path`` holding one fixture's snapshots and no build."""
    shutil.copytree(SLICES[slice_name][0], path / "snapshots")
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


def built_root(path: Path, slice_name: str) -> Path:
    """Make a store root at ``path``, fetch one fixture's records into it and build ``b1``."""
    _, accessions, client = SLICES[slice_name]
    new_root(path, slice_name)
    fetch(path, accessions, client=client(), today=TODAY)
    build(path, "b1")
    return path
