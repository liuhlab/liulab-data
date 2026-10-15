"""A ``README.md`` and a ``manifest.json`` in each store folder, and a check that they match.

The folders are ``snapshots/<source>/<date>/``, ``raw/<source>/`` and ``builds/<build-id>/``.
The README is NCBI FTP style: what the files are, their source, date, size, sha256, layout,
the producing command and its commit, and how to refresh. The manifest holds the same facts
for code. Every write is a temporary file renamed into place.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import raw, snapshots
from labdata.store._files import write_atomic

MANIFEST = "manifest.json"
README = "README.md"

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CHUNK = 1 << 20
_FTP = "https://ftp.ncbi.nlm.nih.gov"
_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_DOWNLOADER = "scripts/bulk/download_ncbi_bulk.py"

#: What each raw folder holds and where its text comes from.
RAW_SOURCES: dict[str, tuple[str, str]] = {
    raw.GEO_SOFT: (
        "The family SOFT of each GEO Series, decompressed, one row per Series.",
        f"{_FTP}/geo/series/<GSEnnn>/<GSE>/soft/<GSE>_family.soft.gz",
    ),
    raw.ENTREZ_LINKS: (
        "Entrez links of each GEO Series, one row per Series: tab-separated lines of from "
        "accession, target database, target UID and target accession.",
        f"{_EUTILS}/elink.fcgi (gds to sra per Sample, gds to bioproject per Series) "
        "and esummary.fcgi for the target accessions",
    ),
    raw.GEO_FILELIST: (
        "The suppl/filelist.txt of each GEO Series, one row per Series: supplementary file "
        "names, sizes, dates and archive members. An empty text means the Series has none.",
        f"{_FTP}/geo/series/<GSEnnn>/<GSE>/suppl/filelist.txt",
    ),
    raw.SRA_XML: (
        "The SRA EXPERIMENT_PACKAGE XML of each experiment, one row per experiment.",
        f"{_EUTILS}/efetch.fcgi?db=sra&rettype=full&retmode=xml",
    ),
    raw.PUBMED_SI: (
        "The PubMed IDs that list each accession in their accession field, one row per "
        "accession queried, one PMID per line. An empty text means none.",
        f"{_EUTILS}/esearch.fcgi?db=pubmed&term=<accession>[si]",
    ),
}

#: Where each snapshot source comes from.
SNAPSHOT_SOURCES: dict[str, str] = {
    snapshots.SRA_ACCESSIONS: f"{_FTP}/sra/reports/Metadata/SRA_Accessions.tab",
    snapshots.BIOPROJECT: f"{_FTP}/bioproject/bioproject.xml",
    snapshots.BIOSAMPLE: f"{_FTP}/biosample/biosample_set.xml.gz",
    snapshots.GEO_SERIES: f"{_EUTILS}/esummary.fcgi?db=gds for every gse[ETYP] UID",
}


def sha256_file(path: Path) -> str:
    """Return the hex sha256 of a file, read as a stream."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def produced_by(command: str) -> dict[str, str | None]:
    """Return the producing command, the git commit of this code and the labdata version."""
    from labdata import __version__

    return {"command": command, "commit": _commit(), "version": __version__}


def file_entries(folder: Path) -> list[dict[str, Any]]:
    """List a folder's data files, recursively, with bytes and sha256.

    Leaves out the README, the manifest and hidden files, which are a writer's temporaries.
    """
    return [
        {
            "name": name,
            "bytes": (folder / name).stat().st_size,
            "sha256": sha256_file(folder / name),
        }
        for name in _data_files(folder)
    ]


def document_raw(root: Path, source: str) -> Path:
    """Write ``raw/<source>/``'s manifest and README from the bucket files now there.

    Returns
    -------
    pathlib.Path
        The folder.
    """
    folder = root / "raw" / source
    files = file_entries(folder)
    dates: set[str] = set()
    for entry in files:
        column = pq.read_table(folder / entry["name"], columns=["fetch_date"]).column(0)
        entry["rows"] = len(column)
        dates |= set(column.to_pylist())
    what, url = RAW_SOURCES.get(source, (f"Live results of the {source} source.", "NCBI"))
    _write(
        folder,
        {
            "title": f"raw/{source}",
            "what": what,
            "source": url,
            "date": _span(dates),
            "layout": (
                "One Parquet file per bucket, rows sorted by accession, columns accession, "
                "raw_text, hash (sha256 of raw_text) and fetch_date. GEO accessions use GEO's "
                "thousand folder (GSE246nnn); others a range of 10,000 numbers (SRX2428nnnn)."
            ),
            "files": files,
            "produced_by": produced_by("labdata store fetch"),
            "refresh": (
                "Run `labdata store fetch <accession> ...`. Saved items are kept; only "
                "missing ones are fetched. A build refreshes this README and manifest."
            ),
        },
    )
    return folder


def document_build(
    root: Path, folder: Path, build_id: str, *, built: str | None = None
) -> dict[str, Any]:
    """Write a build folder's manifest and README, naming the inputs it was built from.

    Returns
    -------
    dict
        The manifest.
    """
    files = file_entries(folder)
    for entry in files:
        entry["rows"] = pq.read_metadata(folder / entry["name"]).num_rows
    manifest = {
        "title": f"builds/{build_id}",
        "what": "The store's tables, built from raw/ and snapshots/ with no network.",
        "source": "raw/ and snapshots/ of this store; see inputs",
        "date": built or dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build": build_id,
        "layout": (
            "records/<type>.parquet, one per record type, sorted by accession; "
            "links_by_from.parquet and links_by_to.parquet, the same links sorted by each end; "
            "holdings.parquet, every accession held; known.parquet, every accession the "
            "snapshots list with the snapshot date; supplementary_files.parquet, the supplementary "
            "files of each held record, sorted by owner accession."
        ),
        "files": files,
        "inputs": _inputs(root),
        "produced_by": produced_by("labdata store build"),
        "refresh": (
            "Run `labdata store build`. It writes a new builds/<build-id>/ and then switches "
            "labdata-store.toml to it; readers keep the build they opened."
        ),
    }
    _write(folder, manifest)
    return manifest


def document_snapshot(folder: Path) -> list[Path]:
    """Write a snapshot folder's manifest and README, each only where absent.

    A manifest already there, such as the downloader's, is kept as it is and the README is
    written from it.

    Returns
    -------
    list of pathlib.Path
        The files written.
    """
    written = []
    source, date = folder.parent.name, folder.name
    if not (folder / MANIFEST).is_file():
        write_atomic(
            folder / MANIFEST,
            _json(
                {
                    "title": f"snapshots/{source}/{date}",
                    "source": SNAPSHOT_SOURCES.get(source, "NCBI"),
                    "date": date,
                    "files": file_entries(folder),
                    "produced_by": produced_by("labdata store verify --write-missing"),
                    "note": f"Documents files already present; {_DOWNLOADER} fetches them.",
                }
            ),
        )
        written.append(folder / MANIFEST)
    if not (folder / README).is_file():
        manifest = json.loads((folder / MANIFEST).read_text(encoding="utf-8"))
        made = manifest.get("produced_by") or {
            "command": (manifest.get("fetched_by") or {}).get("script", _DOWNLOADER),
            "commit": (manifest.get("fetched_by") or {}).get("commit"),
        }
        write_atomic(
            folder / README,
            _render(
                {
                    "title": f"snapshots/{source}/{date}",
                    "what": f"A bulk {source} snapshot, kept whole as NCBI serves it.",
                    "source": manifest.get("source")
                    or manifest.get("url")
                    or SNAPSHOT_SOURCES.get(source, "NCBI"),
                    "date": manifest.get("date") or manifest.get("last_modified") or date,
                    "layout": "The files as downloaded; the folder name is the snapshot date.",
                    "files": manifest.get("files", []),
                    "produced_by": made,
                    "refresh": (
                        f"Run `python -I {_DOWNLOADER} <snapshots-root> --date <YYYY-MM-DD> "
                        f"{source}` into a new dated folder; never overwrite this one."
                    ),
                }
            ),
        )
        written.append(folder / README)
    return written


def folders(root: Path) -> list[Path]:
    """Return every folder that carries a README and manifest, in a stable order."""
    snaps = sorted(
        p for p in (root / "snapshots").glob("*/*") if p.is_dir() and _DATE.match(p.name)
    )
    raws = sorted(p for p in (root / "raw").glob("*") if p.is_dir())
    builds = sorted(
        p for p in (root / "builds").glob("*") if p.is_dir() and not p.name.startswith(".")
    )
    return [*snaps, *raws, *builds]


def verify(root: Path, *, write_missing: bool = False) -> list[str]:
    """Check each folder's manifest against its files, sha256 included.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    write_missing : bool
        First write each snapshot or raw folder's README and manifest where absent.

    Returns
    -------
    list of str
        One line per problem; empty when every folder matches.
    """
    problems = []
    for folder in folders(root):
        rel = folder.relative_to(root)
        if write_missing:
            if folder.parent.parent.name == "snapshots":
                document_snapshot(folder)
            elif folder.parent.name == "raw" and not (folder / MANIFEST).is_file():
                document_raw(root, folder.name)
        missing = [name for name in (MANIFEST, README) if not (folder / name).is_file()]
        if missing:
            problems.append(f"{rel}: no {' and no '.join(missing)}")
            if MANIFEST in missing:
                continue
        manifest = json.loads((folder / MANIFEST).read_text(encoding="utf-8"))
        listed = set()
        for entry in manifest.get("files", []):
            listed.add(entry["name"])
            path = folder / entry["name"]
            if not path.is_file():
                problems.append(f"{rel}/{entry['name']}: listed but missing")
            elif path.stat().st_size != entry.get("bytes", path.stat().st_size):
                problems.append(f"{rel}/{entry['name']}: size differs from the manifest")
            elif "sha256" in entry and sha256_file(path) != entry["sha256"]:
                problems.append(f"{rel}/{entry['name']}: sha256 differs from the manifest")
        if "produced_by" in manifest:
            problems += [
                f"{rel}/{name}: not in the manifest"
                for name in _data_files(folder)
                if name not in listed
            ]
    return problems


def _inputs(root: Path) -> dict[str, Any]:
    raws = {}
    for folder in sorted(p for p in (root / "raw").glob("*") if p.is_dir()):
        manifest = json.loads((folder / MANIFEST).read_text(encoding="utf-8"))
        raws[folder.name] = [{"name": f["name"], "sha256": f["sha256"]} for f in manifest["files"]]
    snaps = {}
    for source in sorted(SNAPSHOT_SOURCES):
        try:
            snap = snapshots.snapshot(root, source)
        except StoreBuildError:
            continue
        recorded = snap.path.parent / MANIFEST
        sha = None
        if recorded.is_file():
            entries = json.loads(recorded.read_text(encoding="utf-8")).get("files", [])
            sha = next((f.get("sha256") for f in entries if f["name"] == snap.path.name), None)
        snaps[source] = {"date": snap.date, "name": snap.path.name, "sha256": sha}
    return {"raw": raws, "snapshots": snaps}


def _data_files(folder: Path) -> list[str]:
    return sorted(
        str(p.relative_to(folder))
        for p in folder.rglob("*")
        if p.is_file()
        and p.name not in (MANIFEST, README)
        and not any(part.startswith(".") for part in p.relative_to(folder).parts)
    )


def _span(dates: set[str]) -> str:
    if not dates:
        return "no rows"
    first, last = min(dates), max(dates)
    return first if first == last else f"fetched {first} to {last}"


def _write(folder: Path, manifest: dict[str, Any]) -> None:
    write_atomic(folder / MANIFEST, _json(manifest))
    write_atomic(folder / README, _render(manifest))


def _json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2) + "\n"


def _render(m: dict[str, Any]) -> str:
    made = m.get("produced_by") or {}
    rows = "\n".join(
        f"| `{f['name']}` | {f.get('bytes', '?')} | `{f.get('sha256', '?')}` |"
        for f in m.get("files", [])
    )
    lines = [
        f"# {m['title']}",
        "",
        m.get("what", ""),
        "",
        "## Source",
        "",
        str(m.get("source", "")),
        "",
        "## Date",
        "",
        str(m.get("date", "")),
        "",
        "## Files",
        "",
        "| File | Bytes | sha256 |",
        "| --- | --- | --- |",
        rows,
        "",
        "## Layout",
        "",
        m.get("layout", ""),
        "",
        "## How these files were made",
        "",
        f"`{made.get('command')}` at commit `{made.get('commit') or 'unknown'}`"
        + (f", labdata {made['version']}." if made.get("version") else "."),
        "",
        "## How to refresh",
        "",
        m.get("refresh", ""),
        "",
    ]
    return "\n".join(lines)


def _commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        out = ""
    if out:
        return out
    from labdata import __version__

    match = re.search(r"\+g([0-9a-f]+)", __version__)
    return match[1] if match else None
