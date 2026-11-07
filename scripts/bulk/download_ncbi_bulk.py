#!/usr/bin/env python3
"""Mirror the NCBI bulk metadata sources that are small enough to take whole.

    python -I scripts/bulk/download_ncbi_bulk.py DEST_ROOT [--date YYYY-MM-DD] [SOURCE ...]

Sources: ``sra-accessions``, ``bioproject``, ``biosample``, ``geo-series`` and
``sra-metadata-parquet``. With none named, all but ``sra-metadata-parquet`` run: the store does
not read the Parquet, so it is fetched only when named. Each lands in ``DEST_ROOT/<source>/<date>/``
next to a ``manifest.json`` holding the URL, the remote Last-Modified/ETag, size and sha256.

Rerunning with the same ``--date`` resumes a partial fetch and verifies a finished one; it
never mixes two remote releases into one file. When the remote file changed since the first
attempt, it stops and asks for a new ``--date``.

``geo-series`` patches the newest earlier snapshot beside it, or the one ``--base`` names: it
lists every Series and fetches only the summaries of new Series and Series GEO updated since
that snapshot. ``--full`` fetches every summary. It runs with liulab-data installed and reads
NCBI credentials as ``labdata ncbi configure`` saves them.

Needs ``curl`` and, for parallel transfers, ``aria2c``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import re
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

FTP = "https://ftp.ncbi.nlm.nih.gov"
FILES = {
    "sra-accessions": f"{FTP}/sra/reports/Metadata/SRA_Accessions.tab",
    "bioproject": f"{FTP}/bioproject/bioproject.xml",
    "biosample": f"{FTP}/biosample/biosample_set.xml.gz",
}
S3_BUCKET = "https://sra-pub-metadata-us-east-1.s3.amazonaws.com"
S3_PREFIX = "sra/metadata/"
DEFAULT_SOURCES = [*FILES, "geo-series"]
SOURCES = [*DEFAULT_SOURCES, "sra-metadata-parquet"]


class FetchError(RuntimeError):
    """A source could not be fetched or verified."""


def log(msg: str) -> None:
    """Print a timestamped progress line."""
    print(f"{dt.datetime.now(dt.UTC):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def sha256(path: Path) -> str:
    """Return the hex sha256 of a file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 24):
            h.update(chunk)
    return h.hexdigest()


def head(url: str) -> dict[str, str]:
    """Return the response headers of a HEAD request, lower-cased."""
    out = subprocess.run(
        ["curl", "-sfI", "--retry", "5", url], check=True, capture_output=True, text=True
    ).stdout
    return _parse_headers(out)


def _parse_headers(text: str) -> dict[str, str]:
    pairs = (line.split(":", 1) for line in text.splitlines() if ":" in line)
    return {k.strip().lower(): v.strip() for k, v in pairs}


def _read_sums(path: Path) -> dict[str, str]:
    """Map file name to sha256 from a ``sha256sum``-format file."""
    return {name: digest for digest, name in (ln.split() for ln in path.read_text().splitlines())}


def download(url: str, dest: Path, connections: int = 4) -> None:
    """Fetch ``url`` to ``dest``, resuming a partial file."""
    if shutil.which("aria2c"):
        cmd = [
            "aria2c", "-c", f"-x{connections}", f"-s{connections}", "--file-allocation=none",
            "--auto-file-renaming=false", "--summary-interval=300", "--console-log-level=warn",
            "-d", str(dest.parent), "-o", dest.name, url,
        ]  # fmt: skip
    else:
        cmd = ["curl", "-fL", "--retry", "10", "-C", "-", "-o", str(dest), url]
    subprocess.run(cmd, check=True)


def write_manifest(out: Path, record: dict[str, object]) -> None:
    """Write ``manifest.json`` with the script's commit appended."""
    record["fetched_by"] = {"script": "scripts/bulk/download_ncbi_bulk.py", "commit": _commit()}
    (out / "manifest.json").write_text(json.dumps(record, indent=2) + "\n")


def _commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def fetch_file(url: str, out: Path) -> None:
    """Fetch one whole file, refusing to resume across a remote release."""
    name = url.rsplit("/", 1)[1]
    dest = out / name
    before = out / f"{name}.headers.before"
    sha_file = out / f"{name}.sha256"
    if before.exists() and sha_file.exists():
        # A finished snapshot is checked against itself, not against today's remote file.
        now = _parse_headers(before.read_text())
        if dest.stat().st_size != int(now["content-length"]):
            raise FetchError(f"{dest}: size differs from the recorded Content-Length")
        log(f"{name}: present, verifying sha256")
        if sha256(dest) != sha_file.read_text().split()[0]:
            raise FetchError(f"{dest} does not match {sha_file}")
    else:
        now = head(url)
        if before.exists():
            first = _parse_headers(before.read_text())
            if first.get("last-modified") != now.get("last-modified"):
                raise FetchError(
                    f"{url} changed since this snapshot began "
                    f"({first.get('last-modified')} -> {now.get('last-modified')}); "
                    "use a new --date"
                )
        else:
            before.write_text("".join(f"{k}: {v}\n" for k, v in now.items()))
        size = int(now["content-length"])
        log(f"{name}: fetching {size} bytes")
        download(url, dest)
        if dest.stat().st_size != size:
            raise FetchError(f"{dest}: size {dest.stat().st_size} != Content-Length {size}")
        after = head(url)
        (out / f"{name}.headers.after").write_text("".join(f"{k}: {v}\n" for k, v in after.items()))
        if after.get("last-modified") != now.get("last-modified"):
            raise FetchError(f"{url} was replaced during the download; use a new --date")
        sha_file.write_text(f"{sha256(dest)}  {name}\n")
    write_manifest(
        out,
        {
            "url": url,
            "last_modified": now.get("last-modified"),
            "etag": now.get("etag"),
            "files": [
                {
                    "name": name,
                    "bytes": dest.stat().st_size,
                    "sha256": sha_file.read_text().split()[0],
                }
            ],
        },
    )


def fetch_parquet(out: Path, tmp: Path) -> None:
    """Fetch every object of the current SRA metadata Parquet snapshot on S3."""
    listing = out / "s3-listing.xml"
    if not listing.exists():
        url = f"{S3_BUCKET}/?list-type=2&prefix={urllib.parse.quote(S3_PREFIX)}"
        with urllib.request.urlopen(url, timeout=120) as r:
            listing.write_bytes(r.read())
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(listing.read_text())
    if root.findtext("s3:IsTruncated", namespaces=ns) != "false":
        raise FetchError("S3 listing is truncated; paging is not implemented")
    objects = [
        {
            "key": c.findtext("s3:Key", namespaces=ns) or "",
            "bytes": int(c.findtext("s3:Size", namespaces=ns) or 0),
            "last_modified": c.findtext("s3:LastModified", namespaces=ns),
            "etag": (c.findtext("s3:ETag", namespaces=ns) or "").strip('"'),
        }
        for c in root.findall("s3:Contents", ns)
    ]
    for o in objects:
        o["name"] = o["key"].rsplit("/", 1)[1] + ".parquet"
    todo = [
        o
        for o in objects
        if not (out / o["name"]).exists() or (out / o["name"]).stat().st_size != o["bytes"]
    ]
    if todo:
        if head(f"{S3_BUCKET}/{todo[0]['key']}").get("etag", "").strip('"') != todo[0]["etag"]:
            raise FetchError("the S3 snapshot was replaced since the listing; use a new --date")
        log(f"parquet: fetching {len(todo)} of {len(objects)} objects")
        tmp.mkdir(parents=True, exist_ok=True)
        inp = tmp / "aria2-input.txt"
        inp.write_text("".join(f"{S3_BUCKET}/{o['key']}\n  out={o['name']}\n" for o in todo))
        if shutil.which("aria2c"):
            subprocess.run(
                ["aria2c", "-c", "-j6", "-x2", "--file-allocation=none",
                 "--auto-file-renaming=false", "--summary-interval=300",
                 "--console-log-level=warn", "-d", str(out), "-i", str(inp)],
                check=True,
            )  # fmt: skip
        else:
            for o in todo:
                download(f"{S3_BUCKET}/{o['key']}", out / o["name"])
    bad = [o["name"] for o in objects if (out / o["name"]).stat().st_size != o["bytes"]]
    if bad:
        raise FetchError(f"size mismatch against the listing: {bad}")
    sums = out / "SHA256SUMS"
    if sums.exists():
        log("parquet: verifying SHA256SUMS")
        recorded = _read_sums(sums)
        for o in objects:
            if sha256(out / o["name"]) != recorded.get(o["name"]):
                raise FetchError(f"{o['name']} does not match SHA256SUMS")
    else:
        sums.write_text("".join(f"{sha256(out / o['name'])}  {o['name']}\n" for o in objects))
    recorded = _read_sums(sums)
    for o in objects:
        o["sha256"] = recorded[o["name"]]
    write_manifest(
        out,
        {
            "url": f"{S3_BUCKET}/{S3_PREFIX}",
            "last_modified": max(str(o["last_modified"]) for o in objects),
            "files": objects,
        },
    )


def fetch_geo_series(out: Path, *, base: Path | None, full: bool) -> None:
    """Patch the previous ``geo-series`` snapshot, or dump every Series summary."""
    from labdata.exceptions import LabdataError
    from labdata.ncbi.entrez import EntrezClient
    from labdata.store import series_snapshot

    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
    try:
        client = EntrezClient(allow_prompt=False)
    except LabdataError as e:
        raise FetchError(str(e)) from e
    try:
        done = series_snapshot.download(out, client, base=base, full=full)
    except LabdataError as e:
        raise FetchError(str(e)) from e
    finally:
        log(f"geo-series: NCBI {client.traffic}")
    how = f"patched from {done.patched_from}" if done.patched_from else "dumped whole"
    log(
        f"geo-series: {done.records} records, {done.fetched} fetched, {done.dropped} dropped, {how}"
    )


def main(argv: list[str] | None = None) -> int:
    """Run the requested sources; return a process exit status."""
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("root", type=Path, help="destination root, e.g. <datalake>/ncbi-bulk")
    p.add_argument("--date", default=f"{dt.datetime.now(dt.UTC):%Y-%m-%d}", help="snapshot dir")
    p.add_argument("--full", action="store_true", help="geo-series: fetch every summary")
    p.add_argument("--base", type=Path, help="geo-series: the snapshot folder to patch from")
    p.add_argument("sources", nargs="*", metavar="SOURCE", help=", ".join(SOURCES))
    args = p.parse_args(argv)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.date):
        p.error("--date must be YYYY-MM-DD")
    if unknown := set(args.sources) - set(SOURCES):
        p.error(f"unknown sources: {sorted(unknown)}")
    failed = []
    for source in args.sources or DEFAULT_SOURCES:
        out = args.root / source / args.date
        out.mkdir(parents=True, exist_ok=True)
        try:
            if source in FILES:
                fetch_file(FILES[source], out)
            elif source == "sra-metadata-parquet":
                fetch_parquet(out, args.root / "tmp" / source / args.date)
            else:
                fetch_geo_series(out, base=args.base, full=args.full)
            log(f"{source}: OK")
        except (FetchError, subprocess.CalledProcessError, OSError) as e:
            log(f"{source}: FAILED: {e}")
            failed.append(source)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
