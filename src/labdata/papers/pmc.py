"""The PMC bucket tier: a paper's XML, PDF and supplements from ``pmc-oa-opendata``.

The bucket's daily inventory names every article version's JSON record with its ETag. A
paper whose newest version and record ETag are those of its last fetch is passed over; any
other paper in the bucket is listed, and each file whose ETag changed is fetched again. A
file streams to a temporary name, is checked against the record's MD5 and renamed into
place, so a killed pass leaves no partial file, and a rerun takes a file already in place
when its MD5 matches.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import http.client
import json
import random
import re
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Callable, Collection
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from labdata._http import RateLimiter, Transport
from labdata.exceptions import LabdataError
from labdata.papers.root import paper_dir
from labdata.papers.tables import PaperTables
from labdata.store._files import temp_beside

BUCKET = "https://pmc-oa-opendata.s3.amazonaws.com"

#: Where the bucket's daily inventory of JSON records lives, one dated folder a day.
INVENTORY = "inventory-reports/pmc-oa-opendata/metadata/"

#: Supplementary files up to this size are downloaded; larger ones are only catalogued.
SUPPLEMENT_MAX = 10 * 2**20

#: Requests started per second, and papers worked on at once.
RATE = 20.0
WORKERS = 4

#: Tries per request; a bad MD5 counts as a failed try.
TRIES = 3

_TIER = "pmc"
_TIMEOUT = 60.0
_CHUNK = 2**20
_SAVE_EVERY = 300.0
_LOG_EVERY = 60.0
_S3 = "{http://s3.amazonaws.com/doc/2006-03-01/}"
_XLINK = "{http://www.w3.org/1999/xlink}href"
_IMAGES = frozenset(
    {".jpg", ".jpeg", ".gif", ".png", ".tif", ".tiff", ".webp", ".eps", ".bmp", ".svg"}
)
_RECORD = re.compile(r"metadata/(PMC\d+)\.(\d+)\.json")

# Bound at import, so tests may stub it.
_sleep = time.sleep


class PmcError(LabdataError):
    """A PMC bucket request failed after its retries, or a file kept failing its MD5."""


def fetch(
    state: PaperTables,
    papers: Collection[str],
    *,
    root: Path,
    transport: Transport,
    today: dt.date,
    log: Callable[[str], None],
) -> None:
    """Fetch what the PMC bucket holds for ``papers`` into the store, changing ``state``.

    It adds or renews ``files`` rows, writes the files under each paper's folder, sets
    ``xml_tier`` and ``pdf_tier`` on ``papers`` rows and adds one ``attempts`` row per paper
    tried. A file the bucket no longer serves keeps its copy and gets a ``gone`` date. A PDF
    another tier supplied stays. The tables are saved every few minutes; the caller
    recomputes status and saves at the end. Papers with no PMCID are passed over.

    Parameters
    ----------
    state : PaperTables
        The paper store's tables, loaded.
    papers : collection of str
        The papers to try: every linked paper the PMC sync holds.
    root : pathlib.Path
        The paper store root.
    transport : Transport
        Sends the bucket's HTTPS requests.
    today : datetime.date
        The UTC date of the pass.
    log : callable
        Takes one progress line.

    Raises
    ------
    PmcError
        If the bucket's inventory cannot be read.
    """
    pmcids = {k: state.papers[k]["pmcid"] for k in papers if state.papers[k].get("pmcid")}
    if not pmcids:
        return
    bucket = _Bucket(transport, RateLimiter(RATE, WORKERS))
    clock = time.monotonic()
    newest = bucket.inventory(set(pmcids.values()), today, root)
    log(
        f"PMC inventory read in {time.monotonic() - clock:.0f} s: "
        f"{len(newest)} of {len(pmcids)} papers with a PMCID are in the bucket"
    )
    last = _last_fetch(state.attempts)
    by_paper: dict[str, list[dict[str, Any]]] = {}
    for row in state.files:
        by_paper.setdefault(row["paper"], []).append(row)
    tally = _Tally()
    todo: list[tuple[str, str, str]] = []
    for key, pmcid in pmcids.items():
        entry = newest.get(pmcid)
        if entry is None:
            for row in by_paper.get(key, []):
                if row["tier"] == _TIER and row["gone"] is None:
                    row["gone"] = today
                    tally.gone += 1
        elif last.get(key) != f"{entry[0]} {entry[1]}":
            todo.append((key, *entry))
    log(f"PMC bucket: {len(todo)} papers to sync, {len(pmcids) - len(todo)} unchanged or absent")

    def save() -> None:
        state.files[:] = [row for rows in by_paper.values() for row in rows]
        state.save(root)

    pool = ThreadPoolExecutor(WORKERS)
    saved = logged = time.monotonic()
    try:
        futures: list[Future[_Outcome]] = [
            pool.submit(
                _article,
                bucket,
                root,
                key,
                folder,
                [r for r in by_paper.get(key, []) if r["tier"] == _TIER],
                any(
                    r["kind"] == "pdf" and r["path"] and r["tier"] != _TIER
                    for r in by_paper.get(key, [])
                ),
                today,
            )
            for key, folder, _etag in todo
        ]
        etags = {key: f"{folder} {etag}" for key, folder, etag in todo}
        for done, future in enumerate(as_completed(futures), 1):
            out = future.result()
            other = [r for r in by_paper.get(out.paper, []) if r["tier"] != _TIER]
            by_paper[out.paper] = other + out.rows
            _set_tiers(state.papers[out.paper], out.rows)
            state.attempts.append(
                {
                    "paper": out.paper,
                    "tier": _TIER,
                    "date": today,
                    "outcome": "failed" if out.error else "fetched",
                    "detail": out.error or etags[out.paper],
                }
            )
            tally.add(out)
            now = time.monotonic()
            if now - saved >= _SAVE_EVERY:
                save()
                saved = now
            if now - logged >= _LOG_EVERY:
                log(f"PMC bucket: {done}/{len(todo)} papers, {tally}")
                logged = now
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    save()
    log(f"PMC bucket: {len(todo)} papers synced in {time.monotonic() - clock:.0f} s, {tally}")


@dataclass
class _Outcome:
    """One paper's sync: its PMC file rows, what was downloaded, and the failure if any."""

    paper: str
    rows: list[dict[str, Any]]
    files: int = 0
    bytes: int = 0
    gone: int = 0
    error: str | None = None


@dataclass
class _Tally:
    files: int = 0
    bytes: int = 0
    failed: int = 0
    gone: int = 0

    def add(self, out: _Outcome) -> None:
        self.files += out.files
        self.bytes += out.bytes
        if out.error:
            self.failed += 1
        self.gone += out.gone

    def __str__(self) -> str:
        return (
            f"{self.files} files, {self.bytes / 1e9:.3f} GB downloaded, "
            f"{self.failed} papers failed, {self.gone} files gone"
        )


def _last_fetch(attempts: list[dict[str, Any]]) -> dict[str, str]:
    """Return each paper's last PMC fetch: ``"<version folder> <record ETag>"``."""
    last: dict[str, tuple[dt.date, str]] = {}
    for a in attempts:
        if a["tier"] == _TIER and a["outcome"] == "fetched" and a["detail"]:
            date = a["date"] or dt.date.min
            if a["paper"] not in last or date >= last[a["paper"]][0]:
                last[a["paper"]] = (date, a["detail"])
    return {k: v for k, (_, v) in last.items()}


def _set_tiers(row: dict[str, Any], files: list[dict[str, Any]]) -> None:
    held = {f["kind"] for f in files if f["path"]}
    if "xml" in held:
        row["xml_tier"] = _TIER
    if "pdf" in held:
        row["pdf_tier"] = _TIER


def _article(
    bucket: _Bucket,
    root: Path,
    key: str,
    folder: str,
    old: list[dict[str, Any]],
    keep_pdf: bool,
    today: dt.date,
) -> _Outcome:
    """Sync one paper with its newest version folder; return its PMC rows."""
    out = _Outcome(key, [])
    previous = {(r["kind"], r["name"]): r for r in old}
    done: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        listing = bucket.list_folder(folder)
        record = json.loads(bucket.get(f"{folder}/{folder}.json"))
        md5s = _md5s(record)
        base = urllib.parse.quote(key, safe="")
        folder_dir = paper_dir(key)
        core = {f"{folder}.{ext}" for ext in ("json", "txt", "xml", "pdf")}
        wanted: list[tuple[str, str, str, Path | None]] = []
        xml_name = f"{folder}.xml"
        if xml_name in listing:
            wanted.append(("xml", f"{base}.xml", xml_name, folder_dir / f"{base}.xml"))
        pdf_name = f"{folder}.pdf"
        if pdf_name in listing and record.get("pdf_url") and not keep_pdf:
            wanted.append(("pdf", f"{base}.pdf", pdf_name, folder_dir / f"{base}.pdf"))
        for kind, name, source, path in wanted:
            done[(kind, name)] = _file(
                bucket,
                root,
                out,
                previous.get((kind, name)),
                kind,
                name,
                folder,
                source,
                path,
                listing,
                md5s,
                today,
            )
        labels: dict[str, tuple[str | None, str | None]] = {}
        graphics: set[str] = set()
        if ("xml", f"{base}.xml") in done:
            labels, graphics = _supplements(root / folder_dir / f"{base}.xml")
        for name in sorted(listing):
            if name in core or not _is_supplement(name, labels, graphics):
                continue
            size = listing[name][0]
            path = folder_dir / "suppl" / name if size <= SUPPLEMENT_MAX and _safe(name) else None
            row = _file(
                bucket,
                root,
                out,
                previous.get(("supplement", name)),
                "supplement",
                name,
                folder,
                name,
                path,
                listing,
                md5s,
                today,
            )
            row["label"], row["caption"] = _match(name, labels) or (None, None)
            done[("supplement", name)] = row
    except (PmcError, ValueError) as err:
        out.error = str(err)
    for k, row in previous.items():
        if k not in done:
            if out.error is None and row["gone"] is None:
                row = {**row, "gone": today}
                out.gone += 1
            done[k] = row
    out.rows = list(done.values())
    return out


def _file(
    bucket: _Bucket,
    root: Path,
    out: _Outcome,
    old: dict[str, Any] | None,
    kind: str,
    name: str,
    folder: str,
    source: str,
    path: Path | None,
    listing: dict[str, tuple[int, str]],
    md5s: dict[str, str],
    today: dt.date,
) -> dict[str, Any]:
    """Return one file's row, downloading it unless the copy held is current."""
    size, etag = listing[source]
    md5 = md5s.get(source) or (etag if "-" not in etag else None)
    row: dict[str, Any] = {
        "paper": out.paper,
        "kind": kind,
        "name": name,
        "path": None,
        "size": size,
        "md5": md5,
        "url": f"{BUCKET}/{folder}/{urllib.parse.quote(source)}",
        "tier": _TIER,
        "label": None,
        "caption": None,
        "fetched": None,
        "gone": None,
        "etag": etag,
    }
    if path is None:
        return row
    target = root / path
    if old and old["path"] and old["etag"] == etag and (root / old["path"]).is_file():
        return {**row, "path": old["path"], "fetched": old["fetched"]}
    if not (target.is_file() and md5 and _md5_of(target) == md5):
        target.parent.mkdir(parents=True, exist_ok=True)
        out.bytes += bucket.download(f"{folder}/{source}", target, md5)
        out.files += 1
    return {**row, "path": path.as_posix(), "fetched": today}


def _md5s(record: dict[str, Any]) -> dict[str, str]:
    """Return the MD5 of each file a JSON record names, by file name."""
    urls = [record.get(k) for k in ("xml_url", "pdf_url", "text_url")]
    urls += record.get("media_urls") or []
    found = {}
    for url in urls:
        if not url:
            continue
        parts = urllib.parse.urlsplit(url)
        md5 = urllib.parse.parse_qs(parts.query).get("md5")
        if md5:
            found[urllib.parse.unquote(parts.path.rsplit("/", 1)[-1])] = md5[0]
    return found


def _supplements(xml: Path) -> tuple[dict[str, tuple[str | None, str | None]], set[str]]:
    """Return the article XML's supplements, href to label and caption, and its figure hrefs."""
    try:
        tree = ET.parse(xml)
    except ET.ParseError:
        return {}, set()
    labels: dict[str, tuple[str | None, str | None]] = {}
    graphics: set[str] = set()
    for el in tree.iter():
        tag = _local(el.tag)
        if tag in ("graphic", "inline-graphic") and el.get(_XLINK):
            graphics.add(str(el.get(_XLINK)).rsplit("/", 1)[-1])
        if tag != "supplementary-material":
            continue
        label = caption = None
        for child in el:
            if _local(child.tag) == "label":
                label = " ".join("".join(child.itertext()).split()) or None
            elif _local(child.tag) == "caption":
                caption = " ".join("".join(child.itertext()).split()) or None
        for sub in el.iter():
            href = sub.get(_XLINK)
            if not href:
                continue
            href = href.rsplit("/", 1)[-1]
            had_label, had_caption = labels.get(href, (None, None))
            labels[href] = (_join(had_label, label), _join(had_caption, caption))
    return labels, graphics - set(labels)


def _join(a: str | None, b: str | None) -> str | None:
    if a and b and b != a:
        return f"{a}; {b}"
    return a or b


def _local(tag: Any) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _match(name: str, hrefs: Collection[str] | dict[str, Any]) -> Any:
    """Return the entry an href names, by full file name or by name without extension."""
    stem = name.rsplit(".", 1)[0]
    for candidate in (name, stem):
        if candidate in hrefs:
            return hrefs[candidate] if isinstance(hrefs, dict) else candidate
    return None


def _is_supplement(
    name: str, labels: dict[str, tuple[str | None, str | None]], graphics: set[str]
) -> bool:
    """Return whether a file is a supplement: the XML names it so, or it is no figure or image."""
    if _match(name, labels) is not None:
        return True
    if _match(name, graphics) is not None:
        return False
    return Path(name).suffix.lower() not in _IMAGES


def _safe(name: str) -> bool:
    return "/" not in name and "\\" not in name and not name.startswith(".")


def _md5_of(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


class _Bucket:
    """Requests to the PMC bucket: paced, and retried with backoff."""

    def __init__(self, transport: Transport, limiter: RateLimiter) -> None:
        self.transport = transport
        self.limiter = limiter

    def get(self, key: str) -> bytes:
        """Return one small object whole."""
        return self._request(f"{BUCKET}/{urllib.parse.quote(key)}", key)

    def list(
        self, prefix: str, delimiter: str = ""
    ) -> tuple[list[tuple[str, int, str]], list[str]]:
        """Return the objects under ``prefix`` as ``(key, size, etag)``, and common prefixes."""
        objects: list[tuple[str, int, str]] = []
        prefixes: list[str] = []
        token = ""
        while True:
            params = {"list-type": "2", "prefix": prefix}
            if delimiter:
                params["delimiter"] = delimiter
            if token:
                params["continuation-token"] = token
            body = self._request(f"{BUCKET}/?{urllib.parse.urlencode(params)}", f"list {prefix}")
            try:
                tree = ET.fromstring(body)
            except ET.ParseError as err:
                raise PmcError(f"list {prefix}: unreadable reply") from err
            for c in tree.iter(f"{_S3}Contents"):
                objects.append(
                    (
                        c.findtext(f"{_S3}Key") or "",
                        int(c.findtext(f"{_S3}Size") or 0),
                        (c.findtext(f"{_S3}ETag") or "").strip('"'),
                    )
                )
            prefixes += [
                p.text or "" for p in tree.iter(f"{_S3}Prefix") if p.text and p.text != prefix
            ]
            token = tree.findtext(f"{_S3}NextContinuationToken") or ""
            if tree.findtext(f"{_S3}IsTruncated") != "true" or not token:
                return objects, prefixes

    def list_folder(self, folder: str) -> dict[str, tuple[int, str]]:
        """Return one version folder's files, by name, as ``(size, etag)``."""
        objects, _ = self.list(f"{folder}/")
        return {key[len(folder) + 1 :]: (size, etag) for key, size, etag in objects}

    def download(self, key: str, target: Path, md5: str | None) -> int:
        """Stream one object to ``target`` through a temporary name; return its size.

        Raises
        ------
        PmcError
            If every try failed or gave bytes that do not match ``md5``.
        """
        url = f"{BUCKET}/{urllib.parse.quote(key)}"
        last = ""
        for attempt in range(TRIES):
            tmp = temp_beside(target, "part")
            try:
                digest = hashlib.md5()
                size = 0
                with (
                    self.limiter.turn(),
                    self.transport.open(
                        "GET", url, body=None, headers=_HEADERS, timeout=_TIMEOUT
                    ) as response,
                ):
                    status = response.status
                    if status == 200:
                        with tmp.open("wb") as out:
                            while chunk := response.read(_CHUNK):
                                digest.update(chunk)
                                out.write(chunk)
                                size += len(chunk)
                if status != 200:
                    last = f"HTTP {status}"
                    if status not in _RETRY:
                        break
                elif md5 is not None and digest.hexdigest() != md5:
                    last = "MD5 mismatch"
                else:
                    tmp.replace(target)
                    return size
            except (OSError, http.client.HTTPException) as err:
                last = type(err).__name__
            finally:
                tmp.unlink(missing_ok=True)
            _sleep(min(30.0, 2.0**attempt) * random.uniform(0.5, 1.0))
        raise PmcError(f"{key}: {last}")

    def inventory(self, pmcids: set[str], today: dt.date, root: Path) -> dict[str, tuple[str, str]]:
        """Return the newest version folder and record ETag of each of ``pmcids`` in the bucket.

        Reads the newest daily inventory dated ``today`` or earlier; its files stream to a
        temporary folder under ``root`` and are read one line at a time.
        """
        _, folders = self.list(INVENTORY, "/")
        dated = sorted(
            f
            for f in folders
            if (m := re.search(r"/(\d{4}-\d{2}-\d{2})T[^/]*/$", f))
            and dt.date.fromisoformat(m.group(1)) <= today
        )
        if not dated:
            raise PmcError(f"no PMC inventory dated {today} or earlier")
        manifest = json.loads(self.get(f"{dated[-1]}manifest.json"))
        newest: dict[str, tuple[int, str]] = {}
        with tempfile.TemporaryDirectory(prefix=".pmc-inventory-", dir=root) as tmp:
            for n, part in enumerate(manifest["files"]):
                path = Path(tmp) / f"{n}.csv.gz"
                self.download(part["key"], path, part.get("MD5checksum"))
                with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
                    for line in csv.reader(fh):
                        m = _RECORD.fullmatch(line[1]) if len(line) > 3 else None
                        if m is None or m.group(1) not in pmcids:
                            continue
                        version = int(m.group(2))
                        if version > newest.get(m.group(1), (0, ""))[0]:
                            newest[m.group(1)] = (version, line[3].strip('"'))
                path.unlink()
        return {p: (f"{p}.{v}", etag) for p, (v, etag) in newest.items()}

    def _request(self, url: str, what: str) -> bytes:
        last = ""
        for attempt in range(TRIES):
            try:
                with (
                    self.limiter.turn(),
                    self.transport.open(
                        "GET", url, body=None, headers=_HEADERS, timeout=_TIMEOUT
                    ) as response,
                ):
                    body = response.read()
                    status = response.status
            except (OSError, http.client.HTTPException) as err:
                last = type(err).__name__
            else:
                if status == 200:
                    return body
                last = f"HTTP {status}"
                if status not in _RETRY:
                    break
            _sleep(min(30.0, 2.0**attempt) * random.uniform(0.5, 1.0))
        raise PmcError(f"{what}: {last}")


_HEADERS = {"User-Agent": "liulab-data"}
_RETRY = frozenset({429, 500, 502, 503, 504})
