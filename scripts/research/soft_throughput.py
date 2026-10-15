"""Measure how fast GEO family SOFT files download from NCBI's download site.

A small, polite benchmark for a research note, not part of the package. It draws a seeded
sample of sequencing Series from E-utilities, then fetches each family SOFT once per
configuration: HTTPS with one connection per worker kept alive, HTTPS with a new connection per
file, and FTP. It stops a configuration at the first 429, 503 or error.

Usage::

    python -I soft_throughput.py sample OUTDIR      # write OUTDIR/gse.txt
    python -I soft_throughput.py run OUTDIR N       # benchmark the first N of them
"""

from __future__ import annotations

import ftplib
import http.client
import json
import random
import socket
import statistics
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HOST = "ftp.ncbi.nlm.nih.gov"
ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"
TERM = 'gse[ETYP] AND "high throughput sequencing"[PTYP]'
UA = "liulab-data-research (+https://github.com/liuhlab/liulab-data)"

# Some hosts resolve IPv6 first but have no IPv6 route: resolve IPv4 only.
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda host, port, family=0, *a, **k: _getaddrinfo(
    host, port, socket.AF_INET, *a, **k
)


def path_of(gse: str) -> str:
    """Return the family SOFT path of a Series."""
    n = int(gse[3:])
    bucket = f"GSE{n // 1000}nnn" if n >= 1000 else "GSEnnn"
    return f"/geo/series/{bucket}/{gse}/soft/{gse}_family.soft.gz"


def sample(out: Path, seed: int = 49, chunks: int = 12, per: int = 50) -> None:
    """Write a seeded sample of sequencing Series accessions to ``out/gse.txt``."""

    def q(**kw: object) -> dict:
        kw.update(db="gds", term=TERM, retmode="json", tool="liulab-data-research")
        url = ESEARCH + urllib.parse.urlencode(kw)
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.load(r)["esearchresult"]

    n = int(q(retmax=0)["count"])
    rng = random.Random(seed)
    got: set[str] = set()
    for start in sorted(rng.sample(range(n - per), chunks)):
        time.sleep(0.5)
        got |= {f"GSE{int(u) - 200_000_000}" for u in q(retstart=start, retmax=per)["idlist"]}
    order = sorted(got)
    rng.shuffle(order)
    (out / "gse.txt").write_text("\n".join(order) + "\n")
    print(f"count={n} sampled={len(order)}")


class Stop(Exception):
    """A throttling or error sign: stop the configuration."""


_local = threading.local()


def https_keepalive(gse: str) -> tuple[int, float]:
    """Fetch over a per-thread kept-alive HTTPS connection."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = _local.conn = http.client.HTTPSConnection(HOST, timeout=60)
    t = time.perf_counter()
    conn.request("GET", path_of(gse), headers={"User-Agent": UA})
    r = conn.getresponse()
    body = r.read()
    if r.status != 200:
        raise Stop(f"{gse} HTTP {r.status} {dict(r.getheaders())}")
    return len(body), time.perf_counter() - t


def https_fresh(gse: str) -> tuple[int, float]:
    """Fetch over a new HTTPS connection (TCP + TLS handshake every file)."""
    t = time.perf_counter()
    conn = http.client.HTTPSConnection(HOST, timeout=60)
    conn.request("GET", path_of(gse), headers={"User-Agent": UA, "Connection": "close"})
    r = conn.getresponse()
    body = r.read()
    conn.close()
    if r.status != 200:
        raise Stop(f"{gse} HTTP {r.status}")
    return len(body), time.perf_counter() - t


def ftp_keepalive(gse: str) -> tuple[int, float]:
    """Fetch over a per-thread logged-in FTP session (one data connection per file)."""
    ftp = getattr(_local, "ftp", None)
    if ftp is None:
        ftp = _local.ftp = ftplib.FTP(HOST, timeout=60)
        ftp.login()
    buf = bytearray()
    t = time.perf_counter()
    try:
        ftp.retrbinary(f"RETR {path_of(gse)}", buf.extend)
    except ftplib.all_errors as e:
        raise Stop(f"{gse} FTP {e}") from e
    return len(buf), time.perf_counter() - t


def run(gses: list[str], fn, workers: int) -> dict:
    """Fetch every Series with ``workers`` threads; return timing stats."""
    lat: list[float] = []
    size = 0
    t0 = time.perf_counter()
    err = None
    with ThreadPoolExecutor(workers) as pool:
        for fut in [pool.submit(fn, g) for g in gses]:
            try:
                n, dt = fut.result()
            except Exception as e:  # noqa: BLE001 - any failure stops the configuration
                err = str(e)[:300]
                pool.shutdown(cancel_futures=True)
                break
            size += n
            lat.append(dt)
    wall = time.perf_counter() - t0
    lat.sort()
    return {
        "files": len(lat),
        "bytes": size,
        "wall_s": round(wall, 2),
        "files_per_s": round(len(lat) / wall, 2),
        "lat_median_s": round(statistics.median(lat), 3) if lat else None,
        "lat_p90_s": round(lat[int(0.9 * (len(lat) - 1))], 3) if lat else None,
        "error": err,
    }


def main() -> None:
    """Dispatch the ``sample`` and ``run`` commands."""
    cmd, out = sys.argv[1], Path(sys.argv[2])
    if cmd == "sample":
        sample(out)
        return
    n = int(sys.argv[3])
    skip = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    # A second fetch of a file is served warm, so each configuration gets its own files.
    gses = (out / "gse.txt").read_text().split()[skip:]
    plan = [
        ("https-keepalive", https_keepalive, 1),
        ("https-keepalive", https_keepalive, 4),
        ("https-keepalive", https_keepalive, 8),
        ("ftp-session", ftp_keepalive, 1),
        ("ftp-session", ftp_keepalive, 4),
    ]
    for i, (name, fn, w) in enumerate(plan):
        part = gses[i * n : (i + 1) * n]
        res = {"mode": name, "workers": w, **run(part, fn, w)}
        print(json.dumps(res), flush=True)
        if res["error"]:
            print("stopping: error or throttling sign", flush=True)
            break
        time.sleep(5)


if __name__ == "__main__":
    main()
