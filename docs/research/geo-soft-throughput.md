---
search:
  exclude: true
---

# How fast GEO family SOFT files download in bulk

Measured **2026-10-15** from one lab compute node, against NCBI's download site
(`https://ftp.ncbi.nlm.nih.gov/`). The question: how fast can the family SOFT file
(`GSExxx_family.soft.gz`) be fetched for about 198,000 sequencing Series, what NCBI allows,
and is there anything better than one request per Series?

Evidence classes, as in the earlier notes. **Measured** means a fetch or a script was run on
2026-10-15 and the number is its output. **Read** means an official page says it.
**Inferred** means reasoned from measured or read facts and not checked directly.

The benchmark is `scripts/research/soft_throughput.py`. It draws a seeded random sample of 600
sequencing Series from E-utilities and fetches each file once per configuration, with fresh
files for each configuration.

## Verdict

**Fetch over HTTPS with kept-alive connections, 4 to 8 in parallel, from one node.** One
connection fetches about 21 small files per second, and 8 connections fetch about 160 per
second, with no sign of throttling (measured, 100 files each). At that rate the 196,000 small
files take about 20 minutes, and the bytes (about 200–260 GB, most of them in the 2,360 mixed
Series) take about 25–45 minutes at the measured 100–190 MB/s single-stream rate. **Estimate:
about 1–1.5 hours of wall time for every family SOFT, on one node (inferred).** Fetching
`filelist.txt` as well doubles the request count, about 20 minutes more (inferred).

Do not use FTP and do not open a new connection per file: both showed timeouts at small scale
(measured, below). Spreading the fetch over several nodes buys little, because one node already
covers the total in about an hour, and it multiplies the risk of tripping a limit NCBI does not
publish (inferred).

**Cons:** no test ran longer than a few seconds, so sustained throttling at full scale is
untested. NCBI publishes no limit for this site, so the safe rate is unknown. The bandwidth
figure comes from NCBI's 1 GB test file, not from GEO files.

## 1. NCBI's guidance and limits

| Claim | Evidence | Source |
| --- | --- | --- |
| The E-utilities limit (3 requests per second, 10 with an API key) is scoped to "E-utility requests" at `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`. Nothing in it covers the download site. | read | [E-utilities usage guidelines](https://www.ncbi.nlm.nih.gov/books/NBK25497/), last updated 2022-11-17 |
| The two hosts run on different infrastructure: `eutils` resolves to an address in Google Cloud's range, `ftp.ncbi.nlm.nih.gov` to NCBI's own address block. So the key limit does not govern the download site. | measured (DNS); the conclusion is inferred | `getent ahosts` from the node |
| GEO says all records are "freely available for bulk download" from the FTP site, and points to `README.ftp` for tuning. For "a very large number or volume of downloads" it suggests Aspera and asks users to email GEO. | read | [Download GEO data](https://www.ncbi.nlm.nih.gov/geo/info/download.html), last modified 2026-07-08 |
| `README.ftp` gives FTP passive mode and a 32 MB socket buffer. It states no limit on connections or request rate. | read | `https://ftp.ncbi.nlm.nih.gov/README.ftp` |
| The large-data guide is about bandwidth for SRA-size files (Aspera, 500 Mbps paths). It says nothing on many small files or connection counts. | read | [Large Data Download Best Practices](https://www.ncbi.nlm.nih.gov/books/NBK51061/), last updated 2011-01-04 |
| No NCBI page found states a cap on concurrent connections or requests per second for the download site. | read (absence, over the pages above) | |
| `robots.txt` on the download site disallows all paths for every crawler. This is advice to crawlers, not a rate limit. | measured | `https://ftp.ncbi.nlm.nih.gov/robots.txt` |
| rsync is refused: connection refused on port 873, from two networks. | measured | `rsync --list-only rsync://ftp.ncbi.nlm.nih.gov/...` |
| The site speaks HTTP/1.1 only (no HTTP/2), and keeps connections alive. | measured | `curl --http2`, `curl -v` |
| Aspera for GEO is offered only on request by email, and Globus is not mentioned on any NCBI page found. | read | GEO download page above |
| `www.ncbi.nlm.nih.gov` documentation pages serve a reCAPTCHA page to `curl` from the compute node. The download site and E-utilities did not. | measured | |

## 2. Measured throughput

Sample: 600 sequencing Series, seeded at random from the 210,789 that E-utilities lists
(`gse[ETYP] AND "high throughput sequencing"[PTYP]`). Each row fetched its own 100 files once.

| Mode | Connections | Files | Wall | Files per second | Median latency | p90 latency |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTPS, kept alive | 1 | 100 | 4.7 s | 21 | 41 ms | 75 ms |
| HTTPS, kept alive | 4 | 100 | 1.3 s | 79 | 44 ms | 77 ms |
| HTTPS, kept alive | 8 | 100 | 0.6 s | 161 | 43 ms | 82 ms |
| FTP, one session per worker | 1 | 40 | 9.2 s | 4.3 | 89 ms | 125 ms |
| FTP, one session per worker | 4 | 10 of 60 | stopped | — | 107 ms | — |

All measured. Further notes:

- **Latency dominates.** A 3 KB file over a kept-alive connection takes about 41 ms, and the
  median per-file latency did not rise from 1 to 8 connections (measured). Throughput scales
  almost linearly with connections up to 8, the most tested.
- **A new connection per file costs more.** `curl` with a new connection took about 78 ms:
  11 ms to connect, 53 ms for TLS, 14 ms for the file (measured, three tries). One of three
  later `curl` connects took 10 s (measured). A run that opened a new TLS connection per file
  hit a handshake timeout after 51 files (measured). Read it as a warning sign, not a proven
  limit.
- **FTP is slower and less robust.** One FTP session took 89 ms per file, about twice HTTPS,
  because each file needs a new data connection (measured; the reason is inferred). Four
  parallel FTP sessions hit a 60 s timeout after 10 files, and testing stopped there (measured).
- **A repeated fetch is served warm.** The same 100 files fetched a second time took 13 ms
  each, against 41 ms cold (measured). The server sets `Cache-Control: max-age=86400`
  (measured). So benchmarks must use fresh files, as this one does.
- **Bandwidth.** A single HTTPS stream of NCBI's 1 GB test file ran at 193 MB/s, and FTP at
  98 MB/s (measured, one try each, 20 s cap). The node is shared, so treat these as upper
  bounds.
- **No throttling sign over HTTPS:** no 429, no 503 and no slowdown over about 500 HTTPS
  fetches (measured).

## 3. Bulk alternatives to one request per Series

- **No archive or dump of family SOFT files exists.** `/geo/` holds only `datasets/`,
  `platforms/`, `samples/`, `series/` and `README.txt` (measured, here and in the earlier
  source survey). `ls-lR.gz` at the site root is 404 (measured).
- **No cloud mirror of GEO.** The AWS Open Data registry lists NCBI's SRA, BLAST databases, PMC
  and others, but no GEO dataset (read, the registry's dataset list). The bucket names
  `ncbi-geo` and `geo-ncbi` answer 404 (measured).
- **GEOmetadb** is a Bioconductor package that ships GEO metadata parsed into SQLite (read,
  [the package page](https://bioconductor.org/packages/GEOmetadb/)). It is a derived parse, not
  GEO's text, so it cannot replace the verbatim SOFT the store keeps (inferred). It could serve
  as a cross-check.
- **Cheap listing of what exists.** Each range directory listing (`/geo/series/GSE60nnn/`)
  gives every Series directory and its date, but no file sizes (measured). The 351 range
  directories take under 10 minutes at 8 in parallel (measured in the earlier source survey).
  The directory date stays fixed when GEO regenerates the files inside it (measured in the
  earlier survey), so it is a list of Series, not a change signal.
- **File size and date per Series cost one request each.** A `soft/` directory listing or a
  `HEAD` request gives size and `Last-Modified`, but that is one round trip per Series, the same
  cost as fetching a 3 KB file (measured: one listing shows `2.8K` and the date). So for the
  small files, fetching is as cheap as listing (inferred). A `HEAD` first pays off only to pick
  out the few very large files (inferred).
- **Conditional GET works.** `If-Modified-Since` with the stored `Last-Modified` returns
  `304 Not Modified` (measured). The site sends no `ETag` (measured). This helps a later
  refresh skip unchanged bytes, but GEO regenerates every family file on a rolling cycle, so
  the date moves even when the text does not (measured in the earlier survey).

## 4. Recommended fetch shape

1. **List.** Take the Series list from E-utilities, as stage 1 does. Optionally cross-check it
   against the range directory listings.
2. **Fetch over HTTPS with kept-alive connections**: a pool of 4–8 workers on one node, each
   holding one connection (for example one `http.client.HTTPSConnection` or a pooled
   `urllib3`/`httpx` client per worker). Start at 4 and raise to 8 only while no 429, 503 or
   timeout shows up.
3. **Back off on any throttling sign**: on 429, 503 or a connection or TLS timeout, halve the
   workers and retry with exponential backoff. Log every such event.
4. **Write each file as it arrives**, with atomic writes, so a preempted job resumes from what
   is saved.
5. **Stream the large files to disk** instead of holding them in memory. The mixed Series reach
   1.8 GB each (measured in stage 1).

Estimated wall time for every family SOFT: **about 1–1.5 hours on one node (inferred)**. The
split is about 20 minutes for the 196,000 small files at 160 per second, plus about 25–45
minutes for 200–260 GB at 100–190 MB/s. The byte total is stage 1's inferred estimate, with a
wide interval.

**What stage 1's code does now (read, `labdata.store.fetch` and `labdata.ncbi.entrez`):**
`fetch_series` fetches one file at a time, and `EntrezClient.fetch_text` opens a new
`urllib` connection for every file. It also keeps every response in an in-memory cache, and
`fetch_series` keeps every text in a dict until the end. At full scale that means about 41 ms
plus a TLS handshake per file serially, about 4–5 hours for the small files alone (inferred),
and every byte held in memory (inferred). The stage 2 fetch needs a pooled, parallel client
and streaming writes.

## Cons and open points

- Runs were seconds long and used at most 8 connections. Sustained behaviour over an hour is
  unmeasured.
- NCBI publishes no limit for the download site, so a block for heavy use is possible and
  undocumented. A short email to GEO before the full run is cheap insurance; the GEO download
  page invites one for "a very large number or volume of downloads" (read).
- All numbers come from one node on one network, at one time of day. NCBI asks that large
  E-utilities jobs run on weekends or between 9 PM and 5 AM Eastern time (read, the
  E-utilities guidelines). That advice is for E-utilities, but it is a sensible default here
  too (inferred).
- The bandwidth figure is from one 20-second stream of a test file.

## What was run

- `scripts/research/soft_throughput.py sample` drew 600 Series (seed 49, 12 random chunks of
  50 from the E-utilities list).
- `run` and `one` timed the HTTPS and FTP configurations above. A first run that reused
  files is discarded, except for its new-connection timeout and the warm-fetch latency.
- `curl` probed latency, conditional GET, HTTP/2, bandwidth on the 1 GB test file, and the
  listings. `rsync --list-only` probed rsync.
- NCBI's documentation pages were read in a browser, because they served a reCAPTCHA page to
  `curl`.
