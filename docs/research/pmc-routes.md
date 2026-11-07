---
search:
  exclude: true
---

# PMC routes to original files

Question ([#138](https://github.com/liuhlab/liulab-data/issues/138)): for a PMID with a PMCID,
what are the bulk and per-paper ways to get the original full text (JATS XML, PDF) from PMC
and Europe PMC? For each route: which papers it covers, which file kinds, how to list what is
there, how it changes over time, and its request limits. Which route suits a daily job that
fetches only new papers?

All probes ran on 2026-11-07 with a few requests each. Each claim is tagged **measured**
(seen in a live response or a count), **read** (stated in the source's own docs) or
**inferred**. Licences and terms of use are out of scope.

## Answer

- **NCBI rebuilt its distribution in 2026.** The PMC FTP article packages (`oa_bulk`,
  `oa_package`, the manuscript packages) and the OA Web Service are gone. They were removed
  the week of 2026-08-24 (read, [FTP page][ftp], [OA service page][oasvc]). Old code and old
  write-ups that use them no longer work.
- **One route serves both XML and PDF: the PMC Article Datasets bucket on AWS S3**,
  `s3://pmc-oa-opendata`. It holds one folder per article version, with the JATS XML, a text
  file, a JSON record and, when the publisher allows, the PDF (read, [README][readme]). It is
  public over plain HTTPS with no account (measured).
- **Coverage is the OA subset plus author manuscripts.** Other PMC articles are not in the
  bucket. No free route returns their full XML (measured on PMC13735078, below).
- **Author manuscripts come without a PDF.** In a sample of 40 GEO-linked articles in the
  bucket, all 33 OA articles had a PDF and none of the 7 author manuscripts did (measured).
- **For GEO papers:** 151,093 PubMed records link to GEO; 126,395 of them (84%) are in PMC;
  110,829 PMC articles with a GEO link are in the bucket, about 73% of the GEO-linked PubMed
  records (measured, eSearch counts; the ratio is inferred, as PubMed and PMC counts differ
  slightly).
- **Daily job: per-paper S3 reads, keyed by the ID converter.** For each newly linked PMID,
  ask the PMC ID converter for the PMCID (200 IDs per call), list `PMC<id>.` in the bucket,
  then fetch the newest version's `.xml` and `.pdf`. Papers with no PMCID yet, or not in
  the bucket, go back on the retry list. Once a day, diff the bucket's S3 inventory against
  the files on hand to catch updated or removed versions (inferred from the read docs and
  measured listings).

## Routes compared

| Route | Covers | File kinds | List what is there | Change over time | Limits |
| --- | --- | --- | --- | --- | --- |
| PMC S3 bucket `pmc-oa-opendata` | OA subset + author manuscripts + historical OCR | JATS XML, TXT, JSON; PDF and media when allowed | prefix listing per PMCID; daily S3 inventory | continuous adds and updates; rare silent removal | none stated by NCBI |
| E-utilities `efetch db=pmc` | all PMC; full body only for OA and author manuscripts | JATS XML | eSearch on `db=pmc` | as PMC | 3 req/s, 10 with API key |
| PMC OAI-PMH | full text only where allowed (`set=pmc-open`) | JATS XML (`pmc`), front matter (`pmc_fm`) | `ListIdentifiers` with `from`/`until` | release date | 3 req/s, no concurrency, >100 requests off-peak |
| BioC API (`pmcoa.cgi`) | OA subset + author manuscripts | BioC XML/JSON (derived, not original) | none | within 24 h of PMC update | none stated; one 429 measured |
| Europe PMC REST `fullTextXML` | OA subset only | JATS XML | `search` with `OPEN_ACCESS:y`, `FIRST_IDATE` ranges | as Europe PMC | 10 req/s or 500/min per IP |
| Europe PMC FTP `oa/`, `pdf/OA/` | OA subset | XML bundles; PDFs | directory listing | XML weekly, PDF monthly | none stated |
| PMC and Europe PMC web PDF links | any article with a PDF | PDF | none | n/a | bot check: not usable by a script |

## The PMC S3 bucket

Source: [README.txt][readme] in the bucket, the [AWS access page][pmcaws] and the
[NCBI Insights post of 2026-02-12][insights].

**What it holds (read).** One prefix per article version, `PMC<id>.<version>/`, with:

- `PMC<id>.<v>.xml`: JATS XML, "the same as" the XML from OAI-PMH and eFetch ([insights]);
- `PMC<id>.<v>.txt`: plain text extracted from the XML;
- `PMC<id>.<v>.json`: metadata, also copied under `metadata/`;
- the PDF and media files "when permissible by the publishers' licenses".

The JSON carries `pmcid`, `version`, `pmid`, `doi`, `mid`, `title`, `citation`,
`is_pmc_openaccess`, `is_manuscript`, `is_historical_ocr`, `is_retracted`, `license_code`,
`xml_url`, `text_url`, `pdf_url` and `media_urls`. Every URL carries the object's MD5 as an
`md5=` parameter (read; measured on PMC10009416.1). The legacy `oa_comm` / `oa_noncomm`
split is gone; the licence is a per-version field ([pmcaws]).

**The demo paper (measured).** PMID 39637862 maps to PMC11701262. The bucket has one version,
`PMC11701262.1/`, with `.xml` (254 KB), `.pdf` (8.1 MB), `.txt`, `.json`, 8 figures and 15
supplementary files. Its XML, TXT and JSON were last modified 2026-06-27; the PDF on
2026-02-23. So objects inside a version are rewritten in place.

**Versions (read, [pmcaws]).** Most articles have only version 1. Several versions exist when
the journal versions an article, or when PMC holds both an author manuscript and the final
version. Version numbers follow processing order, not preference. A version 2 can exist
without a version 1. In the 40-article sample, 3 had two versions (measured).

**How to list (read, [readme] section 5).**

- One paper: `GET https://pmc-oa-opendata.s3.amazonaws.com/?list-type=2&prefix=PMC<id>.&delimiter=/`
  returns its version prefixes. A PMCID outside the bucket returns `KeyCount 0` (measured on
  PMC13735078).
- Everything: the S3 inventory under `inventory-reports/pmc-oa-opendata/metadata/`, one
  dated folder per day, kept 30 days. Columns: bucket, key of the JSON record, last-modified
  date, ETag. Because the JSON lists the MD5 of every object in the version, a change to any
  file changes the JSON's ETag and date (read).
- The 2026-11-06 inventory is 11 gzipped CSV files, 256 MB in all (measured). The README
  gives "roughly 8 million" article versions (read).
- New articles by date: eSearch `db=pmc` with `<date>[pmcrdat] AND (open_access[filter] OR
  author_manuscript[filter])` (read). On 2026-11-04 that matched 3,225 articles and on
  2026-11-05 3,618 (measured).

**How it changes (read).** Versions are added and updated continuously; in rare cases one is
removed, without notice. The inventory lags the bucket by up to a day. Reconciling against the
inventory is the only supported way to detect removals ([pmcaws]). One inventory part file
shows large rewrite waves: of 702,718 records, 563,255 were last modified in 2026-07 and
122,362 in 2026-10 (measured). A content diff therefore needs the ETag, not only the date.

**Limits.** NCBI states none for the bucket. It is anonymous HTTPS or `aws s3 --no-sign-request`
(read, [readme]); plain `curl` works (measured).

## E-utilities and OAI-PMH

- `efetch db=pmc` returns the full JATS body for OA articles: 254 KB with a `<body>` for
  PMC11701262. For the non-OA PMC13735078 it returns 13 KB of front matter and an XML comment
  that the publisher "does not allow downloading of the full text in XML form" (measured).
- E-utilities allow 3 requests per second, 10 with an API key ([E-utilities guide][eutils],
  read).
- OAI-PMH at `https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/` serves `pmc` (full JATS), `pmc_fm`
  (front matter) and `oai_dc`. `set=pmc-open` is "the complete collection of items in PMC for
  which the full text may be retrieved". `from`/`until` filter on the release date.
  `ListRecords` returns 10 records a page. Limits: 3 requests per second, no concurrent
  requests, scripts over 100 requests outside 05:00–21:00 US Eastern on weekdays
  ([OAI page][oai], read).
- Neither returns a PDF.

## BioC API

`https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_[xml|json]/[PMID|PMCID]/[unicode|ascii]`
covers the OA subset and the author manuscript collection, updated "within 24 hours" of PMC
([BioC page][bioc], read). Its output is BioC, a converted form, not the original JATS. For
PMC11701262 it returned 208 KB of BioC XML. For the non-OA PMC13735078 the first request got
HTTP 429, and the retry got "No result can be found" (measured). The page states no rate
limit. It does not suit a job that stores original files.

## Europe PMC

- **REST `fullTextXML`**
  (`https://www.ebi.ac.uk/europepmc/webservices/rest/<PMCID>/fullTextXML`): JATS for
  PMC11701262 (264 KB); HTTP 500 for the non-OA PMC13735078 (measured). The official REST
  docs page returned 403 to our client, so coverage is from probes, not the docs.
- **Search counts (measured):** `IN_EPMC:y` 12,484,394; `OPEN_ACCESS:y` 8,394,428;
  `AUTH_MAN:y` 1,137,689; `IN_EPMC:y AND OPEN_ACCESS:n` 4,091,083. NCBI's own counts on the
  same day: all PMC 12,773,973; `open_access[filter]` 8,374,214; `author_manuscript[filter]`
  1,089,812.
- **New papers:** the date range form works, `FIRST_IDATE:[2026-11-01 TO 2026-11-06]` matched
  16,703 records, 5,384 of them `IN_EPMC:y`. A single-day form `FIRST_IDATE:2026-10-15`
  returned 0 (measured).
- **Rate limit:** 10 requests per second or 500 per minute, per IP; it cannot be raised
  (Europe PMC developer forum reply of 2024-10-01, [forum][epmcrate], read).
- **Bulk FTP** at `https://ftp.ebi.ac.uk/pub/databases/pmc/`:
  `oa/` holds 1,319 `PMC<from>_PMC<to>.xml.gz` bundles, all dated 2026-11-07, so the whole set
  is rewritten on each update (measured). `pdf/OA/` holds PDFs in `PMCxxxx<nnn>/` folders
  (measured). The [downloads page][epmcdl] says XML updates weekly and PDFs monthly (read).
  `manuscripts/` holds NCBI-style author manuscript baseline and incremental packages; the
  newest incremental is dated 2026-04-09 (measured), so it looks frozen since NCBI's change
  (inferred).
- **Web PDF link** `https://europepmc.org/articles/<PMCID>?pdf=render`: the search record lists
  it even for the non-OA PMC13735078. A plain request got HTTP 403 with a Cloudflare
  "Just a moment..." challenge, for OA and non-OA alike (measured).

## Non-OA PMC articles

Europe PMC counts about 4.1 million PMC articles outside the OA subset (measured). Among
GEO-linked papers, 126,334 PMC articles carry a GEO link and 110,829 of those are OA or
author manuscripts, which leaves about 15,500 without a free machine route (measured counts,
difference inferred). For them:

- the S3 bucket, BioC and Europe PMC `fullTextXML` return nothing; eFetch returns front
  matter only (measured on PMC13735078);
- the PMC web PDF `https://pmc.ncbi.nlm.nih.gov/articles/<PMCID>/pdf/` answered with a Google
  reCAPTCHA challenge page (measured), and the Europe PMC PDF link with a Cloudflare
  challenge (measured).

These papers belong to the later tiers of the stage 4 map (OpenAlex, then Paperpile).

## ID mapping

- PMC ID converter API: `https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/`, up to 200
  IDs per request, `idtype=pmid`, `versions=yes` lists PMC versions, asks for `tool` and
  `email` ([ID converter page][idconv], read). It returned PMC11701262 and PMC13735078 for
  PMIDs 39637862 and 39552445 (measured). It does not say whether a PMCID is in the bucket.
- Bulk file `https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz` maps PMCID to PMID, DOI and
  manuscript ID ([FTP page][ftp], read). It is 258 MB and was last modified 2026-11-07
  (measured), so it is refreshed about daily (inferred).

## A daily job for new papers

Inferred from the facts above:

1. Take PMIDs newly linked in the build, plus the retry list.
2. Map them to PMCIDs with the ID converter, 200 per call. No PMCID: retry later; papers
   often reach PMC after their PubMed record.
3. For each PMCID, list `PMC<id>.` in the bucket. Empty: not OA and not an author
   manuscript, so pass it to the next tier.
4. Take the highest version. Download its JSON, `.xml` and, if `pdf_url` is set, the `.pdf`;
   check each against the `md5` in the JSON. An author manuscript gives XML only.
5. Once a day, read the newest S3 inventory (256 MB) and compare each held version's ETag.
   A changed ETag means a re-fetch; a missing key means the version was removed.

Bucket reads cost NCBI's E-utilities nothing. The only E-utilities load is the ID converter,
a few calls a day. Europe PMC adds no paper that the bucket lacks, for XML or for a
script-fetched PDF, so it is a fallback only if the bucket is down.

## Uncertain

- The 40-article PDF sample came from the newest 10,000 GEO-linked articles (eSearch returns
  at most 10,000 IDs), so the "author manuscripts have no PDF" result may not hold for older
  ones.
- Whether `pmc_gds[filter]` matches exactly the papers the store links to Series; the store
  also links SRA-only BioProjects.
- Europe PMC's REST docs page could not be read (403); its coverage claims rest on probes.
- What the version folder holds when an author manuscript and the final article share a
  PMCID: the docs say each is its own version; which one has a PDF was not probed.

[readme]: https://pmc-oa-opendata.s3.amazonaws.com/README.txt
[pmcaws]: https://pmc.ncbi.nlm.nih.gov/tools/pmcaws/
[insights]: https://ncbiinsights.ncbi.nlm.nih.gov/2026/02/12/pmc-article-dataset-distribution-services/
[ftp]: https://pmc.ncbi.nlm.nih.gov/tools/ftp/
[oasvc]: https://pmc.ncbi.nlm.nih.gov/tools/oa-service/
[oai]: https://pmc.ncbi.nlm.nih.gov/tools/oai/
[eutils]: https://www.ncbi.nlm.nih.gov/books/NBK25497/
[bioc]: https://www.ncbi.nlm.nih.gov/research/bionlp/APIs/BioC-PMC/
[idconv]: https://pmc.ncbi.nlm.nih.gov/tools/id-converter-api/
[epmcrate]: https://groups.google.com/a/ebi.ac.uk/g/epmc-webservices/c/x3PLL09mels
[epmcdl]: https://europepmc.org/downloads/openaccess
