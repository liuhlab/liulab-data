---
search:
  exclude: true
---

# Supplementary files of papers

Question ([#145](https://github.com/liuhlab/liulab-data/issues/145)): what supplementary
material comes with the papers the store links, and how can a script get it? Narrowed by the
maintainer to one decision: does labdata download supplementary files, or keep only a
**catalog** (paper, file name, type, size, URL, checksum) that seqforge fetches on demand? Very
large files are not downloaded either way. Judging what a file holds is seqforge's work.

All probes ran on 2026-11-07, gently: listings and small metadata files, no supplementary file
downloaded. Each fact is tagged **measured** (live probe or count), **read** (stated by the
source) or **inferred**. Licences and terms are out of scope. Builds on
[PMC routes](https://github.com/liuhlab/liulab-data/blob/research/pmc-routes/docs/research/pmc-routes.md),
[OpenAlex downloads](https://github.com/liuhlab/liulab-data/blob/research/openalex-downloads/docs/research/openalex-downloads.md)
and
[Paperpile's Drive sync](https://github.com/liuhlab/liulab-data/blob/research/paperpile-drive/docs/research/paperpile-drive.md).

## Answer

- **Recommend: catalog everything, download files up to 5 MB.** The catalog comes almost free
  from the PMC bucket (one listing per article). A 5 MB cap keeps 85% of files but 13% of the
  bytes, about 2.6 MB per article, roughly 200 GB for the whole PMC set (inferred from the
  sample below). Files above the cap stay in the catalog with their URL and MD5, for seqforge to
  fetch on demand.
- **Only the PMC bucket gives a script a usable catalog.** Per article it gives file names,
  sizes, MD5s and stable URLs; the article XML says which files are supplements and gives each
  one's label and caption (measured).
- **Author manuscripts have no supplementary files in the bucket.** Their XML names the files
  (`NIHMS…-supplement-…`), but the files are absent, and the PMC web link is behind a reCAPTCHA
  (measured). Catalog them as "declared, not available".
- **OpenAlex has no supplementary-file data** (measured: no "supplement" on its works
  attribute page). Publisher pages mostly sit behind bot checks; Nature pages are the one
  scriptable case seen (measured). Non-PMC papers get no catalog from a script; Paperpile
  syncs supplements the maintainer attaches (read, Paperpile note).
- **Most files are small tables and documents; a few zips dominate the bytes.** Median file
  217 KB; p90 8.9 MB; p99 59 MB; largest 257 MB. Zips were 8% of files and 55% of bytes
  (measured, 242 articles).

## Options and their cost

Sizes are extrapolated from the 242-article sample to the about 77,000 linked papers in the
bucket (81% of 95,391 PMIDs, from the PMID count research). The sample is GEO-linked PMC
articles, not exactly the store's PMIDs, and one article (PMC12222798, 1.7 GB of zips) moves
the mean a lot, so treat the totals as order of magnitude (inferred).

| Option | Kept per article (mean) | Whole set | Daily cost | seqforge |
| --- | --- | --- | --- | --- |
| Catalog only | a few KB of rows | well under 1 GB | 1 listing per new or changed article | fetches every file it wants |
| Catalog + files ≤ 1 MB | 0.5 MB | ~40 GB | plus small GETs | has 66% of files locally |
| **Catalog + files ≤ 5 MB** | **2.6 MB** | **~200 GB** | plus GETs, ~MB each | **has 85% of files locally** |
| Catalog + files ≤ 10 MB | 4.4 MB | ~340 GB | as above | has 90% of files |
| Catalog + files ≤ 50 MB | 12.5 MB | ~1 TB | as above | has 98% of files |
| Everything | 19.8 MB | ~1.5 TB | as above | all |

Why 5 MB (inferred): sample-metadata and design tables are spreadsheets and documents, and
those are small. `.xlsx` files averaged 1.4 MB and `.docx` 1.6 MB, while zips averaged 30 MB.
Above 5 MB the files are mostly zips, large PDFs and videos, which a reading agent rarely needs
first. Catalog-only is cheaper still, but every look by seqforge then needs a network fetch, and
the bucket can drop a version without notice (read, PMC routes note).

## Route 1: the PMC bucket

### What one article gives

Every article version is a folder `PMC<id>.<v>/` in `s3://pmc-oa-opendata`. Beside the XML,
TXT, JSON and PDF it holds the media files under their original names (measured on 42 + 242
articles, see Probes).

| Source | Gives | Lacks | Evidence |
| --- | --- | --- | --- |
| S3 listing `?list-type=2&prefix=PMC<id>.` | every object of every version: key, size, ETag, last modified; one request | which file is a supplement | measured |
| JSON record `media_urls` | each media file's URL with `md5=` | sizes; figures and supplements are one list | measured on PMC11701262 |
| Article XML `<supplementary-material>` | the file name (`xlink:href`), label ("Table S1"), caption | sizes | measured |
| S3 inventory (daily) | JSON keys with ETag and date only: schema `Bucket, Key, LastModifiedDate, ETag` | media files, sizes | measured, [manifest][manifest] |

So a full catalog row needs the listing (size) plus the XML (is it a supplement, its label and
caption) plus the JSON (MD5). The job fetches the XML and JSON anyway for the paper itself
(PMC routes note), so the catalog adds one listing request per article (inferred). The listing
ETag is not always the MD5: 150 of 3,589 objects had multipart ETags (`…-N`) (measured), so
take the MD5 from the JSON. A change to any file changes the JSON's ETag in the inventory
(read, PMC routes note), so the daily job re-lists only articles whose JSON changed.

### Names and types

- Names keep the publisher's own scheme: `mmc1.pdf` (Cell Press), `…_MOESM1_ESM.xlsx`
  (Springer Nature), `elife-…-supp2.xlsx`, `DataSheet2.xlsx` / `Table_1.XLSX` (Frontiers),
  `supp_gku391_….zip` (Oxford), `sciadv.ade3559_sm.pdf` (Science) (measured). There is no
  common pattern, so a name tells nothing reliable about content (inferred).
- Figures are separate: `gr1.jpg`, `…_Fig1_HTML.jpg` and similar, referenced by `<graphic>`
  in the XML (measured). In the listing sample, `.jpg`/`.gif`/`.png` added 1.2 MB per article
  (measured).
- XML `<supplementary-material>` is not always complete. In PMC4242493 three supplementary
  files sat in the folder but no `<supplementary-material>` named them; in PMC6965328 the
  element also held GEO accession links, not files, and five labels shared one file
  `Table_1.XLSX` (measured). So match listing files against every `xlink:href` in the XML, and
  treat a non-figure file that no element names as a supplement too (inferred).
- Some supplements are not data: PMC10820737's only supplement is a peer-review history PDF
  (measured).

Types in the 242-article listing sample (non-image files outside the core four; this count
includes a few figure files in `.tif`, `.eps` or `.webp`):

| Extension | Files | MB |
| --- | --- | --- |
| `.xlsx` | 350 | 502 |
| `.pdf` | 322 | 1,041 |
| `.docx` | 113 | 177 |
| `.zip` | 89 | 2,648 |
| `.tif` | 69 | 106 |
| `.xls` | 56 | 35 |
| `.webp` | 18 | 4 |
| `.csv` | 10 | 48 |
| `.doc`, `.eps`, `.mov`, `.mp4`, `.html`, `.tiff`, `.wmv` | 48 | 225 |

### Sizes

From the 242-article listing sample (measured):

- 191 of 242 articles (79%) have at least one supplement-like file; 4.5 files per article on
  average.
- Per article: median 2.0 MB, mean 19.8 MB, p90 36 MB, max 1.7 GB.
- Per file: p50 217 KB, p75 2.2 MB, p90 8.9 MB, p95 26 MB, p99 59 MB, max 257 MB.
- Tail: the ten largest files are zips and one 80 MB PDF, from Springer Nature (`MOESM…zip`, up
  to 257 MB) and eLife source-data zips (`elife-…-fig1-data1.zip`, ~60 MB each).

Cap table (measured on the same files):

| Cap | Files kept | Bytes kept | MB per article |
| --- | --- | --- | --- |
| 1 MB | 66% | 3% | 0.5 |
| 5 MB | 85% | 13% | 2.6 |
| 10 MB | 91% | 22% | 4.4 |
| 20 MB | 94% | 34% | 6.7 |
| 50 MB | 98% | 63% | 12.5 |
| 100 MB | 99.7% | 88% | 17.4 |

The 42-article year-stratified sample with XML agrees in shape: median 0.66 MB, mean 5.2 MB
per article; `.xlsx`, `.pdf`, `.xls`, `.doc`/`.docx` were 115 of 130 supplementary files
(measured).

### Author manuscripts

Of 8 author manuscripts in the 42-article sample, 6 had supplements declared in the XML and
only 1 (PMC5325677) had the files in the bucket (measured). The other 5 folders held only
XML, TXT and JSON. Their files are named like `NIHMS196477-supplement-01.doc`. The PMC web
URL `https://pmc.ncbi.nlm.nih.gov/articles/instance/3032599/bin/NIHMS196477-supplement-01.doc`
returned a Google reCAPTCHA page (measured), so it is not a script route.

Europe PMC's `…/rest/<PMCID>/supplementaryFiles` returned a zip of all supplements for an OA
article and "is not open access one" for the two author manuscripts tried (measured). It gives
no file list or sizes before the download, so it adds nothing the bucket lacks (inferred).

Side finding: 3 of the 8 author manuscripts had a PDF (PMC3848608, PMC4418228, PMC5325677),
against none of 7 in the PMC routes sample (measured). Older author manuscripts may carry PDFs.

## Route 2: OpenAlex

The works attribute page has no field for supplementary files: the text "supplement" does not
occur on it (measured, [works attributes][oa-attr]). `locations[].pdf_url` points to the main
PDF only (read, OpenAlex note). OpenAlex can only give the DOI and landing page for a publisher
lookup (inferred).

## Route 3: publisher pages

One landing-page request per DOI, through `doi.org`, with a plain user agent (measured):

| Publisher | DOI | Result |
| --- | --- | --- |
| Nature (Springer Nature) | `10.1038/ncomms13176` | page served; 3 supplement links to `static-content.springer.com/esm/…`; `HEAD` gives size and ETag |
| BMC / Springer (`link.springer.com`) | `10.1186/s13059-014-0482-3` | 3 KB challenge page (`/_fs-ch-…` assets) |
| Cell Press (Elsevier) | `10.1016/j.xgen.2024.100720` | `linkinghub` redirect, then `cell.com` "Just a moment..." (403) |
| Oxford (NAR) | `10.1093/nar/gku391` | 403 "Just a moment..." |
| Science (AAAS) | `10.1126/sciadv.ade3559` | 403 "Just a moment..." |
| eLife | `10.7554/eLife.101035` | 406, empty body |
| Frontiers | `10.3389/fimmu.2019.03034` | page served; supplement links not in the static HTML |
| bioRxiv | `10.1101/2023.06.01.543200` | page and `.supplementary-material` page served; no file links found for this preprint |

Bot-check routes (Springer link, Elsevier, Oxford, Science, eLife's 406) are unusable for a
script. Nature's pages work but need per-publisher HTML parsing (inferred). For OA papers the
same files are already in the PMC bucket, often under the same names (Nature's `MOESM` names
appear in PMC folders, measured). So publisher pages help only non-PMC papers, and only from
a few hosts (inferred).

## Route 4: Paperpile

Paperpile syncs "all PDFs and supplementary files" to Drive, supplements named with an
author-year prefix (read, [Paperpile Drive sync][pp-sync]). For paywalled papers this is the
only route; it covers what the maintainer attaches by hand, with no catalog before that
(inferred).

## Which kinds matter for seqforge

Inferred from types and captions seen, not measured on content:

- **Worth having locally:** spreadsheets (`.xlsx`, `.xls`, `.csv`), supplementary-methods
  documents (`.pdf`, `.docx`, `.doc`) under the cap. These are where sample tables and design
  notes live, and they are small.
- **Catalog only:** zips, videos (`.mov`, `.mp4`, `.wmv`), image files (`.tif`, `.eps`) and
  anything over the cap. Zips are the bulk; their content (source data, count matrices) is
  often what GEO already holds, but some hold tables, so seqforge decides case by case.
- A type-based filter is possible but not needed: under a 5 MB cap the other types add little.

## Probes

1. Year-stratified sample: eSearch `db=pmc`, `pmc_gds[filter] AND (open_access[filter] OR
   author_manuscript[filter]) AND <year>[pdat]`, 2 random articles per year 2004–2026, 46
   articles, 42 in the bucket. For each: one listing, the JSON and the XML.
2. Year-weighted sample: the same query, years drawn in proportion to their counts, 243
   articles, 242 in the bucket. One listing each, no file content.
3. Inventory manifest of 2026-11-06, Europe PMC `supplementaryFiles` for three PMCIDs, one PMC
   web supplement URL, eight publisher landing pages, the OpenAlex works attribute page.

Bucket listings: `https://pmc-oa-opendata.s3.amazonaws.com/?list-type=2&prefix=PMC<id>.`
([bucket README][readme]).

## Uncertain

- Totals for the whole set extrapolate 242 articles; the mean depends on a heavy tail (one
  article had 1.7 GB). The per-cap numbers below 10 MB are stable; the totals above 50 MB are
  not.
- The listing sample counts all non-image files as supplement-like, which includes some figure
  files (`.tif`, `.eps`, `.webp`).
- Whether author manuscript supplements are ever in the bucket for newer manuscripts: 1 of 6
  had them.
- What fraction of supplementary tables actually hold sample metadata: not measured; it needs
  content reading, which is seqforge's work.
- Publisher coverage rests on one DOI per publisher.

[readme]: https://pmc-oa-opendata.s3.amazonaws.com/README.txt
[manifest]: https://pmc-oa-opendata.s3.amazonaws.com/inventory-reports/pmc-oa-opendata/metadata/2026-11-06T01-00Z/manifest.json
[oa-attr]: https://help.openalex.org/data/works/attributes/
[pp-sync]: https://paperpile.com/h/sync-google-drive/
