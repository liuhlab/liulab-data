---
search:
  exclude: true
---

# Can the bulk snapshots be renewed without a full download?

Checked **2026-11-06** against NCBI's download site (`https://ftp.ncbi.nlm.nih.gov/`),
E-utilities and the public SRA metadata bucket on AWS. The question: for each bulk snapshot the
store takes (`scripts/bulk/download_ncbi_bulk.py`), does NCBI publish a daily, incremental or
dated-delta version, how big is the full file, and should a renewal fetch the full file or a
delta?

**Measured** means a listing, a `HEAD` request, a ranged read of the first 20 MB, or an
E-utilities query was run on 2026-11-06 and the number is its output. **Read** means an
official page or file says it. **Inferred** means reasoned from measured or read facts and not
checked directly. No large file was downloaded.

## Verdict

| Snapshot | Full size | Delta from NCBI? | Recommendation |
| --- | ---: | --- | --- |
| `SRA_Accessions.tab` | 35.6 GB | No delta in this format. A daily `livelist.csv.gz` exists, but it lacks the `Study` and `Experiment` columns the store reads | Full re-download |
| `bioproject.xml` | 4.2 GB | No file delta. E-utilities has no modification date for BioProject, only registration date | Full re-download |
| `biosample_set.xml.gz` | 5.0 GB | No file delta. E-utilities can list records by modification date (`[MDAT]`) | Full re-download |
| `geo-series` (E-utilities dump) | about 300,000 records, built by the script | No file at all. E-utilities can list Series by update date (`[UDAT]`) | Delta by `[UDAT]`, with a periodic full rebuild |
| SRA metadata Parquet (S3) | 14.2 GB in 30 objects | No. One current snapshot, replaced daily, no history | Full re-download |

Only `geo-series` gains from a delta: a week of changes is about 1,000 Series, a handful of
E-utilities requests instead of about 630 (inferred). For the four files, a full fetch is
minutes of bandwidth on NCBI's download site, while any delta would either miss columns or
move load onto E-utilities, which the store wants to keep light (inferred).

## 1. Sizes and what NCBI publishes next to each file

| File | Size (bytes) | Last-Modified | Other files in the directory | Evidence |
| --- | ---: | --- | --- | --- |
| `sra/reports/Metadata/SRA_Accessions.tab` | 35,575,829,532 | 2026-11-06 19:09 GMT | `SRA_Run_Members.tab` (4.3 GB); daily and monthly `NCBI_SRA_Metadata_*.tar.gz` | measured (`HEAD`, listing) |
| `bioproject/bioproject.xml` | 4,175,925,142 | 2026-11-06 17:54 GMT | `summary.txt` (77 MB, last changed 2025-04-08); `OLD_XML/core_bioproject.xml` (frozen 2024-02-25); schemas; README | measured |
| `biosample/biosample_set.xml.gz` | 4,977,936,060 | 2026-11-06 18:09 GMT | none: it is the only file | measured |
| S3 `sra-pub-metadata-us-east-1/sra/metadata/` | 14,180,682,713 in 30 objects | all 2026-11-06 00:39–00:41 GMT | no dated prefixes; the bucket root holds only `sra/`, `sra_tax_analysis_tool/`, `test/` | measured (S3 listing) |

- All three download-site files are rewritten daily (measured: today's dates; read: the
  BioProject README says its dump and `summary.txt` are "updated daily", although `summary.txt`
  has not changed since 2025-04-08).
- `bioproject.xml.gz` does not exist (measured: 404). The file is served uncompressed.
- All three answer `Accept-Ranges: bytes` (measured), so a cut transfer resumes. They send no
  `ETag` (measured), only `Last-Modified`.

## 2. SRA: what exists beside `SRA_Accessions.tab`

### Daily and monthly metadata tarballs (`sra/reports/Metadata/`)

- `NCBI_SRA_Metadata_Full_YYYYMMDD.tar.gz` appears monthly, 13 kept, from 2025-10-17 to
  2026-10-13, 14–17 GB each (measured, listing).
- `NCBI_SRA_Metadata_YYYYMMDD.tar.gz` appears daily, with some gaps, back to 2025-09-30, about
  7.0 GB each in the last month (measured, listing; 7,529,135,959 bytes for 2026-11-06).
- Contents (measured, first 20 MB of each, read as a tar stream): one folder per SRA
  submission (`DRA…`, `ERA…`, `SRA…`) holding `submission`, `study`, `sample`, `experiment`
  and `run` XML, plus a whole copy of `SRA_Run_Members` (4,267,387,363 bytes, the same size as
  the standalone file).
- The daily tarball is not a one-day delta. It stays about 7.0 GB every day from 2026-10-14 to
  2026-11-06 (measured), and its first 20 MB move forward in submission number between those
  two days (measured: median `SRA2559506` against `SRA2580028`). It holds recently changed
  submissions plus the full run-members table (inferred). Its exact window is not documented
  on any NCBI page found (read, absence).
- Either way it is submission XML, not the `SRA_Accessions.tab` table, so it cannot patch that
  file (inferred).

### Daily mirroring deltas (`sra/reports/Mirroring/`)

- One folder per day, `NCBI_SRA_Mirroring_YYYYMMDD/`, 46 of them back to 2026-09-22, and a
  monthly `_Full` folder (2026-10-01 and 2026-11-01) (measured, listing). The window rolls:
  older days are removed (inferred from the 46-day span).
- A daily folder holds `livelist.csv.gz` (1.2 MB on 2026-11-05) and `meta_*_set.xml.gz` deltas
  for study, sample, experiment, run and analysis (167 KB–2.3 MB each) (measured). Each `_Full`
  `livelist.csv.gz` is 1.6 GB (measured).
- The 2026-11-05 `livelist.csv.gz` has 125,893 rows: 45,467 runs, 38,893 experiments, 37,879
  samples, 2,724 analyses and 930 studies (measured). Its rows carry `LastUpdate` dates of the
  day before, so it is a true one-day delta (measured on the first rows; inferred for all).
- Its columns: `Accession, Type, Status, Received, Published, LastUpdate, LastMetaUpdate,
  ReplacedBy, BioSample, BioProject, Insdc` (measured).
- `SRA_Accessions.tab` has `Accession, Submission, Status, Updated, Published, Received, Type,
  Center, Visibility, Alias, Experiment, Sample, Study, Loaded, Spots, Bases, Md5sum, BioSample,
  BioProject, ReplacedBy` (measured, first line).
- The store reads `Type`, `BioProject`, `Study`, `Experiment`, `Accession` and `Status` (read,
  `labdata.store.fetch.experiments_under` and `labdata.store.build`). The livelist has no
  `Study` or `Experiment`, so it cannot rebuild the run-to-experiment and study links (inferred).

## 3. BioProject and BioSample through E-utilities

| Query | Count | Evidence |
| --- | ---: | --- |
| `db=biosample`, all records | 60,783,656 | measured |
| `db=biosample`, `2026/10/30:2026/11/05[MDAT]` | 183,265 | measured |
| `db=bioproject`, all records | 1,115,121 | measured |
| `db=bioproject`, `2026/10/30:2026/11/05[DATE]` (registration) | 2,100 | measured |
| `db=bioproject`, `[MDAT]` or `datetype=mdat` | not a field: E-utilities reads it as free text and finds nothing | measured |

- `einfo` lists the date fields: BioSample has `PDAT` and `MDAT` (modification date);
  BioProject has only `DATE` (registration date) (measured).
- So a BioProject delta from E-utilities catches new projects, not edits to old ones. A
  BioSample delta is possible: about 0.3% of records change in a week (measured counts), about
  370 `efetch` batches of 500 per week (inferred). That moves the load from one 5 GB file to
  hundreds of E-utilities calls, which runs against the store's choice to stay gentle on NCBI
  (inferred).

## 4. `geo-series`

- No GEO Series dump exists on the download site; the script builds it from `esearch` and
  `esummary` on `db=gds` (read, the script).
- `gse[ETYP]` matches 300,226 Series (measured). A full dump is about 31 `esearch` pages of
  10,000 and about 600 `esummary` batches of 500 (inferred from the script's batch sizes).
- `einfo` for `gds` lists `PDAT` and `UDAT` (update date) (measured).
  `gse[ETYP] AND 2026/10/30:2026/11/05[UDAT]` matches 996 Series; the same week by `[PDAT]`
  matches 688 (measured).
- A delta: query `[UDAT]` from the last snapshot's date, `esummary` those IDs, and replace their
  lines (inferred). It does not see Series that were withdrawn or made private, so a periodic
  full rebuild is still needed (inferred). Whether every field change in `esummary` moves
  `UDAT` is not checked.

## 5. Full download times

- `SRA_Accessions.tab`: about 7 minutes (measured earlier on a lab node, outside this note).
  That is about 85 MB/s.
- At the same rate, `bioproject.xml` and `biosample_set.xml.gz` take about 1 minute each, and
  the Parquet snapshot about 3 minutes (inferred, not timed).
- `geo-series`: at least 1 minute of request time at the 10 per second E-utilities limit with
  a key; real wall time is set by server latency and was not timed here (inferred).

## Cons and open points

- No NCBI page found documents the daily metadata tarball's window, the mirroring folders, or
  how long either is kept. The facts above come from listings and partial reads.
- The livelist's one-day nature is checked on its first rows only.
- Download times other than `SRA_Accessions.tab` are extrapolated from one rate.

## What was run

- Directory listings and `HEAD` requests over HTTPS on the paths above.
- Ranged reads of the first 20 MB of three tarballs (2026-10-13 Full, 2026-10-14 and 2026-11-06
  daily), decompressed as a stream to list tar members.
- One whole download of the 1.2 MB 2026-11-05 `livelist.csv.gz`, and the first line of
  `SRA_Accessions.tab` and the `_Full` livelist by ranged read.
- `esearch` and `einfo` on `biosample`, `bioproject` and `gds`, and an S3 `list-type=2`
  listing of the Parquet bucket.
