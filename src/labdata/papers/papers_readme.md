# A labdata paper store: the papers linked to GEO and SRA records

This folder is a labdata paper store. It holds the papers that GEO Series and BioProjects in a
labdata store link to: each paper's article as XML or PDF, its small supplementary files, a
list of all its supplementary files, and where each paper stands. A paper is keyed by its
PubMed ID (PMID), or by its DOI when it has no PMID. A preprint and its journal version are
two papers.

Any full copy of this folder is the paper store. Paths in this file are relative to the paper
store root, written `<papers>`. The paper store never writes into the labdata store, and the
labdata store never writes into it.

## Contents

```text
<papers>/
  README.md                 this file
  labdata-papers.toml       the marker: the labdata store it reads, and where it is copied
  paper-status.json         the last pass of the paper job, and counts by status
  .lock                     held by a running pass, so two passes never overlap
  .daily-journal.jsonl      downloads a stopped daily pass has not yet put in the tables
  papers.parquet            one row per paper: IDs and status
  paper_info.parquet        one row per paper: title, authors, abstract
  files.parquet             one row per file, held or only listed
  attempts.parquet          one row per try at a source (the paper job's own record)
  manual-queue.nbib         papers only a person can save, for Paperpile
  manual-queue-dois.txt     the same papers, one DOI a line
  papers/                   the paper files
```

## labdata-papers.toml

- **What:** the marker that makes this folder a paper store.
- **Layout:** TOML. `format_version`; `store`, the root of the labdata store whose current
  build links the papers; `copy_to`, where the paper store is copied with rclone;
  `drive_remote` and `drive_folder`, the rclone remote and folder ID of the Paperpile folder
  on Google Drive.
- **Made by:** `labdata papers init`.

## paper-status.json

- **What:** where the paper job stands. One look tells you the state.
- **Layout:** JSON. `last_pass` gives the kind (`daily` or `pmc-sync`), start, end and
  outcome of the last pass. `last_daily_success` and `last_pmc_sync_success` give the date
  each last succeeded. `fetches` counts the papers fetched today by source, and `quota`
  counts the OpenAlex downloads spent today by key. `counts` gives papers by status, then
  by reason. `paperpile_pending` and `copy_pending` are true when a step was skipped because
  its login has expired.
- **Made by:** each pass of the paper job, at its start and its end.

## The tables

Four Parquet tables at the root, each sorted by `paper`. Query them with DuckDB, or with
`labdata.papers.open_paper_store().reader`, which joins them to the labdata store's links.

| File | One row per | Columns |
| --- | --- | --- |
| `papers.parquet` | paper | `paper`, `pmid`, `pmcid`, `doi`, `openalex`, `published`, `status`, `reason`, `xml_tier`, `pdf_tier`, `added`, `looked_up`, `work_type`, `openalex_pdf`, `host_pdf_urls`, `retracted` |
| `paper_info.parquet` | paper | `paper`, `pmid`, `pmcid`, `doi`, `title`, `journal`, `published`, `authors`, `abstract`, `publication_type` |
| `files.parquet` | file held or only listed | `paper`, `kind`, `name`, `path`, `size`, `md5`, `url`, `tier`, `label`, `caption`, `fetched`, `gone`, `etag` |
| `attempts.parquet` | try at one source for one paper | `paper`, `tier`, `date`, `account`, `outcome`, `detail` |

- `status` is one of these:

  | Status | Meaning |
  | --- | --- |
  | `fetched` | the paper store holds the paper's XML or PDF |
  | `waiting` | a source will serve it later; `reason` says which |
  | `manual` | only a save in Paperpile can serve it; `reason` gives the last failure |
  | `skipped` | not a real paper, such as a placeholder PMID, or retracted |

- `reason` says why a paper is not fetched:

  | Status | Reason | Meaning |
  | --- | --- | --- |
  | `waiting` | `pmc-embargo` | published under a year ago; PMC may still release it |
  | `waiting` | `openalex-backlog` | OpenAlex sells a copy; it waits for a day's quota |
  | `waiting` | `host-link` | a free link on the publisher's site is still to be tried |
  | `manual` | `no-route` | no source serves it |
  | `manual` | `openalex-failed` | OpenAlex's copy could not be fetched |
  | `manual` | `host-blocked` | the publisher's site refused the download |
  | `manual` | `host-failed` | the free link gave no PDF |
  | `skipped` | `placeholder` | a small PMID only BioProject XML gives, not a real paper |
  | `skipped` | `retracted` | the paper is retracted |
  | `skipped` | `not-article` | OpenAlex calls the DOI a dataset or the like, not a paper |

- `retracted` is true once PMC, PubMed or OpenAlex calls the paper retracted. A retracted
  paper is `skipped` with reason `retracted`: nothing more is downloaded for it, and the
  files already held stay.

- A tier is the source a file came from: `pmc` (the PMC bucket), `openalex`, `host` (a free
  link on the publisher's site) or `paperpile`. `xml_tier` and `pdf_tier` name the tier of
  the paper's XML and PDF.
- `kind` is `xml`, `pdf`, `figure` or `supplement`. `path` is relative to the root, and
  empty for a file only listed. Figures and supplementary files up to 10 MB are downloaded;
  larger ones are listed with their URL. Videos are always only listed. When PMC has a
  paper's XML, its PDF from PMC is only listed too: the XML and figures replace it.
- XML and plain-text files are stored compressed with zstd, one file at a time, with `.zst`
  added to the name: `zstd -d` or Python's `compression.zstd` reads them. `size` and `md5`
  are of the file before compression.
- `gone` is the date a source stopped serving a file. The file and its row are kept.
- `added` is the day a paper entered the store. `looked_up` is the day its IDs, details and
  OpenAlex record were last renewed; this happens at most once a month. `work_type` is
  OpenAlex's type for the paper, `openalex_pdf` is true when OpenAlex sells a copy, and
  `host_pdf_urls` are free PDF links on the publisher's or a preprint server's site.
- `account` names the OpenAlex account a paid try used, by its email.
- `authors` is a list of `{name, affiliations}`, in author order.

## papers/

- **What:** the paper files, one folder per paper.
- **Layout:**

  ```text
  papers/39637nnn/39637862/
    39637862.xml.zst         the article as XML, at most one
    39637862.pdf             the article as PDF, at most one
    fig/<file name>          figure images, under the publisher's own names
    suppl/<file name>        supplementary files, under the publisher's own names
  papers/doi/10.1101%2F2024.01.15.575678/
    10.1101%2F2024.01.15.575678.pdf
  ```

  A PMID folder sits in a group of 1,000 PMIDs, as GEO groups Series. A DOI folder is the DOI
  with `/` written as `%2F`. Zip files stay packed. Where a file came from is in
  `files.parquet`, never in its name.
- **Made by:** the paper job, `labdata papers serve`.
