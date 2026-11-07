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
| `papers.parquet` | paper | `paper`, `pmid`, `pmcid`, `doi`, `openalex`, `published`, `status`, `reason`, `xml_tier`, `pdf_tier` |
| `paper_info.parquet` | paper | `paper`, `pmid`, `pmcid`, `doi`, `title`, `journal`, `published`, `authors`, `abstract`, `publication_type` |
| `files.parquet` | file held or only listed | `paper`, `kind`, `name`, `path`, `size`, `md5`, `url`, `tier`, `label`, `caption`, `fetched`, `gone` |
| `attempts.parquet` | try at one source for one paper | `paper`, `tier`, `date`, `outcome`, `detail` |

- `status` is one of these:

  | Status | Meaning |
  | --- | --- |
  | `fetched` | the paper store holds the paper's XML or PDF |
  | `waiting` | a source will serve it later; `reason` says which |
  | `manual` | only a save in Paperpile can serve it; `reason` gives the last failure |
  | `skipped` | not a real paper, such as a placeholder PMID |

- A tier is the source a file came from: `pmc` (the PMC bucket), `openalex`, `host` (a free
  link on the publisher's site) or `paperpile`. `xml_tier` and `pdf_tier` name the tier of
  the paper's XML and PDF.
- `kind` is `xml`, `pdf` or `supplement`. `path` is relative to the root, and empty for a
  file only listed. Supplementary files up to 10 MB are downloaded; larger ones are listed
  with their URL.
- `gone` is the date a source stopped serving a file. The file and its row are kept.
- `authors` is a list of `{name, affiliations}`, in author order.

## papers/

- **What:** the paper files, one folder per paper.
- **Layout:**

  ```text
  papers/39637nnn/39637862/
    39637862.xml             the article as XML, at most one
    39637862.pdf             the article as PDF, at most one
    suppl/<file name>        supplementary files, under the publisher's own names
  papers/doi/10.1101%2F2024.01.15.575678/
    10.1101%2F2024.01.15.575678.pdf
  ```

  A PMID folder sits in a group of 1,000 PMIDs, as GEO groups Series. A DOI folder is the DOI
  with `/` written as `%2F`. Zip files stay packed. Where a file came from is in
  `files.parquet`, never in its name.
- **Made by:** the paper job, `labdata papers serve`.
