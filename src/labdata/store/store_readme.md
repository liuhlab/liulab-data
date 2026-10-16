# A labdata store: GEO and SRA metadata records

This folder is a labdata store. It holds the metadata records of every target GEO Series and
of every record they link to: GEO Samples and Platforms, SRA studies, experiments, samples and
runs, BioProjects and BioSamples. A target Series is a GEO Series whose type names
high-throughput sequencing. Each record is kept as the text NCBI served, and as rows in
Parquet tables that you can query with the `labdata` Python package or with DuckDB.

The store is not tied to one computer. Any full copy of this folder is the store. Paths in
this file are relative to the store root, written `<store>`.

Current build: `${build}`. The store switched to it at ${switched}. It was made by
${made_by}

It reads these snapshots:

${snapshot_table}

`labdata store build` writes this file each time it switches the store to a new build.
`labdata store readme` writes it again for the current build, with fresh folder sizes.

## Contents

```text
<store>/
  README.md                     this file
  labdata-store.toml            the marker: format version and the current build
  builds/<build-id>/            the tables, built from raw/ and snapshots/ (query these)
  raw/<source>/                 the text NCBI served for each record, by source
  snapshots/<source>/<date>/    whole NCBI bulk files, as downloaded
  support/                      kept run logs and hand scripts
  work/                         working files of the download (not part of the store)
```

Every folder under `builds/`, `raw/` and `snapshots/` has its own `README.md` and
`manifest.json`. They list each file with its size in bytes and its sha256, the command and
commit that made it, and how to refresh it. `manifest.json` is the copy for programs. To check
every file against its manifest, run the command below. It reads every byte, so it is slow on
the snapshots.

```bash
labdata store verify --root <store>
```

## labdata-store.toml

- **What:** the marker that makes this folder a store. A folder without it is not a store.
- **Layout:** TOML. `format_version`; `build`, the current build ID; `switched`, when the store
  switched to that build (UTC); the `[switched_away]` table, when each older build went out of
  use; and, if set, `copy_to`, where `labdata store copy` sends the store.
- **Made by:** `labdata store init` writes it. `labdata store build` sets `build` and
  `switched` when a new build is whole. Do not edit `build` by hand.
- **Refresh:** nothing to do. Each build updates it, and `labdata store prune` drops the
  builds it deletes from `[switched_away]`.

## builds/

- **What:** `builds/<build-id>/` holds the store's tables. Readers use only the build that
  `labdata-store.toml` names. A build ID is the UTC time the build started,
  `YYYYMMDDTHHMMSSZ`, unless the build was given a name.
- **Source:** `raw/` and `snapshots/` of this store. A build makes no network call. Its
  `manifest.json` names each raw file and snapshot it read, with its sha256.
- **Layout:**

  | File | One row per | Columns |
  | --- | --- | --- |
  | `records/<type>.parquet` | record, sorted by accession | `accession`, `type`, `status`, `submitted`, `updated`, `published`, `fields`, `raw_text`, `content_hash`, `snapshot_date` |
  | `links_by_from.parquet` | link, sorted by `from` | `from`, `to`, `relation`, `source` |
  | `links_by_to.parquet` | the same links, sorted by `to` | `from`, `to`, `relation`, `source` |
  | `holdings.parquet` | accession the store holds | `accession`, `type` |
  | `known.parquet` | accession a snapshot lists | `accession`, `source`, `snapshot_date` |
  | `supplementary_files.parquet` | supplementary file a GEO record lists | `owner`, `name`, `url`, `size`, `date`, `type`, `archive` |
  | `gaps.parquet` | target Series the store does not hold | `accession`, `reason`, `detail` |
  | `notes.parquet` | thing the build met and kept | `accession`, `kind`, `detail` |

  - The record files are `geo-series`, `geo-sample`, `geo-platform`, `sra-study`,
    `sra-experiment`, `sra-sample`, `sra-run`, `bioproject` and `biosample`. The `type`
    column spells the type out, such as `GEO Series` or `SRA experiment`.
  - `fields` is a list of `{name, value}` pairs: the source's own field names and values, in
    source order, with repeats kept. GEO names are the SOFT names without the `!`, such as
    `Series_title`. SRA names are XML paths, such as `EXPERIMENT/TITLE`.
  - Each link is stored once, under its forward name, such as `sample` (Series to Sample) or
    `SRA` (Sample to SRA experiment). The far end of a link can be an accession the store
    does not hold, or a paper, written `PMID:<n>`. `source` says who stated the link, such
    as `SOFT` or `SRA XML`.
  - Supplementary files are listed, never downloaded.
  - The build's `README.md` gives the coverage of the target Series, the count of notes by
    kind, and any GEO relation label kept as written.
- **Made by:** `labdata store build`, one step of `labdata store run`.
- **Refresh:** see [How to refresh](#how-to-refresh). A new build goes into a new folder.
  Readers that opened the old build keep reading it.

## raw/

- **What:** `raw/<source>/` holds what each fetch saved, one row per item. Text NCBI served
  is kept as served, except that GEO data tables are cut out of family SOFT files.
- **Layout:** Parquet files, one per bucket of accession numbers, named like
  `GSE24nnnn.parquet`. Columns: `accession`, `raw_text`, `hash` (sha256 of `raw_text`) and
  `fetch_date`. Rows are sorted by accession.

  | Source | One row per | Fetched from |
  | --- | --- | --- |
  | `geo-soft` | GEO Series: its family SOFT file, data tables cut out | GEO FTP site, `geo/series/<GSEnnn>/<GSE>/soft/` |
  | `geo-filelist` | GEO Series: its `filelist.txt` of supplementary files | GEO FTP site, `geo/series/<GSEnnn>/<GSE>/suppl/` |
  | `geo-gaps` | target Series whose SOFT could not be saved, with the reason | the GEO fetch |
  | `entrez-links` | GEO Series: its Entrez links to SRA and BioProject, one per line | E-utilities `elink` and `esummary` |
  | `sra-xml` | SRA experiment: its `EXPERIMENT_PACKAGE` XML | E-utilities `efetch`, `db=sra` |
  | `pubmed-si` | accession searched: the PubMed IDs that list it | E-utilities `esearch`, the `[si]` field |

  A `geo-soft` row with data tables cut out starts with two lines that give the size and
  sha256 of the whole file as served.
- **Made by:** the fetch steps of `labdata store run` write parts under `work/parts/`.
  `labdata store pack` merges them into these files and is their only writer.
- **Refresh:** `labdata store run` fetches only what is not saved yet. A saved item is not
  fetched again.

## snapshots/

- **What:** `snapshots/<source>/<date>/` holds whole bulk files from NCBI, kept as downloaded.
  The folder name is the download date. They list every accession NCBI knows, so the store
  can tell "not in NCBI" from "not saved here", and they give the target Series list.
- **Layout:**

  | Source | File | Fetched from |
  | --- | --- | --- |
  | `geo-series` | `gds_gse_esummary.jsonl.gz`: the E-utilities summary of every GEO Series, one JSON object per line | E-utilities `esearch` and `esummary`, `db=gds` |
  | `sra-accessions` | `SRA_Accessions.tab`: every SRA object with its status, dates and parents | NCBI FTP site, `sra/reports/Metadata/` |
  | `bioproject` | `bioproject.xml`: every BioProject | NCBI FTP site, `bioproject/` |
  | `biosample` | `biosample_set.xml.gz`: every BioSample | NCBI FTP site, `biosample/` |

  Beside a file fetched from the FTP site are its `.sha256` and the HTTP headers of a request
  made just before and just after the download (`.headers.before`, `.headers.after`).
  Matching headers show that the file did not change during the download. In
  `SRA_Accessions.tab`, a `-` cell means empty. `biosample_set.xml.gz` is very large once
  unpacked, so read it as a stream.
- **Made by:** `scripts/bulk/download_ncbi_bulk.py` in the liulab-data repository. Each
  folder's `manifest.json` names the commit.
- **Refresh:** download into a new dated folder; never overwrite an old one. From a checkout
  of liulab-data, on a computer that can reach NCBI:

  ```bash
  python -I scripts/bulk/download_ncbi_bulk.py <store>/snapshots --date YYYY-MM-DD \
    geo-series sra-accessions bioproject biosample
  ```

  It needs `curl`, and `aria2c` for faster downloads. The next `labdata store run` uses the
  newest date of each source.

## support/

- **What:** run logs and hand scripts kept with this copy of the store. Nothing reads them,
  and `labdata store copy` leaves them out of the GCS copy.
- **Layout:**
  - `logs/<round>/`: each step's log and the `run.json` of one run. A round is named by the
    `geo-series` snapshot date and the share of the target Series it covered: `f0.01` or
    `f0.1` for a sample, `all` for every one, such as `2026-10-14-all`.
  - `logs/jobs/`: the Slurm job log of each run, `labdata-run.<job id>.log`.
  - `logs/snapshots-<date>/`: the logs of the snapshot download of that date.
  - `scripts/`: the commands run by hand to make the snapshots.
- **Made by:** `scripts/store/keep_run_logs.sh` in the liulab-data repository copies a
  round's step logs, its `run.json` and its Slurm job logs from `work/run/` into `logs/`.
  The snapshot download logs and `scripts/` are kept by hand.
- **Refresh:** after each round, from a checkout of liulab-data:

  ```bash
  scripts/store/keep_run_logs.sh <store> <round>
  ```

  `<round>` is the round's folder under `work/run/`, such as `2026-10-14-all`. The script
  only copies, so running it again is safe.

## work/

- **What:** working files of the download: fetch parts, snapshot extracts, the target Series
  list, each run's step markers and logs, and the build lock. It is not part of the store, and
  `labdata store copy` leaves it out.
- **Refresh:** none. When no run is going, you may delete it. The next run makes it again,
  extracting the snapshots and starting each round from its first step. Fetch steps still skip
  every item saved in `raw/`.

## How to refresh

From a checkout of liulab-data, on a computer that can reach NCBI:

1. Download new snapshots into `snapshots/` (see above).
2. Save your NCBI email and API key once: `labdata ncbi configure`.
3. Run the download. It fetches what is missing, builds the tables, checks coverage, switches
   the store to the new build, deletes builds and snapshots out of use for longer than a grace
   period, and copies the store to GCS:

   ```bash
   labdata store run --root <store>
   ```

   On a Slurm cluster, `scripts/store/run.sbatch` runs the same command as a job.

4. Keep the round's logs in `support/`: `scripts/store/keep_run_logs.sh <store> <round>`.

5. Copy the store to GCS: `labdata store copy --root <store>`. The copy holds
   `labdata-store.toml`, this file, the snapshots the current build reads, `raw/` and the
   current build.

An old build stays for a grace period after the switch, so a reader that opened it can
finish. The liulab-data API reference for `labdata.store.prune` gives its length.

To tidy a store that nobody is reading, `labdata store prune --root <store> --grace 0` deletes
the old builds and the snapshots only they read at once. A reader still on an old build loses
it, so this is a one-off, not a refresh step. `prune` never deletes the current build or
anything it reads.

The liulab-data documentation pages "Run the full download" and "Copy the store to GCS" give
the details.

## Open it as a database

### With labdata

In an environment with liulab-data installed:

```python
from labdata import open_store

store = open_store("<store>")  # or set LABDATA_STORE and call open_store()

series = store.open("GSE20301")
print(series.type)                          # GEO Series
print(dict(series.fields)["Series_title"])  # Dynamic transcriptomes during ...

print(store.follow("GSE20301", "sample"))   # {'GSE20301': ['GSM517436', ...]}
print(store.follow("GSE20301", "PubMed"))   # {'GSE20301': ['PMID:20194744']}
print(store.supplementary_files(["GSE20301"]))  # {'GSE20301': [SupplementaryFile(...)]}
print(store.experiments_for("GSE20301"))    # [Experiment('SRX017370'), ...], no network

# One record type as a table, with chosen fields as list columns
table = store.table("GEO Series", ["Series_title"], where="published >= '2024-01-01'")
print(table.num_rows)
```

`open` raises `NotMirrored` for an accession that a snapshot lists but this store does not
hold, and `NotFound` for one that no snapshot lists.

### With DuckDB

DuckDB reads the Parquet files in place, with nothing to load first. In Python, from the
store root:

```python
import tomllib

import duckdb

with open("labdata-store.toml", "rb") as fh:
    build = "builds/" + tomllib.load(fh)["build"]

# A record with one field
duckdb.sql(f"""
    SELECT accession, published,
           list_filter(fields, x -> x.name = 'Series_title')[1].value AS title
    FROM read_parquet('{build}/records/geo-series.parquet')
    WHERE accession = 'GSE20301'
""").show()

# The samples of a Series, then their SRA experiments
duckdb.sql(f"""
    SELECT s."to" AS sample, x."to" AS experiment
    FROM read_parquet('{build}/links_by_from.parquet') s
    JOIN read_parquet('{build}/links_by_from.parquet') x
      ON x."from" = s."to" AND x.relation = 'SRA'
    WHERE s."from" = 'GSE20301' AND s.relation = 'sample'
""").show()

# The supplementary files of a Series, with sizes
duckdb.sql(f"""
    SELECT owner, name, size, url
    FROM read_parquet('{build}/supplementary_files.parquet')
    WHERE owner = 'GSE20301'
""").show()
```

## Sizes

The size of each folder in this copy when this file was written: the sum of its file sizes.
A build's `known.parquet` can be a hard link to a file under `work/known/`, so it may count in
both.

${size_table}
