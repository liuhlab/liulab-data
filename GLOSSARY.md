# liulab-data

Vocabulary for curating public sequencing metadata for the lab.

## Glossary

### Study

The unit at which a set of experiments and samples is understood together: why there are this
many, how they relate, and what biology they teach. It is decided by reading the content and the
paper. No single source container (a GEO SuperSeries or Series, a BioProject) is a study, and
labdata stores none.

_Avoid_: series, project, dataset (each names one source's container, not the study)

### Build

One complete set of queryable tables, made offline from the store's raw files and snapshots and
never changed afterward. The store names one build as current, and readers open that one.

_Avoid_: snapshot (a dated copy of one bulk source, which a build reads)

### Refresh

One pass that brings the store to the sources' current state: it adds new records, replaces
changed ones, removes withdrawn GEO Series, renews the snapshots and ends in a new current build.
SRA records stay once held.

_Avoid_: update, sync (each says less than the whole pass)

### Build reader

The one way to open a build: it knows where the build lives, reads its tables through a DuckDB
connection capped in memory and threads, and turns rows into records by column name.

_Avoid_: backend

### Table layout

The one declaration of every table in a build: its file name, columns and their types, sort key,
compression and rows per row group. It is also the only code that writes a build table, so
writers hand it rows and it writes them as declared.

_Avoid_: schema (names only the columns and their types)

### Target Series list

The GEO Series a full download must hold: the sequencing Series in a dated `geo-series`
snapshot. A Series is on the list by the snapshot's type, even when its SOFT file gives another.

### Bucket

A range of accession numbers within one prefix whose raw rows share one file,
`raw/<source>/<bucket>.parquet`. Each source has its own bucket width.

### Piece

One unit of fetch work: the accessions of one bucket that nothing has saved yet, plus its
accessions on the re-fetch list that no part holds yet. A finished piece is saved as a part.

### Re-fetch list

The GEO Series a refresh fetches again even though `raw/` holds them: target Series GEO updated
since the last successful refresh, new target Series, and every Series a relation line in theirs
names. Their SRA experiments are fetched again with them. A part's newer row replaces the old one
at pack.

### Part

A file under `work/parts/` holding one piece's fetched rows. It appears whole by a rename, so a
rerun skips a piece whose part exists.

### Pack

The step that merges parts into bucket files. It is the only writer of `raw/`.

_Avoid_: write, save (for `raw/` itself)

### Removal candidate

A GEO Series `raw/` holds that may be withdrawn: GEO's current Series list no longer names it,
or this refresh's fetch got no record for it. A candidate leaves only when a fetch of it alone
confirms it is gone.

### Removal part

A part that lists accessions to delete instead of holding rows. Pack applies it in part order:
it deletes the rows saved before it, and a row saved after it wins.

### Status record

`refresh-status.json` in the store root: the last refresh's start, end and outcome, the date
of the last successful refresh, the records it added, replaced and removed per source, and
whether the GCS copy is pending. The date is written only after the switch to the new build.

_Avoid_: status file, refresh log

### Gap

A Series on the target Series list that the store does not hold, recorded with a reason. Coverage
passes when every target Series is either held or a gap with a known reason.

### Sample build

A build of a seeded share of a full store, made into a scratch root by the full build's own
code. A raw row is in the sample when its accession is, and every record parsed from it comes
with it; BioProject and BioSample records are also sampled by their own accession. A smaller
share's sample lies inside a larger one's.

_Avoid_: round (a share of the target Series list that the full download fetches)

### Round

One run of the full download over a seeded sample of the target Series list, such as 1%. A
smaller round's sample lies inside every larger one's, and gaps and coverage of a round count only
its sample.

### Paper store

The store of paper files beside the GEO/SRA store: each paper's original files (PMC XML, PDF,
supplements), the supplement catalog and each paper's status. It holds files that cost quota or
manual work to replace, so nothing remade from public sources writes into it.

_Avoid_: paper cache

### Tier

One source the paper job tries for a paper, in a fixed order: the PMC bucket, OpenAlex's paid
copy, a free host link, then the manual queue in Paperpile. A paper moves to the next tier only
when the one before cannot serve it.
