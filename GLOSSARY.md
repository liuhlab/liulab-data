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
A refresh never removes an SRA record; only a yearly maintenance run may.

_Avoid_: sync (says less than the whole pass)

### Update

A change to a store that ends finalized. For the GEO/SRA store: a refresh, a full run, a build
by hand or the SRA removal, one at a time under the run lock. For the paper store: a pass. Each
checks the work disk against GCS at its start and ends with a copy.

### Build reader

The one way to open a build: it knows where the build lives, reads its tables through a DuckDB
connection capped in memory and threads, and turns rows into records by column name.

_Avoid_: backend

### Table layout

The one declaration of every table in a build: its file name, columns and their types, sort key,
compression and rows per row group. It is also the only code that writes a build table, so
writers hand it rows and it writes them as declared.

_Avoid_: schema (names only the columns and their types)

### Store layout

The one declaration of every path in a store root and its kind. State is what GCS holds: a copy
sends it and a restore brings it back. Rebuildable is remade on demand from state, never copied
and kept by a cleanup. Scratch is on the work disk only, and a cleanup may delete it when no run
is going. Each store has one, and the GEO/SRA store's is also the only reader and switcher of
the marker.

_Avoid_: table layout (the tables inside one build)

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
rerun skips a piece whose part exists. Parts apply in the order they appeared, so a later part's
row or removal wins over an earlier one's.

### Pack

The step that merges parts into bucket files. It is the only writer of `raw/`.

_Avoid_: write, save (for `raw/` itself)

### Extract

The Parquet copy of one snapshot, cut into parts a build reads in parallel. It holds nothing its
snapshot lacks, so GCS does not hold it: a step that needs a missing extract first makes it
again from the snapshot. Prune deletes an extract with its snapshot.

_Avoid_: snapshot (the downloaded file an extract is made from)

### Removal candidate

A GEO Series `raw/` holds that may be withdrawn: GEO's current Series list no longer names it,
or this refresh's fetch got no record for it. A candidate leaves only when a fetch of it alone
confirms it is gone.

### Removal part

A part that lists accessions to delete instead of holding rows. Every read of `raw/` and pack
apply it in part order: it drops the rows saved before it, and a row saved after it wins.

### Status record

Each store's record of its last update: its start, end and outcome, and whether it is
finalized. The GEO/SRA store's keeps the name `refresh-status.json`, though it records every
update: it names the update (a refresh, a full run, a build by hand or the SRA removal) and its
new build; its outcome is running, failed (stopped before its switch), built (switched on the
work disk, not finalized) or finalized. An unfinished run a newer update replaces leaves its
record in its folder, and prune removes a refresh's folder; one replaced before its switch is
abandoned. It also holds the date of the last successful refresh, written only at a refresh's
switch, the records the refresh added, replaced and removed per source, and the refresh DAG
version that started it; a refresh another version started is not resumed before its switch,
and after it only its copy runs. The paper store's, `paper-status.json`, holds its passes'
dates and paper counts. GCS holds each store's status record as the copy that finalized its
last update left it, so GCS never holds one that is running or pending.

_Avoid_: status file, refresh log

### Run lock

The GEO/SRA store's record of the process running an update, `work/run.lock`: its Slurm job,
host, process and start time. One update holds it at a time: a refresh, a full run, a build by
hand or the SRA removal. Every command that writes the store holds it: a fetch, pack, extract,
copy, prune, restore or README rewrite run by hand too. The steps an update starts inherit the
owner's token and pass. A start that finds it asks whether the owner still runs, by Slurm or on
the owner's host. It refuses while the owner runs, and takes the lock once the owner is gone. When it cannot check, it refuses unless told the owner is gone (`--owner-gone`). A
refresh stopped before its switch resumes only within 7 days of its start; one switched with
its copy pending is finished by the copy alone, however old.

_Avoid_: build lock (the lock one build, prune or copy holds while it writes)

### Maintenance run

One pass of upkeep on the stores, monthly or yearly, done by one written procedure. A monthly run is
the refresh and its light checks; a yearly run adds the heavy checks and the removal of withdrawn
SRA records.

_Avoid_: refresh (one step of a monthly maintenance run, not the whole of it)

### Maintenance log

The file in the repository with one entry per maintenance run: its date, period, build, the
commit it ran, each check's outcome and its time against budget.

_Avoid_: status record (each store's own file of its last update)

### Gap

A Series on the target Series list that the store does not hold, recorded with a reason. Coverage
passes when every target Series is either held or a gap with a known reason. A target Series
confirmed withdrawn is a gap too, and its gap row makes it no removal candidate.

### Sample build

A build of a seeded share of a full store, made into a scratch root by the full build's own
code. A raw row is in the sample when its accession is, and every record parsed from it comes
with it; BioProject and BioSample records are also sampled by their own accession. A smaller
share's sample lies inside a larger one's.

_Avoid_: round (a share of the target Series list that the full download fetches)

### Subset

A small store cut from one build of a full store to a given list of Series, with a full
store's tables and layout. It holds the listed Series and every record reached from them by
following links forward; a paper or a BioProject shared with other Series adds none of them.
It keeps every link of a held record. Its rows are copied from the build, not parsed again,
so it has no raw files or snapshots. A paper store's subset holds the papers that a
subset's Series and BioProjects link, with their files.

_Avoid_: sample build (a seeded share, parsed again from the raw files)

### Round

One run of the full download over a seeded sample of the target Series list, such as 1%. A
smaller round's sample lies inside every larger one's, and gaps and coverage of a round count only
its sample.

### Paper store

The store of paper files beside the GEO/SRA store: each paper's original files (PMC XML, PDF,
supplements), the supplement catalog and each paper's status. It holds files that cost quota or
manual work to replace, so nothing remade from public sources writes into it.

_Avoid_: paper cache

### Paper

A publication linked to a Series or BioProject, preprints included, identified by its PMID, or by
its DOI when it has no PMID. A paper that later gets a PMID is the same paper under its PMID. A
preprint a record links alone is its published version, once that is known. A DOI that names
anything other than one article, such as a dataset, a thesis, a conference abstract or a peer
review, is not a paper, nor is a placeholder PMID that only BioProject records give. The paper
store holds nothing for them.

### DOI-only paper

A paper the store knows by its DOI alone, because no PMID is known for it. It comes in through a
DOI that a Series or BioProject links when that record links no PMID. Once a PMID is found, it is
the same paper under its PMID and is no longer DOI-only.

### Paper status

Where a paper stands in the paper store: fetched (the store holds its XML or PDF), waiting (a tier
will serve it later), manual (only a save in Paperpile can serve it) or skipped (retracted, so
nothing more is fetched). A paper that is not fetched is missing.

_Avoid_: paper state

### Tier

One source the paper job tries for a paper, in a fixed order: the PMC bucket, OpenAlex's paid
copy, a free host link, then the manual queue in Paperpile. A paper moves to the next tier only
when the one before cannot serve it. If PMC later serves a paper, its copy replaces the PDF a
later tier gave.

### Tier ladder

The one rule that turns a paper's row, attempts and files into its paper status, its reason and
the tier to try next. Tiers only record attempts and files; the daily pass and the PMC sync both
settle papers on the ladder, so they always agree.

### Paper job

The one long-running job that keeps the paper store current. It runs the daily pass every day and
the PMC sync every month and after each new build, one at a time, and never runs inside a
refresh.

_Avoid_: paper refresh (a refresh is the GEO/SRA store's pass)

### Daily pass

The paper job's daily run over the tiers that ration work by day: OpenAlex's paid copy, free host
links and the Paperpile folder. It ends by rewriting the manual queue.

### PMC sync

The paper job's monthly run against the PMC bucket, which a new build also starts: it adds
papers newly linked by the current build, renews changed PMC files and paper details, and
releases papers whose PMC wait has ended. The first PMC sync is the backfill.

_Avoid_: PMC refresh

### Manual queue

The papers in manual status, newest first, handed to the maintainer to save in Paperpile.

### Copy record

A store's list of what GCS holds, as the last successful copy left it, kept on the work disk and
in GCS. A routine copy compares it with what the store holds now, so it sends only the changes
and deletes in GCS only what left the store, never what the work disk lacks.

_Avoid_: record (alone), which reads as a store record or the status record

### Held set

What a store's copy sends to GCS, each file with a signature that changes with the file. For
the GEO/SRA store, the current build and what it read; for the paper store, the paper files
the `files` table names and the files at the top of the store. A copy saves it as the copy
record.

### Finalized

An update is finalized once GCS holds the state it made, which is when the copy record in GCS
names that state: for the GEO/SRA store, the current build and the raw files and snapshots it
read; for the paper store, the paper files and the tables that name them. Until then the update
is not done, even when readers on the work disk already use it. GCS is the only source of truth
for both stores.

_Avoid_: synced, done, succeeded (for an update whose copy has not worked)

### Restore

Filling a work disk from GCS, the copy run the other way: it brings the state files the copy
record in GCS names that the disk lacks or holds differently, never scratch. An empty disk gets
the whole store; a disk that fell behind gets only the difference.

_Avoid_: pull, sync
