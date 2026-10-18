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

### Build reader

The one way to open a build: it knows where the build lives, reads its tables through a DuckDB
connection capped in memory and threads, and turns rows into records by column name.

_Avoid_: backend

### Target Series list

The GEO Series a full download must hold: the sequencing Series in a dated `geo-series`
snapshot. A Series is on the list by the snapshot's type, even when its SOFT file gives another.

### Bucket

A range of accession numbers within one prefix whose raw rows share one file,
`raw/<source>/<bucket>.parquet`. Each source has its own bucket width.

### Piece

One unit of fetch work: the accessions of one bucket that nothing has saved yet. A finished
piece is saved as a part.

### Part

A file under `work/parts/` holding one piece's fetched rows. It appears whole by a rename, so a
rerun skips a piece whose part exists.

### Pack

The step that merges parts into bucket files. It is the only writer of `raw/`.

_Avoid_: write, save (for `raw/` itself)

### Gap

A Series on the target Series list that the store does not hold, recorded with a reason. Coverage
passes when every target Series is either held or a gap with a known reason.

### Round

One run of the full download over a seeded sample of the target Series list, such as 1%. A
smaller round's sample lies inside every larger one's, and gaps and coverage of a round count only
its sample.
